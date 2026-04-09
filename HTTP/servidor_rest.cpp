#include "servidor_rest.hpp"
#include "http_util.hpp"
#include "../Core/utilitarios.hpp"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <atomic>
#include <cctype>
#include <chrono>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
namespace {
using keeply::RestRequest;
using keeply::RestResponse;
using keeply::http_internal::closeSocketFd;
using keeply::http_internal::ensureSocketRuntime;
using keeply::http_internal::escapeJson;
using keeply::http_internal::lastSocketError;
using keeply::http_internal::socketErrorMessage;
using keeply::http_internal::socketInterrupted;
using keeply::http_internal::socketTimeoutOrWouldBlock;
using keeply::http_internal::toLower;
static constexpr std::size_t MAX_HEADER_BYTES=(1u<<20);
static constexpr std::size_t MAX_BODY_BYTES=(8u<<20);
using SocketLen =
    socklen_t;
static std::string toUpper(std::string s){
    for(char& c:s) c=(char)std::toupper((unsigned char)c);
    return s;}
static int fromHex(char c){
    if(c>='0'&&c<='9') return c-'0';
    if(c>='a'&&c<='f') return 10+(c-'a');
    if(c>='A'&&c<='F') return 10+(c-'A');
    return -1;}
static std::string urlDecode(const std::string& s){
    std::string out;
    out.reserve(s.size());
    for(std::size_t i=0;i<s.size();++i){
        char c=s[i];
        if(c=='+'){ out.push_back(' '); continue; }
        if(c=='%'&&i+2<s.size()){
            int hi=fromHex(s[i+1]),lo=fromHex(s[i+2]);
            if(hi>=0&&lo>=0){ out.push_back((char)((hi<<4)|lo)); i+=2; continue; }}
        out.push_back(c);}
    return out;}
static void parseQuery(const std::string& q,std::unordered_map<std::string,std::string>& out){
    std::size_t pos=0;
    while(pos<=q.size()){
        auto amp=q.find('&',pos);
        auto part=q.substr(pos,amp==std::string::npos?std::string::npos:amp-pos);
        if(!part.empty()){
            auto eq=part.find('=');
            if(eq==std::string::npos) out[urlDecode(part)]="";
            else out[urlDecode(part.substr(0,eq))]=urlDecode(part.substr(eq+1));}
        if(amp==std::string::npos) break;
        pos=amp+1;}}
static std::string reasonPhrase(int status){
    switch(status){
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 400: return "Bad Request";
        case 405: return "Method Not Allowed";
        case 404: return "Not Found";
        case 413: return "Payload Too Large";
        case 500: return "Internal Server Error";
        default: return "OK";}}
static void sendAll(int fd,const std::string& data){
    std::size_t off=0;
    while(off<data.size()){
        auto n=::send(fd,data.data()+(std::ptrdiff_t)off,data.size()-off,0);
        if(n<0){
            const int err=lastSocketError();
            if(socketInterrupted(err)) continue;
            throw std::runtime_error(std::string("send falhou: ")+socketErrorMessage(err));}
        off+=(std::size_t)n;}}
static void sendHttpResponse(int fd,const RestResponse& resp){
    std::ostringstream oss;
    std::string ct=resp.contentType.empty()?"application/json; charset=utf-8":resp.contentType;
    oss<<"HTTP/1.1 "<<resp.status<<" "<<reasonPhrase(resp.status)<<"\r\n";
    oss<<"Content-Type: "<<ct<<"\r\n";
    oss<<"Content-Length: "<<resp.body.size()<<"\r\n";
    oss<<"Connection: close\r\n\r\n";
    oss<<resp.body;
    sendAll(fd,oss.str());}
static void sendHttpError(int fd,int status,const std::string& msg){
    RestResponse r;
    r.status=status;
    r.contentType="application/json; charset=utf-8";
    r.body=std::string("{\"ok\":false,\"error\":\"")+escapeJson(msg)+"\"}";
    sendHttpResponse(fd,r);}
static void setSocketTimeouts(int fd,int recvMs,int sendMs){
    timeval rtv{recvMs/1000,(recvMs%1000)*1000};
    timeval stv{sendMs/1000,(sendMs%1000)*1000};
    (void)::setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&rtv,sizeof(rtv));
    (void)::setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&stv,sizeof(stv));}
static bool recvHttpRequest(int fd,RestRequest& req){
    std::string buf;
    buf.reserve(8192);
    char tmp[4096];
    std::size_t headerEnd=std::string::npos;
    while(true){
        auto n=::recv(fd,tmp,sizeof(tmp),0);
        if(n==0) return false;
        if(n<0){
            const int err=lastSocketError();
            if(socketInterrupted(err)) continue;
            if(socketTimeoutOrWouldBlock(err)) throw std::runtime_error("timeout lendo request");
            throw std::runtime_error(std::string("recv falhou: ")+socketErrorMessage(err));}
        buf.append(tmp,(std::size_t)n);
        headerEnd=buf.find("\r\n\r\n");
        if(headerEnd!=std::string::npos) break;
        if(buf.size()>MAX_HEADER_BYTES) throw std::runtime_error("Cabecalho HTTP muito grande.");}
    auto headerBlock=buf.substr(0,headerEnd);
    std::istringstream hs(headerBlock);
    std::string requestLine;
    if(!std::getline(hs,requestLine)) throw std::runtime_error("Request line ausente.");
    if(!requestLine.empty()&&requestLine.back()=='\r') requestLine.pop_back();
    std::istringstream rl(requestLine);
    std::string target,httpVersion;
    if(!(rl>>req.method>>target>>httpVersion)) throw std::runtime_error("Request line invalida.");
    req.method=toUpper(req.method);
    req.headers.clear();
    std::string line;
    while(std::getline(hs,line)){
        if(!line.empty()&&line.back()=='\r') line.pop_back();
        if(line.empty()) continue;
        auto colon=line.find(':');
        if(colon==std::string::npos) continue;
        req.headers[toLower(keeply::trim(line.substr(0,colon)))]=keeply::trim(line.substr(colon+1));}
    req.query.clear();
    auto qpos=target.find('?');
    req.path=qpos==std::string::npos?target:target.substr(0,qpos);
    if(qpos!=std::string::npos) parseQuery(target.substr(qpos+1),req.query);
    std::size_t contentLen=0;
    auto itCl=req.headers.find("content-length");
    if(itCl!=req.headers.end()){
        try{ contentLen=(std::size_t)std::stoull(itCl->second); }
        catch(...){ throw std::runtime_error("Content-Length invalido."); }}
    if(contentLen>MAX_BODY_BYTES) throw std::runtime_error("Payload muito grande.");
    auto bodyStart=headerEnd+4;
    while(buf.size()<bodyStart+contentLen){
        auto n=::recv(fd,tmp,sizeof(tmp),0);
        if(n==0) break;
        if(n<0){
            const int err=lastSocketError();
            if(socketInterrupted(err)) continue;
            if(socketTimeoutOrWouldBlock(err)) throw std::runtime_error("timeout lendo body");
            throw std::runtime_error(std::string("recv body falhou: ")+socketErrorMessage(err));}
        buf.append(tmp,(std::size_t)n);
        if(buf.size()>bodyStart+contentLen) break;}
    if(buf.size()<bodyStart+contentLen) throw std::runtime_error("Corpo HTTP truncado.");
    req.body.assign(buf.data()+(std::ptrdiff_t)bodyStart,contentLen);
    return true;}
static int createListenSocket(int port){
    int fd=::socket(AF_INET,SOCK_STREAM,0);
    if(fd<0) throw std::runtime_error(std::string("socket falhou: ")+socketErrorMessage(lastSocketError()));
    int one=1;
    (void)::setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one));
    sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);
    addr.sin_port=htons((uint16_t)port);
    if(::bind(fd,(sockaddr*)&addr,sizeof(addr))<0){
        auto err=std::string("bind falhou: ")+socketErrorMessage(lastSocketError());
        closeSocketFd(fd);
        throw std::runtime_error(err);}
    if(::listen(fd,64)<0){
        auto err=std::string("listen falhou: ")+socketErrorMessage(lastSocketError());
        closeSocketFd(fd);
        throw std::runtime_error(err);}
    return fd;}
static bool startsWith(const std::string& s,const std::string& p){ return s.rfind(p,0)==0; }
static bool endsWith(const std::string& s,const std::string& suf){ return s.size()>=suf.size()&&s.compare(s.size()-suf.size(),suf.size(),suf)==0; }
static std::string pathBetween(const std::string& path,const std::string& prefix,const std::string& suffix){
    if(!startsWith(path,prefix)||!endsWith(path,suffix)||path.size()<=prefix.size()+suffix.size()) return "";
    return path.substr(prefix.size(),path.size()-prefix.size()-suffix.size());}
static bool containsText(const std::string& text,const std::string& pattern){
    return text.find(pattern)!=std::string::npos;}}
namespace keeply {
struct BackupJob{
    std::string id;
    std::string label;
    std::string status;
    std::string error;
    BackupStats stats{};
    std::uint64_t startedAtMs=0,finishedAtMs=0;
};
class KeeplyRestApi::Impl{
public:
    std::shared_ptr<KeeplyApi> api;
    std::shared_ptr<IWsNotifier> ws;
    std::mutex mu;
    std::mutex jobsMu;
    std::mutex workersMu;
    std::unordered_map<std::string,BackupJob> jobs;
    std::vector<std::thread> workers;
    std::atomic<std::uint64_t> seq{1};
    std::atomic<bool> shuttingDown{false};
    static std::uint64_t nowMs(){
        using namespace std::chrono;
        return (std::uint64_t)duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();}
    std::string newJobId(){ return std::to_string(seq.fetch_add(1,std::memory_order_relaxed)); }
    void addWorker(std::thread worker){
        std::lock_guard<std::mutex> lock(workersMu);
        workers.push_back(std::move(worker));}
    void joinWorkers(){
        std::vector<std::thread> pending;{
            std::lock_guard<std::mutex> lock(workersMu);
            pending.swap(workers);}
        for(auto& worker:pending){
            if(worker.joinable()) worker.join();}}
};
KeeplyRestApi::KeeplyRestApi(std::shared_ptr<KeeplyApi> api,std::shared_ptr<IWsNotifier> wsNotifier):api_(std::move(api)),ws_(std::move(wsNotifier)),impl_(std::make_shared<Impl>()){
    if(!api_) throw std::runtime_error("KeeplyRestApi requer KeeplyApi.");
    impl_->api=api_;
    impl_->ws=ws_;}
KeeplyRestApi::~KeeplyRestApi(){
    impl_->shuttingDown.store(true,std::memory_order_relaxed);
    impl_->joinWorkers();}
RestResponse KeeplyRestApi::handle(const RestRequest& req){
    try{
        if(req.method=="GET"&&req.path=="/api/v1/health") return handleGetHealth();
        if(req.method=="GET"&&req.path=="/api/v1/state") return handleGetState();
        if(req.method=="POST"&&req.path=="/api/v1/config/source") return handlePostConfigSource(req);
        if(req.method=="POST"&&req.path=="/api/v1/config/archive") return handlePostConfigArchive(req);
        if(req.method=="POST"&&req.path=="/api/v1/config/restore-root") return handlePostConfigRestoreRoot(req);
        if(req.method=="POST"&&req.path=="/api/v1/backup") return handlePostBackup(req);
        if(req.method=="GET"&&req.path=="/api/v1/backup/jobs") return handleGetBackupJobs();
        if(req.method=="GET"&&startsWith(req.path,"/api/v1/backup/jobs/")) return handleGetBackupJob(req);
        if(req.method=="GET"&&req.path=="/api/v1/snapshots") return handleGetSnapshots();
        if(req.method=="GET"&&req.path=="/api/v1/diff") return handleGetDiff(req);
        if(req.method=="GET"&&startsWith(req.path,"/api/v1/snapshots/")&&endsWith(req.path,"/paths")) return handleGetSnapshotPaths(req);
        if(req.method=="POST"&&req.path=="/api/v1/restore/file") return handlePostRestoreFile(req);
        if(req.method=="POST"&&req.path=="/api/v1/restore/snapshot") return handlePostRestoreSnapshot(req);
        if(startsWith(req.path,"/api/v1/")) return jsonMethodNotAllowed("Metodo HTTP nao suportado para esta rota.");
        return jsonNotFound("Rota nao encontrada.");
    }catch(const KeeplyNotFoundError& e){
        return jsonNotFound(e.what());
    }catch(const KeeplyValidationError& e){
        return jsonBadRequest(e.what());
    }catch(const std::exception& e){
        const std::string msg=e.what();
        if(
            containsText(msg,"nao encontrado")||
            containsText(msg,"Nenhum snapshot encontrado")||
            containsText(msg,"Nao existe snapshot anterior")
        ) return jsonNotFound(msg);
        if(
            containsText(msg,"invalido")||
            containsText(msg,"inválido")||
            containsText(msg,"vazia")||
            containsText(msg,"vazio")||
            containsText(msg,"suportado")||
            containsText(msg,"Formato ")||
            containsText(msg,"Use ambos os parametros")||
            containsText(msg,"Path relativo inseguro")||
            containsText(msg,"Path relativo invalido")||
            containsText(msg,"Path relativo vazio")||
            containsText(msg,"Path escapou do diretorio de restore")||
            containsText(msg,"Origem permitida neste build")||
            containsText(msg,"Tamanho de divisao muito pequeno")
        ) return jsonBadRequest(msg);
        return jsonError(msg);}
    catch(...){ return jsonError("Erro interno desconhecido."); }}
RestResponse KeeplyRestApi::handleGetHealth(){ return jsonOk(R"({"ok":true,"service":"keeply","version":"v1"})"); }
RestResponse KeeplyRestApi::handleGetState(){
    std::lock_guard<std::mutex> lock(impl_->mu);
    const auto& s=impl_->api->state();
    std::ostringstream oss;
    oss<<"{"<<"\"source\":\""<<escapeJson(s.source)<<"\","<<"\"archive\":\""<<escapeJson(s.archive)<<"\","<<"\"restoreRoot\":\""<<escapeJson(s.restoreRoot)<<"\","<<"\"scanScope\":{"<<"\"id\":\""<<escapeJson(s.scanScope.id)<<"\","<<"\"label\":\""<<escapeJson(s.scanScope.label)<<"\","<<"\"requestedPath\":\""<<escapeJson(s.scanScope.requestedPath)<<"\","<<"\"resolvedPath\":\""<<escapeJson(s.scanScope.resolvedPath)<<"\"},"<<"\"archiveSplitEnabled\":"<<(s.archiveSplitEnabled?"true":"false")<<","<<"\"archiveSplitMaxBytes\":"<<s.archiveSplitMaxBytes<<"}";
    return jsonOk(oss.str());}
RestResponse KeeplyRestApi::handlePostConfigSource(const RestRequest& req){
    std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->api->setSource(req.body);
    if(impl_->ws){
        const auto& s=impl_->api->state();
        std::ostringstream oss;
        oss<<"{"<<"\"type\":\"config.updated\","<<"\"field\":\"source\","<<"\"source\":\""<<escapeJson(s.source)<<"\","<<"\"scanScope\":{"<<"\"id\":\""<<escapeJson(s.scanScope.id)<<"\","<<"\"label\":\""<<escapeJson(s.scanScope.label)<<"\","<<"\"requestedPath\":\""<<escapeJson(s.scanScope.requestedPath)<<"\","<<"\"resolvedPath\":\""<<escapeJson(s.scanScope.resolvedPath)<<"\"}"<<"}";
        impl_->ws->broadcastJson(oss.str());}
    return jsonOk(R"({"ok":true})");}
RestResponse KeeplyRestApi::handlePostConfigArchive(const RestRequest& req){
    std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->api->setArchive(req.body);
    if(impl_->ws) impl_->ws->broadcastJson(R"({"type":"config.updated","field":"archive"})");
    return jsonOk(R"({"ok":true})");}
RestResponse KeeplyRestApi::handlePostConfigRestoreRoot(const RestRequest& req){
    std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->api->setRestoreRoot(req.body);
    if(impl_->ws) impl_->ws->broadcastJson(R"({"type":"config.updated","field":"restoreRoot"})");
    return jsonOk(R"({"ok":true})");}
RestResponse KeeplyRestApi::handlePostBackup(const RestRequest& req){
    const auto impl=impl_;
    if(impl->shuttingDown.load(std::memory_order_relaxed)) return jsonError("Servidor em desligamento.");
    std::string label=req.body;
    auto id=impl->newJobId();
    BackupJob job;
    job.id=id;
    job.label=label;
    job.status="running";
    job.startedAtMs=Impl::nowMs();
    { std::lock_guard<std::mutex> lk(impl->jobsMu); impl->jobs[id]=job; }
    impl->addWorker(std::thread([impl,id,label](){
        BackupStats stats{};
        std::string err;
        try{
            std::lock_guard<std::mutex> lock(impl->mu);
            stats=impl->api->runBackup(label);}
        catch(const std::exception& e){ err=e.what(); }
        catch(...){ err="erro interno"; }
        std::string eventJson;{
            std::lock_guard<std::mutex> lk(impl->jobsMu);
            auto it=impl->jobs.find(id);
            if(it==impl->jobs.end()) return;
            it->second.stats=stats;
            it->second.finishedAtMs=Impl::nowMs();
            if(err.empty()){
                it->second.status="finished";
                eventJson=std::string("{\"type\":\"backup.finished\",\"jobId\":\"")+escapeJson(id)+"\"}";
            }else{
                it->second.status="failed";
                it->second.error=err;
                eventJson=std::string("{\"type\":\"backup.failed\",\"jobId\":\"")+escapeJson(id)+"\",\"error\":\""+escapeJson(err)+"\"}";}}
        if(impl->ws) impl->ws->broadcastJson(eventJson);
    }));
    return jsonAccepted(std::string("{\"ok\":true,\"jobId\":\"")+escapeJson(id)+"\"}");}
RestResponse KeeplyRestApi::handleGetBackupJobs(){
    std::vector<BackupJob> items;
    { std::lock_guard<std::mutex> lk(impl_->jobsMu); for(auto& kv:impl_->jobs) items.push_back(kv.second); }
    std::ostringstream oss;
    oss<<"{\"items\":[";
    for(std::size_t i=0;i<items.size();++i){
        if(i) oss<<",";
        auto& j=items[i];
        oss<<"{"<<"\"id\":\""<<escapeJson(j.id)<<"\","<<"\"label\":\""<<escapeJson(j.label)<<"\","<<"\"status\":\""<<escapeJson(j.status)<<"\","<<"\"error\":\""<<escapeJson(j.error)<<"\","<<"\"startedAtMs\":"<<j.startedAtMs<<","<<"\"finishedAtMs\":"<<j.finishedAtMs<<"}";}
    oss<<"]}";
    return jsonOk(oss.str());}
RestResponse KeeplyRestApi::handleGetBackupJob(const RestRequest& req){
    auto id=req.path.substr(std::string("/api/v1/backup/jobs/").size());
    BackupJob j;
    { std::lock_guard<std::mutex> lk(impl_->jobsMu); auto it=impl_->jobs.find(id); if(it==impl_->jobs.end()) return jsonNotFound("Job nao encontrado."); j=it->second; }
    std::size_t chunksReused=(j.stats.chunks>=j.stats.uniqueChunksInserted)?(j.stats.chunks-j.stats.uniqueChunksInserted):0;
    std::ostringstream oss;
    oss<<"{"<<"\"ok\":true,"<<"\"id\":\""<<escapeJson(j.id)<<"\","<<"\"label\":\""<<escapeJson(j.label)<<"\","<<"\"status\":\""<<escapeJson(j.status)<<"\","<<"\"error\":\""<<escapeJson(j.error)<<"\","<<"\"startedAtMs\":"<<j.startedAtMs<<","<<"\"finishedAtMs\":"<<j.finishedAtMs<<","<<"\"filesScanned\":"<<j.stats.scanned<<","<<"\"filesAdded\":"<<j.stats.added<<","<<"\"filesUnchanged\":"<<j.stats.reused<<","<<"\"chunksNew\":"<<j.stats.uniqueChunksInserted<<","<<"\"chunksReused\":"<<chunksReused<<","<<"\"bytesRead\":"<<j.stats.bytesRead<<","<<"\"bytesStoredCompressed\":0,"<<"\"warnings\":"<<j.stats.warnings<<"}";
    return jsonOk(oss.str());}
RestResponse KeeplyRestApi::handleGetSnapshots(){
    std::vector<SnapshotRow> rows;
    { std::lock_guard<std::mutex> lock(impl_->mu); rows=impl_->api->listSnapshots(); }
    std::ostringstream oss;
    oss<<"{\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        auto& r=rows[i];
        oss<<"{"<<"\"id\":"<<r.id<<","<<"\"createdAt\":\""<<escapeJson(r.createdAt)<<"\","<<"\"sourceRoot\":\""<<escapeJson(r.sourceRoot)<<"\","<<"\"label\":\""<<escapeJson(r.label)<<"\","<<"\"fileCount\":"<<r.fileCount<<"}";}
    oss<<"]}";
    return jsonOk(oss.str());}
RestResponse KeeplyRestApi::handleGetDiff(const RestRequest& req){
    auto itOlder=req.query.find("older"),itNewer=req.query.find("newer");
    std::vector<ChangeEntry> rows;
    { std::lock_guard<std::mutex> lock(impl_->mu);
      if(itOlder==req.query.end()&&itNewer==req.query.end()) rows=impl_->api->diffLatestVsPrevious();
      else if(itOlder!=req.query.end()&&itNewer!=req.query.end()) rows=impl_->api->diffSnapshots(itOlder->second,itNewer->second);
      else return jsonBadRequest("Use ambos os parametros 'older' e 'newer', ou nenhum.");}
    std::ostringstream oss;
    oss<<"{\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        auto& c=rows[i];
        oss<<"{"<<"\"path\":\""<<escapeJson(c.path)<<"\","<<"\"status\":\""<<escapeJson(c.status)<<"\","<<"\"oldSize\":"<<c.oldSize<<","<<"\"newSize\":"<<c.newSize<<","<<"\"oldMtime\":"<<c.oldMtime<<","<<"\"newMtime\":"<<c.newMtime<<"}";}
    oss<<"]}";
    return jsonOk(oss.str());}
RestResponse KeeplyRestApi::handleGetSnapshotPaths(const RestRequest& req){
    auto snapshotId=pathBetween(req.path,"/api/v1/snapshots/","/paths");
    if(snapshotId.empty()) return jsonBadRequest("Path de snapshot invalido.");
    std::vector<std::string> paths;
    { std::lock_guard<std::mutex> lock(impl_->mu); paths=impl_->api->listSnapshotPaths(snapshotId); }
    std::ostringstream oss;
    oss<<"{\"snapshot\":\""<<escapeJson(snapshotId)<<"\",\"items\":[";
    for(std::size_t i=0;i<paths.size();++i){ if(i) oss<<","; oss<<"\""<<escapeJson(paths[i])<<"\""; }
    oss<<"]}";
    return jsonOk(oss.str());}
RestResponse KeeplyRestApi::handlePostRestoreFile(const RestRequest& req){
    std::istringstream iss(req.body);
    std::string snapshot,relPath,outRoot;
    if(!std::getline(iss,snapshot,'|')||!std::getline(iss,relPath,'|')) return jsonBadRequest("Formato invalido. Use: snapshot|relpath|outRoot(opcional)");
    std::getline(iss,outRoot);
    snapshot=keeply::trim(snapshot);
    relPath=keeply::trim(relPath);
    outRoot=keeply::trim(outRoot);
    if(snapshot.empty()||relPath.empty()) return jsonBadRequest("Formato invalido. Use: snapshot|relpath|outRoot(opcional)");
    { std::lock_guard<std::mutex> lock(impl_->mu);
      if(outRoot.empty()) impl_->api->restoreFile(snapshot,relPath,std::nullopt);
      else impl_->api->restoreFile(snapshot,relPath,std::filesystem::path(outRoot));}
    if(impl_->ws) impl_->ws->broadcastJson(R"({"type":"restore.file.finished"})");
    return jsonOk(R"({"ok":true})");}
RestResponse KeeplyRestApi::handlePostRestoreSnapshot(const RestRequest& req){
    std::istringstream iss(req.body);
    std::string snapshot,outRoot;
    if(!std::getline(iss,snapshot,'|')) return jsonBadRequest("Formato invalido. Use: snapshot|outRoot(opcional)");
    std::getline(iss,outRoot);
    snapshot=keeply::trim(snapshot);
    outRoot=keeply::trim(outRoot);
    if(snapshot.empty()) return jsonBadRequest("Formato invalido. Use: snapshot|outRoot(opcional)");
    { std::lock_guard<std::mutex> lock(impl_->mu);
      if(outRoot.empty()) impl_->api->restoreSnapshot(snapshot,std::nullopt);
      else impl_->api->restoreSnapshot(snapshot,std::filesystem::path(outRoot));}
    if(impl_->ws) impl_->ws->broadcastJson(R"({"type":"restore.snapshot.finished"})");
    return jsonOk(R"({"ok":true})");}
static const std::string kVersionPrefix=std::string("{\"v\":")+std::to_string(keeply::kProtocolVersion)+",";
RestResponse KeeplyRestApi::jsonOk(const std::string& json){
    RestResponse r; r.status=200; r.contentType="application/json; charset=utf-8";
    if(!json.empty()&&json.front()=='{') r.body=kVersionPrefix+json.substr(1);
    else r.body=json;
    return r;}
RestResponse KeeplyRestApi::jsonCreated(const std::string& json){
    RestResponse r; r.status=201; r.contentType="application/json; charset=utf-8";
    if(!json.empty()&&json.front()=='{') r.body=kVersionPrefix+json.substr(1);
    else r.body=json;
    return r;}
RestResponse KeeplyRestApi::jsonAccepted(const std::string& json){
    RestResponse r; r.status=202; r.contentType="application/json; charset=utf-8";
    if(!json.empty()&&json.front()=='{') r.body=kVersionPrefix+json.substr(1);
    else r.body=json;
    return r;}
RestResponse KeeplyRestApi::jsonBadRequest(const std::string& message){
    RestResponse r; r.status=400; r.contentType="application/json; charset=utf-8";
    r.body=kVersionPrefix+"\"ok\":false,\"code\":400,\"error\":\""+escapeJson(message)+"\"}";
    return r;}
RestResponse KeeplyRestApi::jsonMethodNotAllowed(const std::string& message){
    RestResponse r; r.status=405; r.contentType="application/json; charset=utf-8";
    r.body=kVersionPrefix+"\"ok\":false,\"code\":405,\"error\":\""+escapeJson(message)+"\"}";
    return r;}
RestResponse KeeplyRestApi::jsonNotFound(const std::string& message){
    RestResponse r; r.status=404; r.contentType="application/json; charset=utf-8";
    r.body=kVersionPrefix+"\"ok\":false,\"code\":404,\"error\":\""+escapeJson(message)+"\"}";
    return r;}
RestResponse KeeplyRestApi::jsonError(const std::string& message){
    RestResponse r; r.status=500; r.contentType="application/json; charset=utf-8";
    r.body=kVersionPrefix+"\"ok\":false,\"code\":500,\"error\":\""+escapeJson(message)+"\"}";
    return r;}
std::string KeeplyRestApi::getPathParamAfterPrefix(const std::string& fullPath,const std::string& prefix){
    if(fullPath.rfind(prefix,0)!=0) return "";
    return fullPath.substr(prefix.size());}
class RequestRateLimiter{
    std::deque<std::chrono::steady_clock::time_point> timestamps_;
    std::size_t maxRequests_;
    std::chrono::milliseconds window_;
public:
    RequestRateLimiter(std::size_t maxRequests,std::chrono::milliseconds window)
        :maxRequests_(maxRequests),window_(window){}
    bool allow(){
        const auto now=std::chrono::steady_clock::now();
        while(!timestamps_.empty()&&(now-timestamps_.front())>window_)
            timestamps_.pop_front();
        if(timestamps_.size()>=maxRequests_) return false;
        timestamps_.push_back(now);
        return true;}
};
int runRestHttpServer(const RestHttpServerConfig& config){
    try{
        if(!config.api) throw std::runtime_error("RestHttpServerConfig requer KeeplyApi.");
        if(config.port<=0||config.port>65535) throw std::runtime_error("Porta invalida.");
        KeeplyRestApi rest(config.api,config.wsNotifier);
        int listenFd=createListenSocket(config.port);
        RequestRateLimiter rateLimiter(120,std::chrono::minutes(1));
        std::cout<<"Keeply REST HTTP listening on http://127.0.0.1:"<<config.port<<"\n";
        std::cout<<"Archive: "<<config.api->state().archive<<"\n";
        std::cout<<"RestoreRoot: "<<config.api->state().restoreRoot<<"\n";
        std::cout.flush();
        for(;;){
            sockaddr_in cli{};
            SocketLen cliLen=sizeof(cli);
            int cfd=::accept(listenFd,(sockaddr*)&cli,&cliLen);
            if(cfd<0){
                const int err=lastSocketError();
                if(socketInterrupted(err)) continue;
                throw std::runtime_error(std::string("accept falhou: ")+socketErrorMessage(err));}
            setSocketTimeouts(cfd,8000,8000);
            try{
                if(!rateLimiter.allow()){
                    sendHttpError(cfd,429,"Rate limit excedido. Tente novamente em alguns segundos.");
                    closeSocketFd(cfd);
                    continue;}
                RestRequest req;
                if(recvHttpRequest(cfd,req)){
                    std::cout<<req.method<<" "<<req.path<<"\n";
                    sendHttpResponse(cfd,rest.handle(req));}
            }catch(const std::exception& e){
                try{
                    std::string m=e.what();
                    int code=(m.find("Payload muito grande")!=std::string::npos)?413:500;
                    sendHttpError(cfd,code,m);
                }catch(...){}
            }catch(...){
                try{ sendHttpError(cfd,500,"erro interno"); }catch(...){}}
            closeSocketFd(cfd);}
        closeSocketFd(listenFd);
        return 0;
    }catch(const std::exception& e){
        std::cerr<<"REST HTTP server falhou: "<<e.what()<<"\n";
        return 1;}}}

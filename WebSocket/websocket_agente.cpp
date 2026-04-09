#include "websocket_agente.hpp"
#include "websocket_interno.hpp"
#include "../HTTP/http_util.hpp"
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>
namespace {
namespace fs = std::filesystem;
using namespace keeply::ws_internal;
std::string jsonBool(bool v){return v?"true":"false";}
std::string jsonStrField(const std::string& key,const std::string& value){return "\"" + key + "\":\"" + escapeJson(value) + "\"";}
std::string jsonNumField(const std::string& key,std::uintmax_t value){return "\"" + key + "\":" + std::to_string(value);}
std::string jsonBoolField(const std::string& key,bool value){return "\"" + key + "\":" + jsonBool(value);}
keeply::WsCommand parseLegacyCommand(const std::string& payload){
    keeply::WsCommand cmd;
    cmd.raw=payload;
    if(payload=="ping"){cmd.type="ping";return cmd;}
    if(payload=="state"){cmd.type="state";return cmd;}
    if(payload=="snapshots"){cmd.type="snapshots";return cmd;}
    if(payload=="fs.list"){cmd.type="fs.list";return cmd;}
    if(payload=="fs.disks"){cmd.type="fs.disks";return cmd;}
    if(payload=="backup"){cmd.type="backup";return cmd;}
    if(payload.rfind("fs.list:",0)==0){
        cmd.type="fs.list";
        const std::string args=payload.substr(8);
        const std::size_t sep=args.find('|');
        if(sep==std::string::npos) cmd.path=args;
        else{
            cmd.requestId=args.substr(0,sep);
            cmd.path=args.substr(sep+1);}
        return cmd;}
    if(payload.rfind("fs.disks:",0)==0){
        cmd.type="fs.disks";
        cmd.requestId=payload.substr(9);
        return cmd;}
    if(payload.rfind("scan.scope:",0)==0){
        cmd.type="scan.scope";
        cmd.scopeId=payload.substr(11);
        return cmd;}
    if(payload.rfind("config.source:",0)==0){
        cmd.type="config.source";
        cmd.path=payload.substr(14);
        return cmd;}
    if(payload.rfind("config.archive:",0)==0){
        cmd.type="config.archive";
        cmd.path=payload.substr(15);
        return cmd;}
    if(payload.rfind("config.restoreRoot:",0)==0){
        cmd.type="config.restoreRoot";
        cmd.path=payload.substr(19);
        return cmd;}
    if(payload.rfind("backup:",0)==0){
        cmd.type="backup";
        const auto parts=splitPipe(payload.substr(7));
        if(!parts.empty()) cmd.label=parts[0];
        for(std::size_t i=1;i<parts.size();++i){
            if(parts[i].rfind("storage=",0)==0) cmd.storage=parts[i].substr(8);
            else if(parts[i].rfind("source=",0)==0) cmd.sourcePath=parts[i].substr(7);}
        return cmd;}
    if(payload.rfind("restore.file:",0)==0){
        cmd.type="restore.file";
        const auto parts=splitPipe(payload.substr(13));
        if(parts.size()>=1) cmd.snapshot=parts[0];
        if(parts.size()>=2) cmd.relPath=parts[1];
        if(parts.size()>=3) cmd.outRoot=parts[2];
        return cmd;}
    if(payload.rfind("restore.snapshot:",0)==0){
        cmd.type="restore.snapshot";
        const auto parts=splitPipe(payload.substr(17));
        if(parts.size()>=1) cmd.snapshot=parts[0];
        if(parts.size()>=2) cmd.outRoot=parts[1];
        return cmd;}
    cmd.type="unsupported";
    return cmd;}
keeply::WsCommand parseJsonCommand(const std::string& payload){
    keeply::WsCommand cmd;
    cmd.raw=payload;
    cmd.type=keeply::trim(extractJsonStringField(payload,"type"));
    cmd.requestId=keeply::trim(extractJsonStringField(payload,"requestId"));
    cmd.path=keeply::trim(extractJsonStringField(payload,"path"));
    cmd.sourcePath=keeply::trim(extractJsonStringField(payload,"source"));
    cmd.scopeId=keeply::trim(extractJsonStringField(payload,"scopeId"));
    cmd.label=extractJsonStringField(payload,"label");
    cmd.storage=extractJsonStringField(payload,"storage");
    cmd.snapshot=extractJsonStringField(payload,"snapshot");
    cmd.relPath=extractJsonStringField(payload,"relPath");
    if(cmd.relPath.empty()) cmd.relPath=cmd.path;
    cmd.outRoot=extractJsonStringField(payload,"outRoot");
    cmd.ticketId=extractJsonStringField(payload,"ticketId");
    cmd.downloadPathBase=extractJsonStringField(payload,"downloadPathBase");
    cmd.backupId=extractJsonStringField(payload,"backupId");
    cmd.backupRef=extractJsonStringField(payload,"backupRef");
    cmd.bundleId=extractJsonStringField(payload,"bundleId");
    cmd.archiveFile=extractJsonStringField(payload,"archiveFile");
    cmd.indexFile=extractJsonStringField(payload,"indexFile");
    cmd.packFile=extractJsonStringField(payload,"packFile");
    cmd.blobFiles=extractJsonStringField(payload,"blobFiles");
    cmd.sourceRoot=extractJsonStringField(payload,"sourceRoot");
    cmd.entryType=extractJsonStringField(payload,"entryType");
    return cmd;}}
namespace keeply {
struct KeeplyAgentWsClient::TlsState{
    SSL_CTX* ctx=nullptr;
    SSL* ssl=nullptr;
    ~TlsState(){if(ssl){SSL_shutdown(ssl);SSL_free(ssl);}if(ctx) SSL_CTX_free(ctx);}};
void KeeplyWsHub::addClient(IWsClientSession* client){
    if(!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_[client->id()]=client;}
void KeeplyWsHub::removeClient(IWsClientSession* client){
    if(!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_.erase(client->id());
    clientJobSubscription_.erase(client->id());}
void KeeplyWsHub::broadcastJson(const std::string& json){
    std::vector<IWsClientSession*> snapshot;
    {std::lock_guard<std::mutex> lock(mu_);snapshot.reserve(clients_.size());for(auto& kv:clients_) snapshot.push_back(kv.second);}
    for(auto* c:snapshot) sendSafe_(c,json);}
void KeeplyWsHub::publishJobEvent(const std::string& jobId,const std::string& json){
    std::vector<IWsClientSession*> targets;
    {std::lock_guard<std::mutex> lock(mu_);
        for(auto& [cid,c]:clients_){
            auto it=clientJobSubscription_.find(cid);
            if(it!=clientJobSubscription_.end()&&it->second==jobId) targets.push_back(c);}}
    for(auto* c:targets) sendSafe_(c,json);}
void KeeplyWsHub::onClientMessage(IWsClientSession* client,const std::string& payload){
    if(!client) return;
    if(payload=="ping"){sendSafe_(client,R"({"type":"pong"})");return;}
    const std::string prefix="sub:";
    if(payload.rfind(prefix,0)==0){
        const std::string jobId=payload.substr(prefix.size());
        {std::lock_guard<std::mutex> lock(mu_);clientJobSubscription_[client->id()]=jobId;}
        sendSafe_(client,std::string("{\"type\":\"subscribed\",\"jobId\":\"")+escapeJson(jobId)+"\"}");
        return;}
    sendSafe_(client,R"({"type":"error","message":"Mensagem WS nao suportada no MVP"})");}
void KeeplyWsHub::sendSafe_(IWsClientSession* client,const std::string& payload){
    if(!client) return;
    try{client->sendText(payload);}
    catch(const std::exception& e){std::cerr<<"[ws] sendText falhou para client "<<client->id()<<": "<<e.what()<<"\n";}
    catch(...){std::cerr<<"[ws] sendText falhou para client "<<client->id()<<": erro desconhecido\n";}}
KeeplyAgentWsClient::KeeplyAgentWsClient(std::shared_ptr<KeeplyApi> api,AgentIdentity identity):api_(std::move(api)),identity_(std::move(identity)){
    if(!api_) throw std::runtime_error("KeeplyAgentWsClient requer KeeplyApi.");}
KeeplyAgentWsClient::~KeeplyAgentWsClient(){try{close();}catch(...){}}
void KeeplyAgentWsClient::connect(const WsClientConfig& config){
    close();
    config_=config;
    if(trim(config_.agentId).empty()) throw std::runtime_error("deviceId do websocket nao pode ser vazio.");
    const ParsedUrl parsed=parseUrlCommon(config_.url);
    if(parsed.scheme!="ws"&&parsed.scheme!="wss") throw std::runtime_error("Somente URLs ws:// ou wss:// sao suportadas.");
    UrlParts url;url.scheme=parsed.scheme;url.host=parsed.host;url.port=parsed.port;url.target=parsed.target;
    connectSocket_(url);
    try{
        performHandshake_(url);
        {std::lock_guard<std::mutex> lock(mu_);connected_=true;closeSent_=false;}
        sendHello_();
    }catch(...){close();throw;}}
void KeeplyAgentWsClient::run(){
    constexpr std::size_t kMaxMsgsPerWindow=60;
    constexpr auto kWindow=std::chrono::minutes(1);
    constexpr auto kKeepAliveInterval=std::chrono::seconds(25);
    constexpr auto kKeepAlivePongTimeout=std::chrono::seconds(15);
    std::deque<std::chrono::steady_clock::time_point> msgTimestamps;
    std::size_t droppedCount=0;
    auto waitForReadable=[this](std::chrono::milliseconds timeout){
        if(timeout<decltype(timeout)::zero()) timeout=decltype(timeout)::zero();
        for(;;){
            int fd=-1;
            std::shared_ptr<TlsState> tls;
            bool hasBufferedData=false;{
                std::lock_guard<std::mutex> lock(mu_);
                fd=sockfd_;
                tls=tls_;
                hasBufferedData=!recvBuffer_.empty();}
            if(fd<0) return false;
            if(hasBufferedData||(tls&&tls->ssl&&SSL_pending(tls->ssl)>0)) return true;
            fd_set readfds;
            FD_ZERO(&readfds);
            FD_SET(fd,&readfds);
            timeval tv{};
            tv.tv_sec=static_cast<long>(timeout.count()/1000);
            tv.tv_usec=static_cast<long>((timeout.count()%1000)*1000);
            const int rc=::select(fd+1,&readfds,nullptr,nullptr,&tv);
            if(rc>0) return true;
            if(rc==0) return false;
            const int err=keeply::http_internal::lastSocketError();
            if(keeply::http_internal::socketInterrupted(err)) continue;
            throw std::runtime_error("select falhou: "+keeply::http_internal::socketErrorMessage(err));}};
    auto nextKeepAliveAt=std::chrono::steady_clock::now()+kKeepAliveInterval;
    std::optional<std::chrono::steady_clock::time_point> pongDeadline;
    for(;;){
        const auto now=std::chrono::steady_clock::now();
        auto waitUntil=nextKeepAliveAt;
        if(pongDeadline&&*pongDeadline<waitUntil) waitUntil=*pongDeadline;
        const auto waitFor=waitUntil>now
            ? std::chrono::duration_cast<std::chrono::milliseconds>(waitUntil-now)
            : std::chrono::milliseconds::zero();
        if(!waitForReadable(waitFor)){
            const auto timeoutNow=std::chrono::steady_clock::now();
            if(pongDeadline&&timeoutNow>=*pongDeadline){
                throw std::runtime_error("Timeout aguardando pong do servidor websocket.");}
            if(timeoutNow>=nextKeepAliveAt){
                sendFrame_(0x9,"keepalive");
                nextKeepAliveAt=timeoutNow+kKeepAliveInterval;
                pongDeadline=timeoutNow+kKeepAlivePongTimeout;
            }continue;}
        unsigned char opcode=0;
        std::string payload;
        if(!readFrame_(opcode,payload)) break;
        nextKeepAliveAt=std::chrono::steady_clock::now()+kKeepAliveInterval;
        pongDeadline.reset();
        if(opcode==0x1){
            const auto now=std::chrono::steady_clock::now();
            while(!msgTimestamps.empty()&&(now-msgTimestamps.front())>kWindow)
                msgTimestamps.pop_front();
            if(msgTimestamps.size()>=kMaxMsgsPerWindow){
                ++droppedCount;
                if(droppedCount==1||droppedCount%10==0){
                    std::cerr<<"[keeply][ws][warn] rate limit: "<<droppedCount<<" mensagens ignoradas.\n";}continue;}
            msgTimestamps.push_back(now);
            if(droppedCount>0){
                std::cerr<<"[keeply][ws][info] rate limit normalizado após "<<droppedCount<<" mensagens ignoradas.\n";
                droppedCount=0;}
            handleServerMessage_(payload);
            continue;}
        if(opcode==0x8){try{sendClose_(1000,"");}catch(...){}
            break;}
        if(opcode==0x9){sendPong_(payload);continue;}
        if(opcode==0xA) continue;
        throw std::runtime_error("Opcode websocket nao suportado recebido do servidor.");}
    close();}
void KeeplyAgentWsClient::close(){
    bool shouldSendClose=false;
    {std::lock_guard<std::mutex> lock(mu_);shouldSendClose=connected_&&!closeSent_&&sockfd_>=0;}
    if(shouldSendClose){try{sendClose_(1000,"client.shutdown");}catch(...){}}
    int fd=-1;
    std::shared_ptr<TlsState> tls;{
        std::lock_guard<std::mutex> ioLock(ioMu_);
        std::lock_guard<std::mutex> lock(mu_);
        fd=sockfd_;
        sockfd_=-1;
        connected_=false;
        closeSent_=true;
        recvBuffer_.clear();
        tls=std::move(tls_);}
    if(fd>=0){
        ::shutdown(fd, SHUT_RDWR);
        keeply::http_internal::closeSocketFd(fd);}}
bool KeeplyAgentWsClient::isConnected() const{std::lock_guard<std::mutex> lock(mu_);return connected_;}
void KeeplyAgentWsClient::sendText(const std::string& payload){sendFrame_(0x1,payload);}

void KeeplyAgentWsClient::connectSocket_(const UrlParts& url){
    const int fd=openTcpSocket(url.host,url.port);
    {std::lock_guard<std::mutex> lock(mu_);sockfd_=fd;recvBuffer_.clear();}
    if(url.scheme=="wss") configureTls_(url);}
void KeeplyAgentWsClient::configureTls_(const UrlParts& url){
    keeply::TlsContextConfig tlsCfg;
    tlsCfg.host=url.host;
    tlsCfg.allowInsecure=config_.allowInsecureTls;
    if(!identity_.certPemPath.empty()&&!identity_.keyPemPath.empty()){
        tlsCfg.certPemPath=identity_.certPemPath;
        tlsCfg.keyPemPath=identity_.keyPemPath;}
    auto tls=std::make_shared<TlsState>();
    tls->ssl=keeply::createConnectedTlsSession([&]{
        std::lock_guard<std::mutex> lock(mu_);return sockfd_;}(),tlsCfg,tls->ctx);
    std::lock_guard<std::mutex> lock(mu_);
    tls_=std::move(tls);}
void KeeplyAgentWsClient::performHandshake_(const UrlParts& url){
    const std::string key=randomBase64(16);
    std::string target=url.target;
    const std::string separator=target.find('?')==std::string::npos?"?":"&";
    target+=separator+std::string("deviceId=")+urlEncode(config_.agentId);
    std::ostringstream req;
    req<<"GET "<<target<<" HTTP/1.1\r\n";
    req<<"Host: "<<url.host<<":"<<url.port<<"\r\n";
    req<<"Upgrade: websocket\r\n";
    req<<"Connection: Upgrade\r\n";
    req<<"Sec-WebSocket-Key: "<<key<<"\r\n";
    req<<"Sec-WebSocket-Version: 13\r\n";
    req<<"User-Agent: keeply-agent/1.0\r\n";
    req<<"\r\n";
    const std::string request=req.str();
    writeRaw_(request.data(),request.size());
    const std::string header=readHttpHeader_();
    std::istringstream iss(header);
    std::string statusLine;
    std::getline(iss,statusLine);
    if(statusLine.find("101")==std::string::npos) throw std::runtime_error("Handshake websocket rejeitado pelo backend: "+trim(statusLine));
    unsigned char sha[SHA_DIGEST_LENGTH];
    const std::string acceptInput=key+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    SHA1((const unsigned char*)acceptInput.data(),acceptInput.size(),sha);
    const std::string expectedAccept=base64Encode(sha,sizeof(sha));
    bool hasUpgrade=false,hasConnection=false,hasAccept=false;
    std::string line;
    while(std::getline(iss,line)){
        if(!line.empty()&&line.back()=='\r') line.pop_back();
        const std::size_t colon=line.find(':');
        if(colon==std::string::npos) continue;
        const std::string keyName=toLower(trim(line.substr(0,colon)));
        const std::string value=trim(line.substr(colon+1));
        const std::string lowerValue=toLower(value);
        if(keyName=="upgrade"&&lowerValue=="websocket") hasUpgrade=true;
        if(keyName=="connection"&&lowerValue.find("upgrade")!=std::string::npos) hasConnection=true;
        if(keyName=="sec-websocket-accept"&&value==expectedAccept) hasAccept=true;}
    if(!hasUpgrade||!hasConnection||!hasAccept) throw std::runtime_error("Handshake websocket invalido recebido do backend.");}
void KeeplyAgentWsClient::handleServerMessage_(const std::string& payload){
    if(payload.empty()) return;
    try{
        const WsCommand cmd=payload.front()=='{'?parseJsonCommand(payload):parseLegacyCommand(payload);
        executeCommand_(cmd);
    }catch(const std::exception& e){
        sendJson_(std::string("{\"type\":\"error\",\"message\":\"")+escapeJson(e.what())+"\"}");}}
void KeeplyAgentWsClient::executeCommand_(const WsCommand& cmd){
    if(cmd.type=="ping"){sendJson_(R"({"type":"pong"})");return;}
    if(cmd.type=="state"){sendJson_(buildStateJson_());return;}
    if(cmd.type=="snapshots"){sendJson_(buildSnapshotsJson_());return;}
    if(cmd.type=="fs.list"){sendJson_(buildFsListJson_(cmd.requestId,cmd.path));return;}
    if(cmd.type=="fs.disks"){sendJson_(buildFsDisksJson_(cmd.requestId));return;}
    if(cmd.type=="scan.scope"){
        if(cmd.scopeId.empty()) throw std::runtime_error("scan.scope requer scopeId.");
        api_->setScanScope(cmd.scopeId);
        std::ostringstream oss;
        oss<<"{"<<jsonStrField("type","scan.scope.updated")<<","<<jsonStrField("agentId",config_.agentId)<<","<<jsonStrField("source",api_->state().source)<<","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
        sendJson_(oss.str());
        return;}
    if(cmd.type=="config.source"){
        api_->setSource(cmd.path);
        std::ostringstream oss;
        oss<<"{"<<jsonStrField("type","config.updated")<<","<<jsonStrField("field","source")<<","<<jsonStrField("agentId",config_.agentId)<<","<<jsonStrField("source",api_->state().source)<<","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
        sendJson_(oss.str());
        return;}
    if(cmd.type=="config.archive"){api_->setArchive(cmd.path);sendJson_(R"({"type":"config.updated","field":"archive"})");return;}
    if(cmd.type=="config.restoreRoot"){api_->setRestoreRoot(cmd.path);sendJson_(R"({"type":"config.updated","field":"restoreRoot"})");return;}
    if(cmd.type=="backup"){
        if(!cmd.sourcePath.empty()) api_->setSource(cmd.sourcePath);
        runBackupCommand_(cmd.label,cmd.storage);
        return;}
    if(cmd.type=="restore.file"){runRestoreFileCommand_(cmd.snapshot,cmd.relPath,cmd.outRoot);return;}
    if(cmd.type=="restore.snapshot"){runRestoreSnapshotCommand_(cmd.snapshot,cmd.outRoot);return;}
    if(cmd.type=="restore.cloud.snapshot"){runRestoreCloudSnapshotCommand_(cmd);return;}
    throw std::runtime_error("Comando websocket nao suportado: "+(cmd.type.empty()?cmd.raw:cmd.type));}
void KeeplyAgentWsClient::sendHello_(){
    const auto& s=api_->state();
    std::ostringstream oss;
    oss<<"{"
       <<"\"type\":\"agent.hello\","
       <<"\"protocolVersion\":"<<keeply::kProtocolVersion<<","
       <<"\"deviceId\":\""<<escapeJson(config_.agentId)<<"\","
       <<"\"agentId\":\""<<escapeJson(config_.agentId)<<"\","
       <<"\"osName\":\""<<escapeJson(config_.osName)<<"\","
       <<"\"hostName\":\""<<escapeJson(config_.hostName)<<"\","
       <<"\"fingerprintSha256\":\""<<escapeJson(identity_.fingerprintSha256)<<"\","
       <<"\"connectedAt\":\""<<escapeJson(nowIsoLocal())<<"\","
       <<"\"source\":\""<<escapeJson(s.source)<<"\","
       <<"\"archive\":\""<<escapeJson(s.archive)<<"\","
       <<"\"restoreRoot\":\""<<escapeJson(s.restoreRoot)<<"\","
       <<"\"deviceDetails\":"<<buildDeviceDetailsJson_()<<","
       <<"\"scanScope\":"<<buildScanScopeJson_()
       <<"}";
    sendJson_(oss.str());}
void KeeplyAgentWsClient::sendJson_(const std::string& payload){
    if(!payload.empty()&&payload.front()=='{'){
        sendText(std::string("{\"v\":")+std::to_string(keeply::kProtocolVersion)+","+payload.substr(1));
    }else{
        sendText(payload);}}
void KeeplyAgentWsClient::sendPong_(const std::string& payload){sendFrame_(0xA,payload);}
void KeeplyAgentWsClient::sendClose_(std::uint16_t code,const std::string& reason){
    std::string payload;
    payload.push_back((char)((code>>8)&0xFF));
    payload.push_back((char)(code&0xFF));
    payload+=reason;
    sendFrame_(0x8,payload);
    std::lock_guard<std::mutex> lock(mu_);
    closeSent_=true;}
void KeeplyAgentWsClient::sendFrame_(unsigned char opcode,const std::string& payload){
    std::vector<unsigned char> frame;
    frame.reserve(payload.size()+16);
    frame.push_back((unsigned char)(0x80u|(opcode&0x0Fu)));
    const std::size_t len=payload.size();
    if(len<=125){
        frame.push_back((unsigned char)(0x80u|len));
    }else if(len<=0xFFFFu){
        frame.push_back(0x80u|126u);
        frame.push_back((unsigned char)((len>>8)&0xFFu));
        frame.push_back((unsigned char)(len&0xFFu));
    }else{
        frame.push_back(0x80u|127u);
        for(int shift=56;shift>=0;shift-=8) frame.push_back((unsigned char)(((std::uint64_t)len>>shift)&0xFFu));}
    std::array<unsigned char,4> mask{};
    std::random_device rd;
    for(auto& b:mask) b=(unsigned char)rd();
    frame.insert(frame.end(),mask.begin(),mask.end());
    for(std::size_t i=0;i<payload.size();++i) frame.push_back((unsigned char)payload[i]^mask[i%4]);
    writeRaw_(frame.data(),frame.size());}
std::string KeeplyAgentWsClient::readHttpHeader_(){
    for(;;){
        const std::size_t pos=recvBuffer_.find("\r\n\r\n");
        if(pos!=std::string::npos){
            const std::size_t end=pos+4;
            const std::string out=recvBuffer_.substr(0,end);
            recvBuffer_.erase(0,end);
            return out;}
        char tmp[4096];
        const std::size_t n=readRaw_(tmp,sizeof(tmp));
        if(n==0) throw std::runtime_error("Conexao fechada durante o handshake websocket.");
        recvBuffer_.append(tmp,n);
        if(recvBuffer_.size()>(1u<<20)) throw std::runtime_error("Cabecalho websocket muito grande.");}}
bool KeeplyAgentWsClient::readFrame_(unsigned char& opcode,std::string& payload){
    auto ensureBytes=[&](std::size_t needed){
        while(recvBuffer_.size()<needed){
            char tmp[4096];
            const std::size_t n=readRaw_(tmp,sizeof(tmp));
            if(n==0) return false;
            recvBuffer_.append(tmp,n);}
        return true;
    };
    if(!ensureBytes(2)) return false;
    const unsigned char b0=(unsigned char)recvBuffer_[0];
    const unsigned char b1=(unsigned char)recvBuffer_[1];
    const bool fin=(b0&0x80u)!=0;
    opcode=(unsigned char)(b0&0x0Fu);
    const bool masked=(b1&0x80u)!=0;
    std::uint64_t len=(std::uint64_t)(b1&0x7Fu);
    std::size_t offset=2;
    if(len==126){
        if(!ensureBytes(4)) return false;
        len=((std::uint64_t)(unsigned char)recvBuffer_[2]<<8)|(std::uint64_t)(unsigned char)recvBuffer_[3];
        offset=4;
    }else if(len==127){
        if(!ensureBytes(10)) return false;
        len=0;
        for(std::size_t i=0;i<8;++i) len=(len<<8)|(std::uint64_t)(unsigned char)recvBuffer_[2+i];
        offset=10;}
    std::array<unsigned char,4> mask{};
    if(masked){
        if(!ensureBytes(offset+4)) return false;
        for(std::size_t i=0;i<mask.size();++i) mask[i]=(unsigned char)recvBuffer_[offset+i];
        offset+=4;}
    if(len>(std::uint64_t)std::numeric_limits<std::size_t>::max()) throw std::runtime_error("Frame websocket grande demais.");
    const std::size_t payloadLen=(std::size_t)len;
    if(!ensureBytes(offset+payloadLen)) return false;
    payload.assign(recvBuffer_.data()+(std::ptrdiff_t)offset,payloadLen);
    recvBuffer_.erase(0,offset+payloadLen);
    if(masked) for(std::size_t i=0;i<payload.size();++i) payload[i]=(char)((unsigned char)payload[i]^mask[i%mask.size()]);
    if(!fin) throw std::runtime_error("Frames websocket fragmentados nao sao suportados neste cliente.");
    return true;}
std::string KeeplyAgentWsClient::buildScanScopeJson_() const{
    const auto& scanScope=api_->state().scanScope;
    std::ostringstream oss;
    oss<<"{"<<"\"id\":\""<<escapeJson(scanScope.id)<<"\","<<"\"label\":\""<<escapeJson(scanScope.label)<<"\","<<"\"requestedPath\":\""<<escapeJson(scanScope.requestedPath)<<"\","<<"\"resolvedPath\":\""<<escapeJson(scanScope.resolvedPath)<<"\""<<"}";
    return oss.str();}
std::string KeeplyAgentWsClient::buildDeviceDetailsJson_() const{
    std::ostringstream oss;
    oss<<"{";
    oss<<"\"localIps\":[";
    for(std::size_t i=0;i<config_.localIps.size();++i){
        if(i>0) oss<<",";
        oss<<"\""<<escapeJson(config_.localIps[i])<<"\"";
    }
    oss<<"],";
    oss<<"\"hardware\":{";
    oss<<"\"cpuModel\":\""<<escapeJson(config_.cpuModel)<<"\",";
    oss<<"\"cpuArchitecture\":\""<<escapeJson(config_.cpuArchitecture)<<"\",";
    oss<<"\"kernelVersion\":\""<<escapeJson(config_.kernelVersion)<<"\",";
    oss<<"\"cpuCores\":"<<config_.cpuCores<<",";
    oss<<"\"totalMemoryBytes\":"<<config_.totalMemoryBytes;
    oss<<"}";
    oss<<"}";
    return oss.str();}
std::string KeeplyAgentWsClient::buildStateJson_() const{
    const auto& s=api_->state();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"state\","<<"\"deviceId\":\""<<escapeJson(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson(config_.agentId)<<"\","<<"\"source\":\""<<escapeJson(s.source)<<"\","<<"\"archive\":\""<<escapeJson(s.archive)<<"\","<<"\"restoreRoot\":\""<<escapeJson(s.restoreRoot)<<"\","<<"\"deviceDetails\":"<<buildDeviceDetailsJson_()<<","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"archiveSplitEnabled\":"<<(s.archiveSplitEnabled?"true":"false")<<","<<"\"archiveSplitMaxBytes\":"<<s.archiveSplitMaxBytes<<"}";
    return oss.str();}
std::string KeeplyAgentWsClient::buildSnapshotsJson_(){
    const auto rows=api_->listSnapshots();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"snapshots\","<<"\"deviceId\":\""<<escapeJson(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson(config_.agentId)<<"\","<<"\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& r=rows[i];
        oss<<"{"<<"\"id\":"<<r.id<<","<<"\"createdAt\":\""<<escapeJson(r.createdAt)<<"\","<<"\"sourceRoot\":\""<<escapeJson(r.sourceRoot)<<"\","<<"\"label\":\""<<escapeJson(r.label)<<"\","<<"\"fileCount\":"<<r.fileCount<<"}";}
    oss<<"]}";
    return oss.str();}
std::size_t KeeplyAgentWsClient::writeRaw_(const void* data,std::size_t size){
    std::shared_ptr<TlsState> tls;
    int fd=-1;{
        std::lock_guard<std::mutex> ioLock(ioMu_);
        std::lock_guard<std::mutex> lock(mu_);
        tls=tls_;
        fd=sockfd_;
        if(fd<0) throw std::runtime_error("Socket do agente nao esta inicializado.");
        if(tls&&tls->ssl) return writeAllSsl(tls->ssl,data,size);
        return writeAllFd(fd,data,size);}}
std::size_t KeeplyAgentWsClient::readRaw_(void* data,std::size_t size){
    std::shared_ptr<TlsState> tls;
    int fd=-1;{
        std::lock_guard<std::mutex> ioLock(ioMu_);
        std::lock_guard<std::mutex> lock(mu_);
        tls=tls_;
        fd=sockfd_;
        if(fd<0) throw std::runtime_error("Socket do agente nao esta inicializado.");
        if(tls&&tls->ssl) return readSomeSsl(tls->ssl,data,size);
        return readSomeFd(fd,data,size);}}
void KeeplyAgentWsClient::ensureConnected_() const{
    if(!connected_||sockfd_<0) throw std::runtime_error("Cliente websocket do agente nao esta conectado.");}
}


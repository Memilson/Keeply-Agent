#include "websocket_agente.hpp"
#include "websocket_interno.hpp"
#include "../http_interno.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif
#include <openssl/sha.h>
#include <openssl/ssl.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdio>
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

struct FsListRow{
    bool isDir=false;
    std::string name;
    std::string fullPath;
    std::uintmax_t size=0;
};

struct LsblkEntry{
    std::string name;
    std::string label;
    std::string model;
    std::string type;
    std::string mountpoint;
    std::string parentName;
};

struct DiskMountCandidate{
    std::string label;
    std::string mountpoint;
    int score=0;
};

std::string jsonBool(bool v){return v?"true":"false";}
std::string jsonStrField(const std::string& key,const std::string& value){return "\"" + key + "\":\"" + escapeJson(value) + "\"";}
std::string jsonNumField(const std::string& key,std::uintmax_t value){return "\"" + key + "\":" + std::to_string(value);}
std::string jsonBoolField(const std::string& key,bool value){return "\"" + key + "\":" + jsonBool(value);}
std::string storageModeToString(BackupStorageMode mode){
    switch(mode){
        case BackupStorageMode::CloudOnly: return "cloud_only";
        case BackupStorageMode::Hybrid: return "local_then_cloud";
        case BackupStorageMode::LocalOnly:
        default: return "local_only";
    }
}

std::map<std::string,std::string> parseKeyValueLine(const std::string& line){
    std::map<std::string,std::string> out;
    std::size_t i=0;
    while(i<line.size()){
        while(i<line.size()&&std::isspace(static_cast<unsigned char>(line[i]))) ++i;
        if(i>=line.size()) break;
        const std::size_t eq=line.find('=',i);
        if(eq==std::string::npos) break;
        const std::string key=line.substr(i,eq-i);
        i=eq+1;
        std::string value;
        if(i<line.size()&&line[i]=='"'){
            ++i;
            while(i<line.size()){
                if(line[i]=='"'&&(i==0||line[i-1]!='\\')){
                    ++i;
                    break;
                }
                value.push_back(line[i++]);
            }
        }else{
            const std::size_t next=line.find(' ',i);
            value=line.substr(i,next==std::string::npos?std::string::npos:next-i);
            i=next==std::string::npos?line.size():next;
        }
        out[key]=value;
    }
    return out;
}

bool shouldSkipMountType(const std::string& fsType){
    static const std::set<std::string> skipped={
        "proc","sysfs","tmpfs","devtmpfs","devpts","cgroup","cgroup2","mqueue","overlay","squashfs",
        "nsfs","tracefs","securityfs","pstore","debugfs","configfs","fusectl","binfmt_misc","ramfs",
        "autofs","hugetlbfs","rpc_pipefs","selinuxfs","bpf"
    };
    return skipped.find(fsType)!=skipped.end();
}

void appendRootRow(std::vector<FsListRow>& rows,std::unordered_set<std::string>& seen,const fs::path& path){
    if(path.empty()) return;
    std::error_code ec;
    const fs::path absolutePath=fs::absolute(path,ec);
    const fs::path finalPath=ec?path:absolutePath;
    const std::string fullPath=finalPath.string();
    if(fullPath.empty()||seen.count(fullPath)) return;
    if(!fs::exists(finalPath,ec)||ec||!fs::is_directory(finalPath,ec)||ec) return;
    seen.insert(fullPath);
    FsListRow row;
    row.isDir=true;
    row.fullPath=fullPath;
    row.name=finalPath.filename().string();
    if(row.name.empty()) row.name=fullPath;
    rows.push_back(std::move(row));
}

std::vector<FsListRow> listRootRows(){
    std::vector<FsListRow> rows;
    std::unordered_set<std::string> seen;
#if defined(_WIN32)
    for(char drive='A';drive<='Z';++drive){
        const std::string root=std::string(1,drive)+":\\";
        appendRootRow(rows,seen,fs::path(root));
    }
#else
#if defined(__linux__)
    std::vector<LsblkEntry> entries;
    if(FILE* pipe=popen("lsblk -P -o NAME,LABEL,MODEL,TYPE,MOUNTPOINT,PKNAME 2>/dev/null","r")){
        char buffer[4096];
        while(std::fgets(buffer,sizeof(buffer),pipe)){
            const auto kv=parseKeyValueLine(buffer);
            LsblkEntry entry;
            entry.name=keeply::trim(kv.count("NAME")?kv.at("NAME"):"");
            entry.label=keeply::trim(kv.count("LABEL")?kv.at("LABEL"):"");
            entry.model=keeply::trim(kv.count("MODEL")?kv.at("MODEL"):"");
            entry.type=keeply::trim(kv.count("TYPE")?kv.at("TYPE"):"");
            entry.mountpoint=keeply::trim(kv.count("MOUNTPOINT")?kv.at("MOUNTPOINT"):"");
            entry.parentName=keeply::trim(kv.count("PKNAME")?kv.at("PKNAME"):"");
            if(!entry.name.empty()) entries.push_back(std::move(entry));
        }
        pclose(pipe);
    }
    std::map<std::string,std::string> diskLabels;
    for(const auto& entry:entries){
        if(entry.type!="disk"||entry.name.rfind("loop",0)==0) continue;
        const std::string trimmedLabel=keeply::trim(entry.label);
        const std::string trimmedModel=keeply::trim(entry.model);
        diskLabels[entry.name]=!trimmedLabel.empty()?trimmedLabel:!trimmedModel.empty()?trimmedModel:entry.name;
    }
    std::map<std::string,DiskMountCandidate> bestMountByDisk;
    for(const auto& entry:entries){
        if(entry.mountpoint.empty()||entry.type=="loop") continue;
        std::string diskKey=!entry.parentName.empty()?entry.parentName:entry.name;
        if(diskKey.rfind("loop",0)==0) continue;
        std::string label=diskLabels.count(diskKey)?diskLabels[diskKey]:(!entry.model.empty()?entry.model:!entry.label.empty()?entry.label:diskKey);
        int score=10;
        if(entry.mountpoint=="/") score=100;
        else if(entry.mountpoint.rfind("/media/",0)==0||entry.mountpoint.rfind("/run/media/",0)==0) score=80;
        else if(entry.mountpoint.rfind("/mnt/",0)==0) score=70;
        else if(entry.mountpoint=="/boot/efi") score=5;
        auto it=bestMountByDisk.find(diskKey);
        if(it==bestMountByDisk.end()||score>it->second.score) bestMountByDisk[diskKey]=DiskMountCandidate{label,entry.mountpoint,score};
    }
    for(const auto& [diskKey,candidate]:bestMountByDisk){
        FsListRow row;
        row.isDir=true;
        row.name=candidate.label;
        row.fullPath=candidate.mountpoint;
        if(!row.fullPath.empty()&&!seen.count(row.fullPath)){
            seen.insert(row.fullPath);
            rows.push_back(std::move(row));
        }
    }
    if(rows.empty()){
        std::ifstream mounts("/proc/mounts");
        std::string device;
        std::string mountPoint;
        std::string fsType;
        while(mounts>>device>>mountPoint>>fsType){
            std::string restOfLine;
            std::getline(mounts,restOfLine);
            if(shouldSkipMountType(fsType)) continue;
            if(mountPoint=="/"||mountPoint.rfind("/mnt/",0)==0||mountPoint.rfind("/media/",0)==0||mountPoint.rfind("/run/media/",0)==0) appendRootRow(rows,seen,fs::path(mountPoint));
        }
    }
#elif defined(__APPLE__)
    appendRootRow(rows,seen,fs::path("/"));
    std::error_code volumesEc;
    for(fs::directory_iterator it(fs::path("/Volumes"),fs::directory_options::skip_permission_denied,volumesEc),end;it!=end;it.increment(volumesEc)){
        if(volumesEc){
            volumesEc.clear();
            continue;
        }
        appendRootRow(rows,seen,it->path());
    }
#else
    appendRootRow(rows,seen,fs::path("/"));
#endif
#endif
    std::sort(rows.begin(),rows.end(),[](const FsListRow& a,const FsListRow& b){return toLower(a.name)<toLower(b.name);});
    return rows;
}

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
            cmd.path=args.substr(sep+1);
        }
        return cmd;
    }
    if(payload.rfind("fs.disks:",0)==0){
        cmd.type="fs.disks";
        cmd.requestId=payload.substr(9);
        return cmd;
    }
    if(payload.rfind("scan.scope:",0)==0){
        cmd.type="scan.scope";
        cmd.scopeId=payload.substr(11);
        return cmd;
    }
    if(payload.rfind("config.source:",0)==0){
        cmd.type="config.source";
        cmd.path=payload.substr(14);
        return cmd;
    }
    if(payload.rfind("config.archive:",0)==0){
        cmd.type="config.archive";
        cmd.path=payload.substr(15);
        return cmd;
    }
    if(payload.rfind("config.restoreRoot:",0)==0){
        cmd.type="config.restoreRoot";
        cmd.path=payload.substr(19);
        return cmd;
    }
    if(payload.rfind("backup:",0)==0){
        cmd.type="backup";
        const auto parts=splitPipe(payload.substr(7));
        if(!parts.empty()) cmd.label=parts[0];
        for(std::size_t i=1;i<parts.size();++i){
            if(parts[i].rfind("storage=",0)==0) cmd.storage=parts[i].substr(8);
        }
        return cmd;
    }
    if(payload.rfind("restore.file:",0)==0){
        cmd.type="restore.file";
        const auto parts=splitPipe(payload.substr(13));
        if(parts.size()>=1) cmd.snapshot=parts[0];
        if(parts.size()>=2) cmd.relPath=parts[1];
        if(parts.size()>=3) cmd.outRoot=parts[2];
        return cmd;
    }
    if(payload.rfind("restore.snapshot:",0)==0){
        cmd.type="restore.snapshot";
        const auto parts=splitPipe(payload.substr(17));
        if(parts.size()>=1) cmd.snapshot=parts[0];
        if(parts.size()>=2) cmd.outRoot=parts[1];
        return cmd;
    }
    cmd.type="unsupported";
    return cmd;
}

keeply::WsCommand parseJsonCommand(const std::string& payload){
    keeply::WsCommand cmd;
    cmd.raw=payload;
    cmd.type=keeply::trim(extractJsonStringField(payload,"type"));
    cmd.requestId=keeply::trim(extractJsonStringField(payload,"requestId"));
    cmd.path=keeply::trim(extractJsonStringField(payload,"path"));
    cmd.scopeId=keeply::trim(extractJsonStringField(payload,"scopeId"));
    cmd.label=extractJsonStringField(payload,"label");
    cmd.storage=extractJsonStringField(payload,"storage");
    cmd.snapshot=extractJsonStringField(payload,"snapshot");
    cmd.relPath=extractJsonStringField(payload,"relPath");
    if(cmd.relPath.empty()) cmd.relPath=cmd.path;
    cmd.outRoot=extractJsonStringField(payload,"outRoot");
    return cmd;
}

} // namespace

namespace keeply {

struct KeeplyAgentWsClient::TlsState{
    SSL_CTX* ctx=nullptr;
    SSL* ssl=nullptr;
    ~TlsState(){if(ssl){SSL_shutdown(ssl);SSL_free(ssl);}if(ctx) SSL_CTX_free(ctx);}
};

void KeeplyWsHub::addClient(IWsClientSession* client){
    if(!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_[client->id()]=client;
}

void KeeplyWsHub::removeClient(IWsClientSession* client){
    if(!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_.erase(client->id());
    clientJobSubscription_.erase(client->id());
}

void KeeplyWsHub::broadcastJson(const std::string& json){
    std::vector<IWsClientSession*> snapshot;
    {std::lock_guard<std::mutex> lock(mu_);snapshot.reserve(clients_.size());for(auto& kv:clients_) snapshot.push_back(kv.second);}
    for(auto* c:snapshot) sendSafe_(c,json);
}

void KeeplyWsHub::publishJobEvent(const std::string& jobId,const std::string& json){
    std::vector<IWsClientSession*> targets;
    {std::lock_guard<std::mutex> lock(mu_);
        for(auto& [cid,c]:clients_){
            auto it=clientJobSubscription_.find(cid);
            if(it!=clientJobSubscription_.end()&&it->second==jobId) targets.push_back(c);
        }
    }
    for(auto* c:targets) sendSafe_(c,json);
}

void KeeplyWsHub::onClientMessage(IWsClientSession* client,const std::string& payload){
    if(!client) return;
    if(payload=="ping"){sendSafe_(client,R"({"type":"pong"})");return;}
    const std::string prefix="sub:";
    if(payload.rfind(prefix,0)==0){
        const std::string jobId=payload.substr(prefix.size());
        {std::lock_guard<std::mutex> lock(mu_);clientJobSubscription_[client->id()]=jobId;}
        sendSafe_(client,std::string("{\"type\":\"subscribed\",\"jobId\":\"")+escapeJson(jobId)+"\"}");
        return;
    }
    sendSafe_(client,R"({"type":"error","message":"Mensagem WS nao suportada no MVP"})");
}

void KeeplyWsHub::sendSafe_(IWsClientSession* client,const std::string& payload){
    if(!client) return;
    try{client->sendText(payload);}
    catch(const std::exception& e){std::cerr<<"[ws] sendText falhou para client "<<client->id()<<": "<<e.what()<<"\n";}
    catch(...){std::cerr<<"[ws] sendText falhou para client "<<client->id()<<": erro desconhecido\n";}
}

KeeplyAgentWsClient::KeeplyAgentWsClient(std::shared_ptr<KeeplyApi> api,AgentIdentity identity):api_(std::move(api)),identity_(std::move(identity)){
    if(!api_) throw std::runtime_error("KeeplyAgentWsClient requer KeeplyApi.");
}

KeeplyAgentWsClient::~KeeplyAgentWsClient(){try{close();}catch(...){}} 

void KeeplyAgentWsClient::connect(const WsClientConfig& config){
    close();
    config_=config;
    if(trim(config_.agentId).empty()) throw std::runtime_error("deviceId do websocket nao pode ser vazio.");
    const UrlParts url=parseUrl_(config_.url);
    connectSocket_(url);
    try{
        performHandshake_(url);
        {std::lock_guard<std::mutex> lock(mu_);connected_=true;closeSent_=false;}
        sendHello_();
    }catch(...){close();throw;}
}

void KeeplyAgentWsClient::run(){
    for(;;){
        unsigned char opcode=0;
        std::string payload;
        if(!readFrame_(opcode,payload)) break;
        if(opcode==0x1){handleServerMessage_(payload);continue;}
        if(opcode==0x8){try{sendClose_(1000,"");}catch(...){}
            break;
        }
        if(opcode==0x9){sendPong_(payload);continue;}
        if(opcode==0xA) continue;
        throw std::runtime_error("Opcode websocket nao suportado recebido do servidor.");
    }
    close();
}

void KeeplyAgentWsClient::close(){
    bool shouldSendClose=false;
    {std::lock_guard<std::mutex> lock(mu_);shouldSendClose=connected_&&!closeSent_&&sockfd_>=0;}
    if(shouldSendClose){try{sendClose_(1000,"client.shutdown");}catch(...){}} 
    int fd=-1;
    std::shared_ptr<TlsState> tls;
    {
        std::lock_guard<std::mutex> ioLock(ioMu_);
        std::lock_guard<std::mutex> lock(mu_);
        fd=sockfd_;
        sockfd_=-1;
        connected_=false;
        closeSent_=true;
        recvBuffer_.clear();
        tls=std::move(tls_);
    }
    if(fd>=0){
#ifdef _WIN32
        ::shutdown(fd, SD_BOTH);
#else
        ::shutdown(fd, SHUT_RDWR);
#endif
        keeply::http_internal::closeSocketFd(fd);
    }
}

bool KeeplyAgentWsClient::isConnected() const{std::lock_guard<std::mutex> lock(mu_);return connected_;}

void KeeplyAgentWsClient::sendText(const std::string& payload){sendFrame_(0x1,payload);}

KeeplyAgentWsClient::UrlParts KeeplyAgentWsClient::parseUrl_(const std::string& url){
    const ParsedUrl parsed=parseUrlCommon(url);
    if(parsed.scheme!="ws"&&parsed.scheme!="wss") throw std::runtime_error("Somente URLs ws:// ou wss:// sao suportadas.");
    UrlParts out;out.scheme=parsed.scheme;out.host=parsed.host;out.port=parsed.port;out.target=parsed.target;
    return out;
}

std::string KeeplyAgentWsClient::escapeJson_(const std::string& value){return escapeJson(value);}
std::string KeeplyAgentWsClient::urlEncode_(const std::string& value){return urlEncode(value);}
std::string KeeplyAgentWsClient::httpUrlFromWsUrl_(const std::string& wsUrl,const std::string& path){return httpUrlFromWsUrl(wsUrl,path);}

void KeeplyAgentWsClient::connectSocket_(const UrlParts& url){
    const int fd=openTcpSocket(url.host,url.port);
    {std::lock_guard<std::mutex> lock(mu_);sockfd_=fd;recvBuffer_.clear();}
    if(url.scheme=="wss") configureTls_(url);
}

void KeeplyAgentWsClient::configureTls_(const UrlParts& url){
    auto tls=std::make_shared<TlsState>();
    tls->ctx=SSL_CTX_new(TLS_client_method());
    if(!tls->ctx) throw std::runtime_error("Falha ao criar SSL_CTX do websocket.");
    if(config_.allowInsecureTls){
        SSL_CTX_set_verify(tls->ctx,SSL_VERIFY_NONE,nullptr);
    }else{
        SSL_CTX_set_verify(tls->ctx,SSL_VERIFY_PEER,nullptr);
        if(SSL_CTX_set_default_verify_paths(tls->ctx)!=1) throw std::runtime_error("Falha ao configurar trust store do websocket.");
    }
    if(!identity_.certPemPath.empty()&&!identity_.keyPemPath.empty()){
        if(SSL_CTX_use_certificate_file(tls->ctx,identity_.certPemPath.string().c_str(),SSL_FILETYPE_PEM)!=1) throw std::runtime_error("Falha ao carregar certificado cliente para websocket.");
        if(SSL_CTX_use_PrivateKey_file(tls->ctx,identity_.keyPemPath.string().c_str(),SSL_FILETYPE_PEM)!=1) throw std::runtime_error("Falha ao carregar chave privada cliente para websocket.");
    }
    tls->ssl=SSL_new(tls->ctx);
    if(!tls->ssl) throw std::runtime_error("Falha ao criar SSL do websocket.");
    int fd=-1;
    {std::lock_guard<std::mutex> lock(mu_);fd=sockfd_;}
    SSL_set_fd(tls->ssl,fd);
    SSL_set_tlsext_host_name(tls->ssl,url.host.c_str());
    if(!config_.allowInsecureTls){
#if OPENSSL_VERSION_NUMBER >= 0x10002000L
        if(SSL_set1_host(tls->ssl,url.host.c_str())!=1) throw std::runtime_error("Falha ao configurar validacao de hostname do websocket.");
#endif
    }
    if(SSL_connect(tls->ssl)!=1) throw std::runtime_error("Falha no handshake TLS do websocket.");
    if(!config_.allowInsecureTls && SSL_get_verify_result(tls->ssl)!=X509_V_OK){
        throw std::runtime_error("Validacao do certificado TLS do websocket falhou.");
    }
    std::lock_guard<std::mutex> lock(mu_);
    tls_=std::move(tls);
}

void KeeplyAgentWsClient::performHandshake_(const UrlParts& url){
    const std::string key=randomBase64(16);
    std::string target=url.target;
    const std::string separator=target.find('?')==std::string::npos?"?":"&";
    target+=separator+std::string("deviceId=")+urlEncode_(config_.agentId);
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
        if(keyName=="sec-websocket-accept"&&value==expectedAccept) hasAccept=true;
    }
    if(!hasUpgrade||!hasConnection||!hasAccept) throw std::runtime_error("Handshake websocket invalido recebido do backend.");
}

void KeeplyAgentWsClient::handleServerMessage_(const std::string& payload){
    if(payload.empty()) return;
    try{
        const WsCommand cmd=payload.front()=='{'?parseJsonCommand(payload):parseLegacyCommand(payload);
        executeCommand_(cmd);
    }catch(const std::exception& e){
        sendJson_(std::string("{\"type\":\"error\",\"message\":\"")+escapeJson_(e.what())+"\"}");
    }
}

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
        return;
    }
    if(cmd.type=="config.source"){
        api_->setSource(cmd.path);
        std::ostringstream oss;
        oss<<"{"<<jsonStrField("type","config.updated")<<","<<jsonStrField("field","source")<<","<<jsonStrField("agentId",config_.agentId)<<","<<jsonStrField("source",api_->state().source)<<","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
        sendJson_(oss.str());
        return;
    }
    if(cmd.type=="config.archive"){api_->setArchive(cmd.path);sendJson_(R"({"type":"config.updated","field":"archive"})");return;}
    if(cmd.type=="config.restoreRoot"){api_->setRestoreRoot(cmd.path);sendJson_(R"({"type":"config.updated","field":"restoreRoot"})");return;}
    if(cmd.type=="backup"){runBackupCommand_(cmd.label,cmd.storage);return;}
    if(cmd.type=="restore.file"){runRestoreFileCommand_(cmd.snapshot,cmd.relPath,cmd.outRoot);return;}
    if(cmd.type=="restore.snapshot"){runRestoreSnapshotCommand_(cmd.snapshot,cmd.outRoot);return;}
    throw std::runtime_error("Comando websocket nao suportado: "+(cmd.type.empty()?cmd.raw:cmd.type));
}

void KeeplyAgentWsClient::runRestoreFileCommand_(const std::string& snapshot,const std::string& relPath,const std::string& outRootRaw){
    if(snapshot.empty()||relPath.empty()) throw std::runtime_error("Formato restore.file invalido. Use restore.file:snapshot|relPath|outRoot(opcional)");
    const std::optional<fs::path> outRoot=!outRootRaw.empty()?std::optional<fs::path>(fs::path(outRootRaw)):std::nullopt;
    api_->restoreFile(snapshot,relPath,outRoot);
    sendJson_(std::string("{\"type\":\"restore.file.finished\",\"snapshot\":\"")+escapeJson_(snapshot)+"\",\"path\":\""+escapeJson_(relPath)+"\"}");
}

void KeeplyAgentWsClient::runRestoreSnapshotCommand_(const std::string& snapshot,const std::string& outRootRaw){
    if(snapshot.empty()) throw std::runtime_error("Formato restore.snapshot invalido. Use restore.snapshot:snapshot|outRoot(opcional)");
    const std::optional<fs::path> outRoot=!outRootRaw.empty()?std::optional<fs::path>(fs::path(outRootRaw)):std::nullopt;
    api_->restoreSnapshot(snapshot,outRoot);
    sendJson_(std::string("{\"type\":\"restore.snapshot.finished\",\"snapshot\":\"")+escapeJson_(snapshot)+"\"}");
}

void KeeplyAgentWsClient::sendBackupProgress_(const std::string& label,const BackupProgress& progress){
    std::ostringstream progressJson;
    progressJson<<"{"<<"\"type\":\"backup.progress\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"phase\":\""<<escapeJson_(progress.phase)<<"\","<<"\"discoveryComplete\":"<<(progress.discoveryComplete?"true":"false")<<","<<"\"currentFile\":\""<<escapeJson_(progress.currentFile)<<"\","<<"\"filesQueued\":"<<progress.filesQueued<<","<<"\"filesCompleted\":"<<progress.filesCompleted<<","<<"\"filesScanned\":"<<progress.stats.scanned<<","<<"\"filesAdded\":"<<progress.stats.added<<","<<"\"filesUnchanged\":"<<progress.stats.reused<<","<<"\"chunksNew\":"<<progress.stats.uniqueChunksInserted<<","<<"\"bytesRead\":"<<progress.stats.bytesRead<<","<<"\"warnings\":"<<progress.stats.warnings<<"}";
    sendJson_(progressJson.str());
}

void KeeplyAgentWsClient::sendBackupFinished_(const std::string& label,const BackupStats& stats){
    const std::size_t chunksReused=stats.chunks>=stats.uniqueChunksInserted?stats.chunks-stats.uniqueChunksInserted:0;
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"backup.finished\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"filesScanned\":"<<stats.scanned<<","<<"\"filesAdded\":"<<stats.added<<","<<"\"filesUnchanged\":"<<stats.reused<<","<<"\"chunksNew\":"<<stats.uniqueChunksInserted<<","<<"\"chunksReused\":"<<chunksReused<<","<<"\"bytesRead\":"<<stats.bytesRead<<","<<"\"warnings\":"<<stats.warnings<<"}";
    sendJson_(oss.str());
}

void KeeplyAgentWsClient::sendBackupFailed_(const std::string& label,const BackupProgress& latestProgress,const std::string& message){
    std::ostringstream failedJson;
    failedJson<<"{"<<"\"type\":\"backup.failed\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"phase\":\"failed\","<<"\"discoveryComplete\":"<<(latestProgress.discoveryComplete?"true":"false")<<","<<"\"currentFile\":\""<<escapeJson_(latestProgress.currentFile)<<"\","<<"\"filesQueued\":"<<latestProgress.filesQueued<<","<<"\"filesCompleted\":"<<latestProgress.filesCompleted<<","<<"\"filesScanned\":"<<latestProgress.stats.scanned<<","<<"\"filesAdded\":"<<latestProgress.stats.added<<","<<"\"filesUnchanged\":"<<latestProgress.stats.reused<<","<<"\"chunksNew\":"<<latestProgress.stats.uniqueChunksInserted<<","<<"\"bytesRead\":"<<latestProgress.stats.bytesRead<<","<<"\"warnings\":"<<latestProgress.stats.warnings<<","<<"\"message\":\""<<escapeJson_(message)<<"\""<<"}";
    sendJson_(failedJson.str());
}

void KeeplyAgentWsClient::sendBackupLocalOnly_(const std::string& label,const std::string& storageMode){
    std::ostringstream localOnlyJson;
    localOnlyJson<<"{"<<"\"type\":\"backup.storage.applied\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson_(storageMode)<<"\","<<"\"message\":\"Backup mantido somente no destino local atribuido.\""<< "}";
    sendJson_(localOnlyJson.str());
}

void KeeplyAgentWsClient::runBackupUpload_(const std::string& label,const BackupStoragePolicy& storagePolicy){
    try{
        const std::string storageMode=storageModeToString(storagePolicy.mode);
        sendJson_(std::string("{\"type\":\"backup.upload.started\",\"label\":\"")+escapeJson_(label)+"\",\"source\":\""+escapeJson_(api_->state().source)+"\",\"scanScope\":"+buildScanScopeJson_()+",\"storageMode\":\""+escapeJson_(storageMode)+"\"}");
        const UploadBundleResult uploadResp=uploadArchiveBackup(
            config_,
            identity_,
            api_->state(),
            label,
            [this,label,&storageMode](const UploadProgressSnapshot& progress){
                std::ostringstream uploadProgressJson;
                uploadProgressJson<<"{"<<"\"type\":\"backup.upload.progress\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson_(storageMode)<<"\","<<"\"phase\":\"uploading\","<<"\"currentObject\":\""<<escapeJson_(progress.currentObject)<<"\","<<"\"bundleRole\":\""<<escapeJson_(progress.bundleRole)<<"\","<<"\"filesUploaded\":"<<progress.filesUploaded<<","<<"\"filesTotal\":"<<progress.filesTotal<<","<<"\"blobPartIndex\":"<<progress.blobPartIndex<<","<<"\"blobPartCount\":"<<progress.blobPartCount<<"}";
                sendJson_(uploadProgressJson.str());
            }
        );
        const std::string storageId=trim(extractJsonStringField(uploadResp.manifestResponse.body,"storageId"));
        const std::string storageScope=trim(extractJsonStringField(uploadResp.manifestResponse.body,"storageScope"));
        const std::string uri=trim(extractJsonStringField(uploadResp.manifestResponse.body,"uri"));
        const std::string bucket=trim(extractJsonStringField(uploadResp.manifestResponse.body,"bucket"));
        if(storagePolicy.deleteLocalAfterUpload) deleteLocalArchiveArtifacts(keeply::fs::path(api_->state().archive));
        std::ostringstream uploadedJson;
        uploadedJson<<"{"<<"\"type\":\"backup.uploaded\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson_(storageMode)<<"\","<<"\"storageId\":\""<<escapeJson_(storageId)<<"\","<<"\"storageScope\":\""<<escapeJson_(storageScope)<<"\","<<"\"bucket\":\""<<escapeJson_(bucket)<<"\","<<"\"uri\":\""<<escapeJson_(uri)<<"\","<<"\"bundleId\":\""<<escapeJson_(uploadResp.bundleId)<<"\","<<"\"filesUploaded\":"<<uploadResp.filesUploaded<<","<<"\"blobPartCount\":"<<uploadResp.blobPartCount<<"}";
        sendJson_(uploadedJson.str());
    }catch(const std::exception& uploadEx){
        const std::string storageMode=storageModeToString(storagePolicy.mode);
        std::ostringstream uploadFailedJson;
        uploadFailedJson<<"{"<<"\"type\":\"backup.upload.failed\","<<"\"label\":\""<<escapeJson_(label)<<"\","<<"\"source\":\""<<escapeJson_(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson_(storageMode)<<"\","<<"\"message\":\""<<escapeJson_(uploadEx.what())<<"\""<<"}";
        sendJson_(uploadFailedJson.str());
    }
}

void KeeplyAgentWsClient::runBackupCommand_(const std::string& label,const std::string& storage){
    BackupStoragePolicy storagePolicy;
    BackupProgress latestProgress;
    if(!storage.empty()) storagePolicy=parseBackupStoragePolicy(storage);
    std::cout<<"[backup] iniciado";
    if(!label.empty()) std::cout<<" | label="<<label;
    std::cout<<" | source="<<api_->state().source<<"\n";
    sendJson_(std::string("{\"type\":\"backup.started\",\"label\":\"")+escapeJson_(label)+"\",\"source\":\""+escapeJson_(api_->state().source)+"\",\"scanScope\":"+buildScanScopeJson_()+"}");
    try{
        const BackupStats stats=api_->runBackup(label,[this,label,&latestProgress](const BackupProgress& progress){
            latestProgress=progress;
            sendBackupProgress_(label,progress);
        });
        sendBackupFinished_(label,stats);
        if(storagePolicy.uploadCloud) runBackupUpload_(label,storagePolicy);
        else sendBackupLocalOnly_(label,storageModeToString(storagePolicy.mode));
        std::cout<<"[backup] concluido"<<" | scanned="<<stats.scanned<<" added="<<stats.added<<" unchanged="<<stats.reused<<" bytes="<<stats.bytesRead<<" warnings="<<stats.warnings<<"\n";
        std::cout<<"[agent] backup finalizado. Agente segue online aguardando comandos.\n";
    }catch(const std::exception& e){
        sendBackupFailed_(label,latestProgress,e.what());
        std::cout<<"[backup] falhou";
        if(!label.empty()) std::cout<<" | label="<<label;
        std::cout<<" | error="<<e.what()<<"\n";
        std::cout<<"[agent] agente segue online aguardando comandos.\n";
    }
}

void KeeplyAgentWsClient::sendHello_(){
    const auto& s=api_->state();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"agent.hello\","<<"\"deviceId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"fingerprintSha256\":\""<<escapeJson_(identity_.fingerprintSha256)<<"\","<<"\"connectedAt\":\""<<escapeJson_(nowIsoLocal())<<"\","<<"\"source\":\""<<escapeJson_(s.source)<<"\","<<"\"archive\":\""<<escapeJson_(s.archive)<<"\","<<"\"restoreRoot\":\""<<escapeJson_(s.restoreRoot)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<"}";
    sendJson_(oss.str());
}

void KeeplyAgentWsClient::sendJson_(const std::string& payload){sendText(payload);}
void KeeplyAgentWsClient::sendPong_(const std::string& payload){sendFrame_(0xA,payload);}

void KeeplyAgentWsClient::sendClose_(std::uint16_t code,const std::string& reason){
    std::string payload;
    payload.push_back((char)((code>>8)&0xFF));
    payload.push_back((char)(code&0xFF));
    payload+=reason;
    sendFrame_(0x8,payload);
    std::lock_guard<std::mutex> lock(mu_);
    closeSent_=true;
}

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
        for(int shift=56;shift>=0;shift-=8) frame.push_back((unsigned char)(((std::uint64_t)len>>shift)&0xFFu));
    }
    std::array<unsigned char,4> mask{};
    std::random_device rd;
    for(auto& b:mask) b=(unsigned char)rd();
    frame.insert(frame.end(),mask.begin(),mask.end());
    for(std::size_t i=0;i<payload.size();++i) frame.push_back((unsigned char)payload[i]^mask[i%4]);
    writeRaw_(frame.data(),frame.size());
}

std::string KeeplyAgentWsClient::readHttpHeader_(){
    for(;;){
        const std::size_t pos=recvBuffer_.find("\r\n\r\n");
        if(pos!=std::string::npos){
            const std::size_t end=pos+4;
            const std::string out=recvBuffer_.substr(0,end);
            recvBuffer_.erase(0,end);
            return out;
        }
        char tmp[4096];
        const std::size_t n=readRaw_(tmp,sizeof(tmp));
        if(n==0) throw std::runtime_error("Conexao fechada durante o handshake websocket.");
        recvBuffer_.append(tmp,n);
        if(recvBuffer_.size()>(1u<<20)) throw std::runtime_error("Cabecalho websocket muito grande.");
    }
}

bool KeeplyAgentWsClient::readFrame_(unsigned char& opcode,std::string& payload){
    auto ensureBytes=[&](std::size_t needed){
        while(recvBuffer_.size()<needed){
            char tmp[4096];
            const std::size_t n=readRaw_(tmp,sizeof(tmp));
            if(n==0) return false;
            recvBuffer_.append(tmp,n);
        }
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
        offset=10;
    }
    std::array<unsigned char,4> mask{};
    if(masked){
        if(!ensureBytes(offset+4)) return false;
        for(std::size_t i=0;i<mask.size();++i) mask[i]=(unsigned char)recvBuffer_[offset+i];
        offset+=4;
    }
    if(len>(std::uint64_t)std::numeric_limits<std::size_t>::max()) throw std::runtime_error("Frame websocket grande demais.");
    const std::size_t payloadLen=(std::size_t)len;
    if(!ensureBytes(offset+payloadLen)) return false;
    payload.assign(recvBuffer_.data()+(std::ptrdiff_t)offset,payloadLen);
    recvBuffer_.erase(0,offset+payloadLen);
    if(masked) for(std::size_t i=0;i<payload.size();++i) payload[i]=(char)((unsigned char)payload[i]^mask[i%mask.size()]);
    if(!fin) throw std::runtime_error("Frames websocket fragmentados nao sao suportados neste cliente.");
    return true;
}

std::string KeeplyAgentWsClient::buildScanScopeJson_() const{
    const auto& scanScope=api_->state().scanScope;
    std::ostringstream oss;
    oss<<"{"<<"\"id\":\""<<escapeJson_(scanScope.id)<<"\","<<"\"label\":\""<<escapeJson_(scanScope.label)<<"\","<<"\"requestedPath\":\""<<escapeJson_(scanScope.requestedPath)<<"\","<<"\"resolvedPath\":\""<<escapeJson_(scanScope.resolvedPath)<<"\""<<"}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildStateJson_() const{
    const auto& s=api_->state();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"state\","<<"\"deviceId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"source\":\""<<escapeJson_(s.source)<<"\","<<"\"archive\":\""<<escapeJson_(s.archive)<<"\","<<"\"restoreRoot\":\""<<escapeJson_(s.restoreRoot)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"archiveSplitEnabled\":"<<(s.archiveSplitEnabled?"true":"false")<<","<<"\"archiveSplitMaxBytes\":"<<s.archiveSplitMaxBytes<<"}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildSnapshotsJson_(){
    const auto rows=api_->listSnapshots();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"snapshots\","<<"\"deviceId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"agentId\":\""<<escapeJson_(config_.agentId)<<"\","<<"\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& r=rows[i];
        oss<<"{"<<"\"id\":"<<r.id<<","<<"\"createdAt\":\""<<escapeJson_(r.createdAt)<<"\","<<"\"sourceRoot\":\""<<escapeJson_(r.sourceRoot)<<"\","<<"\"label\":\""<<escapeJson_(r.label)<<"\","<<"\"fileCount\":"<<r.fileCount<<"}";
    }
    oss<<"]}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildFsDisksJson_(const std::string& requestId) const{
    const auto rows=listRootRows();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"fs.disks\","<<"\"requestId\":\""<<escapeJson_(trim(requestId))<<"\","<<"\"path\":\"\","<<"\"parentPath\":\"\","<<"\"truncated\":false,"<<"\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& row=rows[i];
        oss<<"{"<<"\"name\":\""<<escapeJson_(row.name)<<"\","<<"\"path\":\""<<escapeJson_(row.fullPath)<<"\","<<"\"kind\":\"dir\","<<"\"size\":0"<<"}";
    }
    oss<<"]}";
    return oss.str();
}

std::string KeeplyAgentWsClient::buildFsListJson_(const std::string& requestId,const std::string& requestedPath) const{
    std::string targetRaw=trim(requestedPath);
    const auto& s=api_->state();
    if(targetRaw.empty()) targetRaw=trim(s.source);
    std::error_code ec;
    fs::path targetPath=targetRaw.empty()?fs::current_path(ec):fs::path(targetRaw);
    if(ec){
        ec.clear();
        targetPath=fs::path(".");
    }
    if(!targetPath.is_absolute()){
        targetPath=fs::absolute(targetPath,ec);
        if(ec){
            std::ostringstream err;
            err<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson_(trim(requestId))<<"\",\"path\":\""<<escapeJson_(requestedPath)<<"\",\"error\":\"falha-ao-resolver-caminho\",\"items\":[]}";
            return err.str();
        }
    }
    if(!fs::exists(targetPath,ec)||ec){
        std::ostringstream err;
        err<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson_(trim(requestId))<<"\",\"path\":\""<<escapeJson_(targetPath.string())<<"\",\"error\":\"caminho-nao-encontrado\",\"items\":[]}";
        return err.str();
    }
    if(!fs::is_directory(targetPath,ec)||ec){
        ec.clear();
        const fs::path parent=targetPath.parent_path();
        if(parent.empty()||!fs::is_directory(parent,ec)||ec){
            std::ostringstream err;
            err<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson_(trim(requestId))<<"\",\"path\":\""<<escapeJson_(targetPath.string())<<"\",\"error\":\"caminho-nao-e-diretorio\",\"items\":[]}";
            return err.str();
        }
        targetPath=parent;
    }
    static constexpr std::size_t kFsListMaxItems=120;
    static constexpr std::size_t kFsListMaxEstimatedJsonBytes=32*1024;
    std::vector<FsListRow> rows;
    rows.reserve(kFsListMaxItems);
    bool truncated=false;
    std::size_t estimatedJsonBytes=256;
    for(fs::directory_iterator it(targetPath,fs::directory_options::skip_permission_denied,ec),end;it!=end;it.increment(ec)){
        if(ec){ec.clear();continue;}
        const fs::path itemPath=it->path();
        const std::string name=itemPath.filename().string();
        if(name.empty()) continue;
        FsListRow row;
        row.name=name;
        row.fullPath=itemPath.string();
        row.isDir=it->is_directory(ec);
        if(ec){
            ec.clear();
            row.isDir=false;
        }
        if(!row.isDir) continue;
        row.size=0;
        const std::size_t rowEstimate=row.name.size()+row.fullPath.size()+96u;
        if(!rows.empty()&&estimatedJsonBytes+rowEstimate>kFsListMaxEstimatedJsonBytes){
            truncated=true;
            break;
        }
        rows.push_back(std::move(row));
        estimatedJsonBytes+=rowEstimate;
        if(rows.size()>=kFsListMaxItems){
            truncated=true;
            break;
        }
    }
    std::sort(rows.begin(),rows.end(),[](const FsListRow& a,const FsListRow& b){
        if(a.isDir!=b.isDir) return a.isDir>b.isDir;
        return toLower(a.name)<toLower(b.name);
    });
    const std::string parentPath=targetPath.has_parent_path()?targetPath.parent_path().string():"";
    std::ostringstream oss;
    oss<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson_(trim(requestId))<<"\",\"path\":\""<<escapeJson_(targetPath.string())<<"\",\"parentPath\":\""<<escapeJson_(parentPath)<<"\",\"truncated\":"<<(truncated?"true":"false")<<",\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& row=rows[i];
        oss<<"{\"name\":\""<<escapeJson_(row.name)<<"\",\"path\":\""<<escapeJson_(row.fullPath)<<"\",\"kind\":\""<<(row.isDir?"dir":"file")<<"\",\"size\":"<<row.size<<"}";
    }
    oss<<"]}";
    return oss.str();
}

std::size_t KeeplyAgentWsClient::writeRaw_(const void* data,std::size_t size){
    std::shared_ptr<TlsState> tls;
    int fd=-1;
    {
        std::lock_guard<std::mutex> ioLock(ioMu_);
        std::lock_guard<std::mutex> lock(mu_);
        tls=tls_;
        fd=sockfd_;
        if(fd<0) throw std::runtime_error("Socket do agente nao esta inicializado.");
        if(tls&&tls->ssl) return writeAllSsl(tls->ssl,data,size);
        return writeAllFd(fd,data,size);
    }
}

std::size_t KeeplyAgentWsClient::readRaw_(void* data,std::size_t size){
    std::shared_ptr<TlsState> tls;
    int fd=-1;
    {
        std::lock_guard<std::mutex> ioLock(ioMu_);
        std::lock_guard<std::mutex> lock(mu_);
        tls=tls_;
        fd=sockfd_;
        if(fd<0) throw std::runtime_error("Socket do agente nao esta inicializado.");
        if(tls&&tls->ssl) return readSomeSsl(tls->ssl,data,size);
        return readSomeFd(fd,data,size);
    }
}

void KeeplyAgentWsClient::ensureConnected_() const{
    if(!connected_||sockfd_<0) throw std::runtime_error("Cliente websocket do agente nao esta conectado.");
}

} // namespace keeply

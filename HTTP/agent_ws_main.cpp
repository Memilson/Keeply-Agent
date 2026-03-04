#include "ws.hpp"
#include <filesystem>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#if defined(__linux__) || defined(__APPLE__)
#include <unistd.h>
#endif

namespace fs = std::filesystem;

static std::string envOrEmpty(const char* key){
    const char* v=std::getenv(key);
    return v?std::string(v):std::string();
}
static bool envTruthy(const char* key){
    auto v=envOrEmpty(key);
    for(auto& c:v) c=char(std::tolower((unsigned char)c));
    return v=="1"||v=="true"||v=="yes"||v=="on";
}
static std::string detectOsName(){
#if defined(__linux__)
    return "linux";
#elif defined(_WIN32)
    return "windows";
#elif defined(__APPLE__)
    return "macos";
#else
    return "unknown";
#endif
}
static std::string detectHostName(){
#if defined(_WIN32)
    auto v=envOrEmpty("COMPUTERNAME");
    return v.empty()?"keeply-host":v;
#else
    char buf[256]{};
    if(::gethostname(buf,sizeof(buf)-1)==0 && buf[0]) return std::string(buf);
    auto v=envOrEmpty("HOSTNAME");
    return v.empty()?"keeply-host":v;
#endif
}
static std::string argOrEmpty(int argc,char** argv,int i){
    return (i<argc && argv[i])?std::string(argv[i]):std::string();
}
static fs::path pickDataDir(){
    auto fromEnv=envOrEmpty("KEEPly_DATA_DIR");
    if(!fromEnv.empty()) return fs::path(fromEnv);
    return fs::current_path()/ "build";
}

int main(int argc,char** argv){
    try{
        keeply::WsClientConfig config;
        auto url=argOrEmpty(argc,argv,1);
        if(url.empty()) url=envOrEmpty("KEEPly_WS_URL");
        if(url.empty()) throw std::runtime_error("URL websocket vazia. Use argv[1] ou env KEEPly_WS_URL.");
        config.url=url;

        config.hostName=detectHostName();
        config.deviceName=config.hostName;
        auto dev=argOrEmpty(argc,argv,2);
        if(!dev.empty()) config.deviceName=dev;
        auto devEnv=envOrEmpty("KEEPly_DEVICE_NAME");
        if(!devEnv.empty()) config.deviceName=devEnv;

        config.osName=detectOsName();

        auto verbose=envTruthy("KEEPly_VERBOSE");
        auto dataDir=pickDataDir();
        std::error_code ec;
        fs::create_directories(dataDir,ec);
        if(ec) throw std::runtime_error("Falha ao criar dataDir: "+dataDir.string()+" | "+ec.message());

        auto api=std::make_shared<keeply::KeeplyApi>();
        api->setArchive((dataDir/"keeply_agent_ws.db").string());
        api->setRestoreRoot((dataDir/"restore_agent_ws").string());

        keeply::AgentIdentity identity=keeply::KeeplyAgentBootstrap::ensureRegistered(config);
        config.agentId=identity.deviceId;

        keeply::KeeplyAgentWsClient client(api,identity);
        client.connect(config);

        std::cout<<"Keeply agent websocket conectado em "<<config.url<<" com deviceId="<<config.agentId<<"\n";
        if(verbose){
            if(!identity.pairingCode.empty()) std::cout<<"Codigo de ativacao: "<<identity.pairingCode<<"\n";
            std::cout<<"Hostname: "<<config.hostName<<"\n";
            std::cout<<"Fingerprint SHA-256: "<<identity.fingerprintSha256<<"\n";
            std::cout<<"Cert PEM: "<<identity.certPemPath<<"\n";
            std::cout<<"Key PEM : "<<identity.keyPemPath<<"\n";
        }
        std::cout<<"Comandos remotos suportados: ping, state, snapshots, config.source:<path>, config.archive:<path>, config.restoreRoot:<path>, backup[:label], restore.file:snapshot|relPath|outRoot(opcional), restore.snapshot:snapshot|outRoot(opcional)\n";

        client.run();
        return 0;
    }catch(const std::exception& e){
        std::cerr<<"Falha no agente websocket: "<<e.what()<<"\n";
        return 1;
    }
}
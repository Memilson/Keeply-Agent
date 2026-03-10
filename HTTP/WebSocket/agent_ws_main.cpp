#include "ws.hpp"
#include "../Backup/cbt.hpp"
#include "../Backup/Linux/inotify_service.hpp"
#include <filesystem>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#if defined(__linux__) || defined(__APPLE__)
#include <csignal>
#include <sys/types.h>
#include <unistd.h>
#endif
#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
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
    return fs::path("/tmp/keeply");
}
static fs::path pickWatchRoot(){
    auto fromEnv=envOrEmpty("KEEPly_ROOT");
    if(!fromEnv.empty()) return fs::path(fromEnv);
    auto home=envOrEmpty("HOME");
    if(!home.empty()) return fs::path(home);
    std::error_code ec;
    auto cwd=fs::current_path(ec);
    return ec?fs::path("/tmp"):cwd;
}
static fs::path pickPidFilePath(const fs::path& dataDir){
    auto fromEnv=envOrEmpty("KEEPly_AGENT_PID_FILE");
    if(!fromEnv.empty()) return fs::path(fromEnv);
    return dataDir/"keeply_agent.pid";
}
static std::string findExecutableOnPath(const char* name){
    auto pathEnv=envOrEmpty("PATH");
    if(pathEnv.empty()) return {};
    std::size_t start=0;
    while(start<=pathEnv.size()){
        const std::size_t end=pathEnv.find(':',start);
        const std::string entry=pathEnv.substr(start,end==std::string::npos?std::string::npos:end-start);
        if(!entry.empty()){
            fs::path candidate=fs::path(entry)/name;
            std::error_code ec;
            const auto perms=fs::status(candidate,ec).permissions();
            if(!ec&&fs::exists(candidate,ec)&&!ec&&
               (perms&fs::perms::owner_exec)!=fs::perms::none){
                return candidate.string();
            }
        }
        if(end==std::string::npos) break;
        start=end+1;
    }
    return {};
}
#ifdef __linux__
static void daemonizeProcess(){
    pid_t pid=fork();
    if(pid<0) throw std::runtime_error("fork falhou.");
    if(pid>0) std::exit(0);

    if(setsid()<0) throw std::runtime_error("setsid falhou.");

    pid=fork();
    if(pid<0) throw std::runtime_error("fork 2 falhou.");
    if(pid>0) std::exit(0);

    umask(0);
    if(chdir("/")!=0) throw std::runtime_error("chdir falhou.");

    const int nullFd=open("/dev/null",O_RDWR);
    if(nullFd>=0){
        dup2(nullFd,STDIN_FILENO);
        dup2(nullFd,STDOUT_FILENO);
        dup2(nullFd,STDERR_FILENO);
        if(nullFd>STDERR_FILENO) close(nullFd);
    }
}
static void writePidFile(const fs::path& pidFile){
    std::error_code ec;
    fs::create_directories(pidFile.parent_path(),ec);
    if(ec) throw std::runtime_error("Falha criando diretorio do PID file: "+ec.message());
    std::ofstream out(pidFile,std::ios::trunc);
    if(!out) throw std::runtime_error("Falha criando PID file: "+pidFile.string());
    out<<getpid()<<"\n";
}
class OptionalTrayIndicator{
public:
    void start(){
        const std::string zenity=findExecutableOnPath("zenity");
        if(zenity.empty()) return;
        pid_=fork();
        if(pid_<0){
            pid_=-1;
            return;
        }
        if(pid_==0){
            execlp(zenity.c_str(),
                   zenity.c_str(),
                   "--notification",
                   "--text=Keeply Agent ativo",
                   "--window-icon=network-workgroup",
                   static_cast<char*>(nullptr));
            _exit(127);
        }
    }
    void stop() noexcept{
        if(pid_>0){
            kill(pid_,SIGTERM);
            int status=0;
            waitpid(pid_,&status,0);
            pid_=-1;
        }
    }
    ~OptionalTrayIndicator(){ stop(); }
private:
    pid_t pid_=-1;
};
#else
static void writePidFile(const fs::path&){
}
class OptionalTrayIndicator{
public:
    void start(){}
    void stop() noexcept{}
};
#endif
static void printCommands(){
    std::cout
        <<"Comandos remotos suportados:\n"
        <<"  - ping\n"
        <<"  - state\n"
        <<"  - snapshots\n"
        <<"  - scan.scope:<home|documents|desktop|downloads|pictures|music|videos>\n"
        <<"  - config.source:<path>\n"
        <<"  - config.archive:<path>\n"
        <<"  - config.restoreRoot:<path>\n"
        <<"  - backup[:label]\n"
        <<"  - restore.file:snapshot|relPath|outRoot(opcional)\n"
        <<"  - restore.snapshot:snapshot|outRoot(opcional)\n";
}
static void printStartupSummary(const keeply::WsClientConfig& config,
                                const keeply::AgentIdentity& identity,
                                const fs::path& dataDir,
                                const fs::path& watchRoot,
                                bool cbtEnabled,
                                bool trayEnabled,
                                bool foreground,
                                bool verbose){
    std::cout
        <<"============================================================\n"
        <<" Keeply Agent Online\n"
        <<"============================================================\n"
        <<"  WebSocket : "<<config.url<<"\n"
        <<"  Device ID : "<<config.agentId<<"\n"
        <<"  Hostname  : "<<config.hostName<<"\n"
        <<"  Device    : "<<config.deviceName<<"\n"
        <<"  Data Dir  : "<<dataDir<<"\n"
        <<"  Watch Root: "<<watchRoot<<"\n"
        <<"  Foreground: "<<(foreground?"yes":"no")<<"\n"
        <<"  CBT Local : "<<(cbtEnabled?"enabled":"disabled")<<"\n"
        <<"  Tray Icon : "<<(trayEnabled?"best-effort":"disabled")<<"\n";
    if(!identity.pairingCode.empty()) std::cout<<"  Pairing   : "<<identity.pairingCode<<"\n";
    if(verbose){
        std::cout
            <<"  OS        : "<<config.osName<<"\n"
            <<"  Fingerprint SHA-256 : "<<identity.fingerprintSha256<<"\n"
            <<"  Cert PEM  : "<<identity.certPemPath<<"\n"
            <<"  Key PEM   : "<<identity.keyPemPath<<"\n";
    }
    std::cout<<"------------------------------------------------------------\n";
    printCommands();
    std::cout<<"============================================================\n";
}

struct AgentRuntimeOptions{
    std::string url;
    std::string deviceName;
    fs::path watchRoot;
    bool foreground=false;
    bool enableLocalCbt=true;
    bool enableTray=true;
};

static AgentRuntimeOptions parseArgs(int argc,char** argv){
    AgentRuntimeOptions options;
    options.url=envOrEmpty("KEEPly_WS_URL");
    options.deviceName=envOrEmpty("KEEPly_DEVICE_NAME");
    options.watchRoot=pickWatchRoot();
    options.enableLocalCbt=!envTruthy("KEEPly_DISABLE_CBT");
    options.enableTray=!envTruthy("KEEPly_DISABLE_TRAY");

    int positionalIndex=0;
    for(int i=1;i<argc;++i){
        const std::string arg=argOrEmpty(argc,argv,i);
        if(arg=="--help"){
            std::cout
                <<"Uso: keeply_agent [--url <ws-url>] [--device <nome>] [--root <diretorio>] [--foreground] [--no-cbt] [--no-tray]\n"
                <<"Compatibilidade: keeply_agent <ws-url> [device]\n";
            std::exit(0);
        }
        if(arg=="--url"&&i+1<argc){ options.url=argOrEmpty(argc,argv,++i); continue; }
        if(arg=="--device"&&i+1<argc){ options.deviceName=argOrEmpty(argc,argv,++i); continue; }
        if(arg=="--root"&&i+1<argc){ options.watchRoot=fs::path(argOrEmpty(argc,argv,++i)); continue; }
        if(arg=="--foreground"){ options.foreground=true; continue; }
        if(arg=="--no-cbt"){ options.enableLocalCbt=false; continue; }
        if(arg=="--no-tray"){ options.enableTray=false; continue; }
        if(!arg.empty()&&arg[0]=='-') throw std::runtime_error("Argumento invalido: "+arg);
        if(positionalIndex==0) options.url=arg;
        else if(positionalIndex==1) options.deviceName=arg;
        else throw std::runtime_error("Argumentos posicionais demais.");
        ++positionalIndex;
    }
    if(options.url.empty()) throw std::runtime_error("URL websocket vazia. Use --url, argv[1] ou env KEEPly_WS_URL.");
    return options;
}

int main(int argc,char** argv){
    try{
#if defined(__linux__) || defined(__APPLE__)
        std::signal(SIGPIPE,SIG_IGN);
#endif
        const AgentRuntimeOptions options=parseArgs(argc,argv);
        keeply::WsClientConfig config;
        config.url=options.url;

        config.hostName=detectHostName();
        config.deviceName=config.hostName;
        if(!options.deviceName.empty()) config.deviceName=options.deviceName;

        config.osName=detectOsName();

        auto verbose=envTruthy("KEEPly_VERBOSE");
        auto dataDir=pickDataDir();
        auto watchRoot=options.watchRoot.empty()?pickWatchRoot():options.watchRoot;
        auto pidFile=pickPidFilePath(dataDir);
        config.identityDir=dataDir/"agent_identity";
        std::error_code ec;
        fs::create_directories(dataDir,ec);
        if(ec) throw std::runtime_error("Falha ao criar dataDir: "+dataDir.string()+" | "+ec.message());

#ifdef __linux__
        if(!options.foreground) daemonizeProcess();
#endif
        writePidFile(pidFile);

        auto api=std::make_shared<keeply::KeeplyApi>();
        api->setArchive((dataDir/"keeply.kipy").string());
        api->setRestoreRoot((dataDir/"restore_agent_ws").string());
        api->setSource(watchRoot.string());

        OptionalTrayIndicator tray;
        if(options.enableTray) tray.start();

        keeply::BackgroundCbtWatcher watcher;
        bool cbtStarted=false;
        if(options.enableLocalCbt){
            try{
                watcher.start(watchRoot);
                cbtStarted=watcher.running();
            }catch(const std::exception& ex){
                std::cerr<<"[keeply][cbt][warn] watcher local desativado: "<<ex.what()<<"\n";
            }
        }

        bool printedStartup=false;

        for(;;){
            try{
                keeply::AgentIdentity identity=keeply::KeeplyAgentBootstrap::ensureRegistered(config);
                config.agentId=identity.deviceId;
                if(options.foreground&&!printedStartup){
                    printStartupSummary(config,identity,dataDir,watchRoot,cbtStarted,options.enableTray,options.foreground,verbose);
                    printedStartup=true;
                }
                keeply::KeeplyAgentWsClient client(api,identity);
                client.connect(config);
                client.run();
                std::cerr<<"Conexao websocket encerrada. Tentando reconectar em 2s...\n";
            }catch(const std::exception& loopEx){
                std::cerr<<"Loop websocket falhou: "<<loopEx.what()<<"\n";
            }
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        return 0;
    }catch(const std::exception& e){
        std::cerr<<"Falha no agente Keeply: "<<e.what()<<"\n";
        return 1;
    }
}

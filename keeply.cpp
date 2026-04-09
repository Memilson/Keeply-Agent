#define KEEPLY_DAEMON_PROGRAM 1
#define main keeply_cbt_main
#include "Backup/Rastreamento/Linux/daemon.cpp"
#undef main
#undef KEEPLY_DAEMON_PROGRAM
#include "WebSocket/websocket_agente.hpp"
#include "Backup/Rastreamento/rastreamento_mudancas.hpp"
#include <atomic>
#include <algorithm>
#include <filesystem>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#ifdef __linux__
#include <arpa/inet.h>
#include <csignal>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>
#endif
#ifdef __linux__
#include <fcntl.h>
#endif
namespace fs = std::filesystem;
namespace eventos = keeply::rastreamento_eventos_base;
static std::string envOrEmptyAny(std::initializer_list<const char*> keys) {
    for (const char* key : keys) {
        auto value = eventos::envOrEmpty(key);
        if (!value.empty()) return value;}
    return {};}
static bool envTruthyAny(std::initializer_list<const char*> keys) {
    return keeply::parseTruthyValue(envOrEmptyAny(keys));}
static std::string detectOsName() {
#if defined(__linux__)
    return "linux";
#else
    return "unknown";
#endif
}
static std::string detectHostName() {
    char buf[256]{};
    if (::gethostname(buf, sizeof(buf) - 1) == 0 && buf[0]) return std::string(buf);
    auto v = eventos::envOrEmpty("HOSTNAME");
    return v.empty() ? "keeply-host" : v;}
static std::vector<std::string> detectLocalIps() {
    std::vector<std::string> out;
#ifdef __linux__
    ifaddrs* ifaddr = nullptr;
    if (getifaddrs(&ifaddr) != 0 || !ifaddr) return out;
    auto addUnique = [&](const std::string& ip) {
        if (ip.empty() || ip == "127.0.0.1" || ip == "::1") return;
        if (std::find(out.begin(), out.end(), ip) == out.end()) out.push_back(ip);
    };
    for (ifaddrs* current = ifaddr; current; current = current->ifa_next) {
        if (!current->ifa_addr || !current->ifa_name) continue;
        if ((current->ifa_flags & IFF_UP) == 0 || (current->ifa_flags & IFF_LOOPBACK) != 0) continue;
        char host[INET6_ADDRSTRLEN]{};
        if (current->ifa_addr->sa_family == AF_INET) {
            const auto* addr = reinterpret_cast<sockaddr_in*>(current->ifa_addr);
            if (inet_ntop(AF_INET, &addr->sin_addr, host, sizeof(host))) addUnique(host);
            continue;
        }
        if (current->ifa_addr->sa_family == AF_INET6) {
            const auto* addr = reinterpret_cast<sockaddr_in6*>(current->ifa_addr);
            if (IN6_IS_ADDR_LINKLOCAL(&addr->sin6_addr)) continue;
            if (inet_ntop(AF_INET6, &addr->sin6_addr, host, sizeof(host))) addUnique(host);
        }
    }
    freeifaddrs(ifaddr);
#endif
    return out;}
static std::string readCpuModel() {
#ifdef __linux__
    std::ifstream input("/proc/cpuinfo");
    std::string line;
    while (std::getline(input, line)) {
        const auto pos = line.find(':');
        if (pos == std::string::npos) continue;
        const std::string key = keeply::lowerAscii(keeply::trim(line.substr(0, pos)));
        if (key == "model name" || key == "hardware") {
            const std::string value = keeply::trim(line.substr(pos + 1));
            if (!value.empty()) return value;
        }
    }
#endif
    return {};}
static std::uint64_t readTotalMemoryBytes() {
#ifdef __linux__
    std::ifstream input("/proc/meminfo");
    std::string line;
    while (std::getline(input, line)) {
        if (line.rfind("MemTotal:", 0) != 0) continue;
        const std::string value = keeply::trim(line.substr(9));
        const std::size_t unitPos = value.find("kB");
        const std::string numeric = keeply::trim(unitPos == std::string::npos ? value : value.substr(0, unitPos));
        try { return static_cast<std::uint64_t>(std::stoull(numeric)) * 1024ull; } catch (...) { return 0; }
    }
#endif
    return 0;}
static std::string detectCpuArchitecture() {
#ifdef __linux__
    utsname info{};
    if (uname(&info) == 0 && info.machine[0]) return std::string(info.machine);
#endif
    return {};}
static std::string detectKernelVersion() {
#ifdef __linux__
    utsname info{};
    if (uname(&info) == 0 && info.release[0]) return std::string(info.release);
#endif
    return {};}
static std::string argOrEmpty(int argc, char** argv, int i) {
    return (i < argc && argv[i]) ? std::string(argv[i]) : std::string();}
static fs::path pickDataDir() {
    auto fromEnv = envOrEmptyAny({"KEEPLY_DATA_DIR", "KEEPly_DATA_DIR"});
    if (!fromEnv.empty()) return fs::path(fromEnv);
    return keeply::defaultKeeplyDataDir();}
static fs::path pickWatchRoot() {
    auto fromEnv = envOrEmptyAny({"KEEPLY_ROOT", "KEEPly_ROOT"});
    if (!fromEnv.empty()) return fs::path(fromEnv);
    return keeply::defaultSourceRootPath();}
static fs::path pickPidFilePath(const fs::path& dataDir) {
    auto fromEnv = envOrEmptyAny({"KEEPLY_AGENT_PID_FILE", "KEEPly_AGENT_PID_FILE"});
    if (!fromEnv.empty()) return fs::path(fromEnv);
    return dataDir / "keeply_agent.pid";}
static std::string findExecutableOnPath(const char* name) {
    auto pathEnv = eventos::envOrEmpty("PATH");
    if (pathEnv.empty()) return {};
    const char separator =
        ':';
    std::vector<std::string> candidates;
    candidates.emplace_back(name);
    std::size_t start = 0;
    while (start <= pathEnv.size()) {
        const std::size_t end = pathEnv.find(separator, start);
        const std::string entry = pathEnv.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (!entry.empty()) {
            for (const auto& candidateName : candidates) {
                fs::path candidate = fs::path(entry) / candidateName;
                std::error_code ec;
                if (!fs::exists(candidate, ec) || ec) continue;
                const auto perms = fs::status(candidate, ec).permissions();
                if (!ec && (perms & fs::perms::owner_exec) != fs::perms::none) return candidate.string();}}
        if (end == std::string::npos) break;
        start = end + 1;}
    return {};}
static void writePidFile(const fs::path& pidFile) {
    std::error_code ec;
    fs::create_directories(pidFile.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretorio do PID file: " + ec.message());
    std::ofstream out(pidFile, std::ios::trunc);
    if (!out) throw std::runtime_error("Falha criando PID file: " + pidFile.string());
    out << getpid() << "\n";}
class SingleInstanceGuard {
public:
    explicit SingleInstanceGuard(const fs::path& pidFile) : pidFile_(pidFile) {
        std::error_code ec;
        fs::create_directories(pidFile.parent_path(), ec);
        if (ec) throw std::runtime_error("Falha criando diretorio do PID file: " + ec.message());
        fd_ = ::open(pidFile_.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd_ < 0) throw std::runtime_error("Falha abrindo PID file para lock: " + pidFile_.string());
        if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
            const std::string reason = errno == EWOULDBLOCK
                ? "Ja existe outra instancia do Keeply Agent em execucao."
                : "Falha adquirindo lock do agente.";
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error(reason);}
        if (::ftruncate(fd_, 0) != 0) {
            const std::string message = "Falha limpando PID file para lock.";
            ::flock(fd_, LOCK_UN);
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error(message);}
        std::string pidText = std::to_string(getpid()) + "\n";
        if (::write(fd_, pidText.data(), pidText.size()) < 0) {
            const std::string message = "Falha escrevendo PID file com lock.";
            ::flock(fd_, LOCK_UN);
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error(message);}}
    ~SingleInstanceGuard() {
        std::error_code ec;
        fs::remove(pidFile_, ec);
        if (fd_ >= 0) {
            ::flock(fd_, LOCK_UN);
            ::close(fd_);
            fd_ = -1;}}
    SingleInstanceGuard(const SingleInstanceGuard&) = delete;
    SingleInstanceGuard& operator=(const SingleInstanceGuard&) = delete;
private:
    fs::path pidFile_;
    int fd_ = -1;
};
#ifdef __linux__
static fs::path currentExecutablePath() {
    std::vector<char> buffer(4096, '\0');
    for (;;) {
        const ssize_t n = ::readlink("/proc/self/exe", buffer.data(), buffer.size() - 1);
        if (n < 0) throw std::runtime_error("Falha resolvendo /proc/self/exe.");
        if (static_cast<std::size_t>(n) < buffer.size() - 1) {
            buffer[static_cast<std::size_t>(n)] = '\0';
            return fs::path(buffer.data());}
        buffer.resize(buffer.size() * 2, '\0');}}
static std::string readSmallTextFile(const fs::path& path) {
    std::ifstream in(path);
    if (!in) return {};
    std::ostringstream oss;
    oss << in.rdbuf();
    return keeply::trim(oss.str());}
static std::optional<pid_t> readDaemonPid() {
    const auto text = readSmallTextFile(keeply::defaultEventStorePidPath());
    if (text.empty()) return std::nullopt;
    try {
        const long long value = std::stoll(text);
        if (value <= 0) return std::nullopt;
        return static_cast<pid_t>(value);
    } catch (...) { return std::nullopt; }}
static bool isPidAlive(pid_t pid) {
    if (pid <= 0) return false;
    if (::kill(pid, 0) == 0) return true;
    return errno == EPERM;}
static std::string daemonRootValue() {
    return readSmallTextFile(keeply::defaultEventStoreRootPath());}
static std::string normalizeGenericPath(const fs::path& path) {
    return fs::absolute(path).lexically_normal().generic_string();}
static void removeStaleDaemonMetadata() {
    std::error_code ec;
    fs::remove(keeply::defaultEventStorePidPath(), ec);
    ec.clear();
    fs::remove(keeply::defaultEventStoreRootPath(), ec);}
static void stopDaemonProcess(pid_t pid) {
    if (pid <= 0) return;
    ::kill(pid, SIGTERM);
    for (int attempt = 0; attempt < 20; ++attempt) {
        if (!isPidAlive(pid)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));}
    ::kill(pid, SIGKILL);
    for (int attempt = 0; attempt < 20; ++attempt) {
        if (!isPidAlive(pid)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));}}
static std::string locateCbtDaemonExecutable() {
    const auto fromEnv = envOrEmptyAny({"KEEPLY_CBT_DAEMON", "KEEPly_CBT_DAEMON"});
    if (!fromEnv.empty()) return fromEnv;
    try {
        const fs::path currentExe = currentExecutablePath();
        const fs::path sibling = currentExe.parent_path() / "keeply_cbt_daemon";
        std::error_code ec;
        const auto perms = fs::status(sibling, ec).permissions();
        if (!ec && fs::exists(sibling, ec) && !ec && (perms & fs::perms::owner_exec) != fs::perms::none)
            return sibling.string();
    } catch (...) {}
    return findExecutableOnPath("keeply_cbt_daemon");}
static bool systemdAvailable() {
    std::error_code ec;
    return fs::exists("/run/systemd/system", ec) && !ec && !findExecutableOnPath("systemctl").empty();}
static int runDetachedCommand(const fs::path& executable, std::initializer_list<const char*> args) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        const int nullFd = open("/dev/null", O_RDWR);
        if (nullFd >= 0) {
            dup2(nullFd, STDOUT_FILENO);
            dup2(nullFd, STDERR_FILENO);
            if (nullFd > STDERR_FILENO) close(nullFd);}
        std::vector<char*> argv;
        argv.reserve(args.size() + 2);
        argv.push_back(const_cast<char*>(executable.c_str()));
        for (const char* arg : args) argv.push_back(const_cast<char*>(arg));
        argv.push_back(nullptr);
        execv(executable.c_str(), argv.data());
        _exit(127);}
    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno == EINTR) continue;
        return -1;}
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    return -1;}
static bool tryEnsureDaemonViaSystemd(const std::string& expectedRoot) {
    if (!systemdAvailable()) return false;
    const std::string systemctl = findExecutableOnPath("systemctl");
    if (systemctl.empty()) return false;
    if (runDetachedCommand(systemctl, {"start", "keeply-cbt-daemon.service"}) != 0) return false;
    for (int attempt = 0; attempt < 30; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const auto daemonPid = readDaemonPid();
        if (daemonPid && isPidAlive(*daemonPid) && daemonRootValue() == expectedRoot) return true;}
    return false;}
static bool ensureLinuxCbtDaemon(const fs::path& watchRoot) {
    const std::string expectedRoot = normalizeGenericPath(watchRoot);
    if (auto existingPid = readDaemonPid(); existingPid && isPidAlive(*existingPid)) {
        if (daemonRootValue() == expectedRoot) return true;
        if (!systemdAvailable()) {
            stopDaemonProcess(*existingPid);
            removeStaleDaemonMetadata();}
    } else {
        removeStaleDaemonMetadata();}
    if (tryEnsureDaemonViaSystemd(expectedRoot)) return true;
    const std::string daemonExe = locateCbtDaemonExecutable();
    if (daemonExe.empty()) throw std::runtime_error("Executavel keeply_cbt_daemon nao encontrado.");
    pid_t pid = fork();
    if (pid < 0) throw std::runtime_error("fork falhou ao iniciar daemon CBT.");
    if (pid == 0) {
        execl(daemonExe.c_str(), daemonExe.c_str(), "--root", expectedRoot.c_str(), static_cast<char*>(nullptr));
        _exit(127);}
    for (int attempt = 0; attempt < 30; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const auto daemonPid = readDaemonPid();
        if (daemonPid && isPidAlive(*daemonPid) && daemonRootValue() == expectedRoot) return true;
        int status = 0;
        const pid_t waitResult = waitpid(pid, &status, WNOHANG);
        if (waitResult == pid) break;}
    throw std::runtime_error("Daemon CBT nao confirmou inicializacao.");}
static void daemonizeProcess() {
    pid_t pid = fork();
    if (pid < 0) throw std::runtime_error("fork falhou.");
    if (pid > 0) std::exit(0);
    if (setsid() < 0) throw std::runtime_error("setsid falhou.");
    pid = fork();
    if (pid < 0) throw std::runtime_error("fork 2 falhou.");
    if (pid > 0) std::exit(0);
    umask(077);
    if (chdir("/") != 0) throw std::runtime_error("chdir falhou.");
    const int nullFd = open("/dev/null", O_RDWR);
    if (nullFd >= 0) {
        dup2(nullFd, STDIN_FILENO);
        dup2(nullFd, STDOUT_FILENO);
        dup2(nullFd, STDERR_FILENO);
        if (nullFd > STDERR_FILENO) close(nullFd);}}
class OptionalTrayIndicator {
public:
    void start() {
        const std::string zenity = findExecutableOnPath("zenity");
        if (zenity.empty()) return;
        pid_ = fork();
        if (pid_ < 0) { pid_ = -1; return; }
        if (pid_ == 0) {
            execlp(zenity.c_str(), zenity.c_str(), "--notification",
                   "--text=Keeply Agent ativo", "--window-icon=network-workgroup",
                   static_cast<char*>(nullptr));
            _exit(127);}}
    void stop() noexcept {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            int status = 0;
            waitpid(pid_, &status, 0);
            pid_ = -1;}}
    ~OptionalTrayIndicator() { stop(); }
private:
    pid_t pid_ = -1;
};
#else
class OptionalTrayIndicator {
public:
    void start() {}
    void stop() noexcept {}
};
#endif
class CbtEventLogger {
public:
    void start(const fs::path& watchRoot) {
        stop();
        root_ = fs::absolute(watchRoot).lexically_normal();
        stopRequested_.store(false);
        worker_ = std::thread([this]() {
            try {
                keeply::EventStore store(keeply::defaultEventStorePath());
                lastToken_ = store.latestToken(root_).value_or(0);
                while (!stopRequested_.load()) {
                    std::uint64_t newToken = lastToken_;
                    auto changes = store.loadChangesSince(root_, lastToken_, newToken);
                    if (!changes.empty()) {
                        for (const auto& change : changes) logChange_(change);
                        lastToken_ = newToken;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(750));
                }
            } catch (const std::exception& ex) {
                std::cerr << "[keeply][cbt][warn] logger de eventos indisponivel: " << ex.what() << "\n";
            }
        });
    }
    void stop() noexcept {
        stopRequested_.store(true);
        if (worker_.joinable()) worker_.join();
    }
    ~CbtEventLogger() { stop(); }
private:
    static const char* labelFor_(const keeply::ChangedFile& change) {
        if (change.isDeleted || change.eventType == "delete") return "removido";
        if (change.eventType == "modify") return "modificado";
        return "adicionado";
    }
    static void logChange_(const keeply::ChangedFile& change) {
        if (change.relPath.empty()) return;
        std::cout << "[keeply][cbt] " << labelFor_(change) << " | " << change.relPath << "\n";
        std::cout.flush();
    }
    fs::path root_;
    std::atomic<bool> stopRequested_{false};
    std::thread worker_;
    std::uint64_t lastToken_ = 0;
};
struct AgentRuntimeOptions {
    std::string url;
    std::string deviceName;
    fs::path watchRoot;
    bool foreground = false;
    bool enableTray = true;
    bool allowInsecureTls = false;
};
static AgentRuntimeOptions parseArgs(int argc, char** argv) {
    AgentRuntimeOptions options;
    static const std::string kDefaultWsUrl = "wss://backend.keeply.app.br/ws/agent";
    options.url        = envOrEmptyAny({"KEEPLY_WS_URL", "KEEPly_WS_URL"});
    options.deviceName = envOrEmptyAny({"KEEPLY_DEVICE_NAME", "KEEPly_DEVICE_NAME"});
    options.watchRoot  = pickWatchRoot();
    options.enableTray     = !envTruthyAny({"KEEPLY_DISABLE_TRAY", "KEEPly_DISABLE_TRAY"});
    options.allowInsecureTls = envTruthyAny({"KEEPLY_INSECURE_TLS", "KEEPly_INSECURE_TLS"});
    int positionalIndex = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argOrEmpty(argc, argv, i);
        if (arg == "--help") {
            std::cout
                << "Uso: keeply [--url <ws-url>] [--device <nome>] [--root <dir>]\n"
                << "            [--foreground] [--no-tray] [--insecure-tls]\n"
                << "     keeply cbt --root <dir> ...\n";
            std::exit(0);}
        if (arg == "--url"    && i + 1 < argc) { options.url        = argOrEmpty(argc, argv, ++i); continue; }
        if (arg == "--device" && i + 1 < argc) { options.deviceName = argOrEmpty(argc, argv, ++i); continue; }
        if (arg == "--root"   && i + 1 < argc) { options.watchRoot  = fs::path(argOrEmpty(argc, argv, ++i)); continue; }
        if (arg == "--foreground")   { options.foreground = true; continue; }
        if (arg == "--no-tray")      { options.enableTray = false; continue; }
        if (arg == "--insecure-tls") { options.allowInsecureTls = true; continue; }
        if (!arg.empty() && arg[0] == '-') throw std::runtime_error("Argumento invalido: " + arg);
        if (positionalIndex == 0) options.url = arg;
        else if (positionalIndex == 1) options.deviceName = arg;
        else throw std::runtime_error("Argumentos posicionais demais.");
        ++positionalIndex;}
    if (options.url.empty()) options.url = kDefaultWsUrl;
    return options;}
static void runAgentLoop(const AgentRuntimeOptions& options) {
    auto verbose   = envTruthyAny({"KEEPLY_VERBOSE", "KEEPly_VERBOSE"});
    auto dataDir   = pickDataDir();
    auto watchRoot = options.watchRoot.empty() ? pickWatchRoot() : options.watchRoot;
    auto pidFile   = pickPidFilePath(dataDir);
    keeply::WsClientConfig config;
    config.url              = options.url;
    config.allowInsecureTls = options.allowInsecureTls;
    config.hostName         = detectHostName();
    config.deviceName       = config.hostName;
    if (!options.deviceName.empty()) config.deviceName = options.deviceName;
    config.osName           = detectOsName();
    config.localIps         = detectLocalIps();
    config.cpuModel         = readCpuModel();
    config.cpuArchitecture  = detectCpuArchitecture();
    config.kernelVersion    = detectKernelVersion();
    config.totalMemoryBytes = readTotalMemoryBytes();
    config.cpuCores         = std::thread::hardware_concurrency();
    config.identityDir      = dataDir / "agent_identity";
    std::error_code ec;
    fs::create_directories(dataDir, ec);
    if (ec) throw std::runtime_error("Falha ao criar dataDir: " + dataDir.string() + " | " + ec.message());
    SingleInstanceGuard singleInstance(pidFile);
    auto api = std::make_shared<keeply::KeeplyApi>();
    api->setArchive(keeply::pathToUtf8(dataDir / "keeply.kipy"));
    api->setRestoreRoot(keeply::pathToUtf8(dataDir / "restore_agent_ws"));
    api->setSource(keeply::pathToUtf8(watchRoot));
    OptionalTrayIndicator tray;
    if (options.enableTray) tray.start();
    keeply::BackgroundCbtWatcher watcher;
    CbtEventLogger cbtLogger;
    bool cbtStarted = false;
#ifdef __linux__
    try {
        cbtStarted = ensureLinuxCbtDaemon(watchRoot);
    } catch (const std::exception& ex) {
        std::cerr << "[keeply][cbt][warn] daemon CBT indisponivel, usando watcher local: " << ex.what() << "\n";
        try { watcher.start(watchRoot); cbtStarted = watcher.running(); }
        catch (const std::exception& wx) { std::cerr << "[keeply][cbt][warn] watcher local desativado: " << wx.what() << "\n"; }}
#else
    try { watcher.start(watchRoot); cbtStarted = watcher.running(); }
    catch (const std::exception& ex) { std::cerr << "[keeply][cbt][warn] watcher local desativado: " << ex.what() << "\n"; }
#endif
    if (options.foreground && cbtStarted) cbtLogger.start(watchRoot);
    constexpr int kBackoffInitialMs = 1000;
    constexpr int kBackoffMaxMs     = 60000;
    constexpr double kBackoffJitterFactor = 0.25;
    int backoffMs = kBackoffInitialMs;
    bool printedStartup = false;
    for (;;) {
        try {
            keeply::AgentIdentity identity = keeply::KeeplyAgentBootstrap::ensureRegistered(config);
            config.agentId = identity.deviceId;
            if (options.foreground && !printedStartup) {
                std::cout
                    << "============================================================\n"
                    << " Keeply Agent Online\n"
                    << "============================================================\n"
                    << "  WebSocket : " << config.url        << "\n"
                    << "  Device ID : " << config.agentId    << "\n"
                    << "  Hostname  : " << config.hostName   << "\n"
                    << "  Device    : " << config.deviceName << "\n"
                    << "  Data Dir  : " << dataDir           << "\n"
                    << "  Watch Root: " << watchRoot         << "\n"
                    << "  CBT Local : " << (cbtStarted ? "enabled" : "disabled") << "\n"
                    << "  TLS Verify: " << (config.allowInsecureTls ? "disabled" : "enabled") << "\n";
                if (!identity.pairingCode.empty())
                    std::cout << "  Pairing   : " << identity.pairingCode << "\n";
                if (verbose) {
                    std::cout
                        << "  OS        : " << config.osName << "\n"
                        << "  Fingerprint SHA-256 : " << identity.fingerprintSha256 << "\n";}
                std::cout << "============================================================\n";
                printedStartup = true;}
            if (config.allowInsecureTls)
                std::cerr << "[keeply][tls][warn] verificacao TLS desabilitada por configuracao explicita.\n";
            keeply::KeeplyAgentWsClient client(api, identity);
            client.connect(config);
            backoffMs = kBackoffInitialMs;
            client.run();
            std::cerr << "Conexao websocket encerrada. Reconectando em " << backoffMs << "ms...\n";
        } catch (const std::exception& loopEx) {
            std::cerr << "Loop websocket falhou: " << loopEx.what() << "\n";
            std::cerr << "Reconectando em " << backoffMs << "ms...\n";}
        const int jitterRange = static_cast<int>(backoffMs * kBackoffJitterFactor);
        const int jitter = jitterRange > 0 ? (std::rand() % (jitterRange * 2 + 1)) - jitterRange : 0;
        const int sleepMs = std::max(100, backoffMs + jitter);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMs));
        backoffMs = std::min(backoffMs * 2, kBackoffMaxMs);}}
int main(int argc, char** argv) {
    try {
#ifdef __linux__
        std::signal(SIGPIPE, SIG_IGN);
#endif
        if (argc >= 2 && std::string(argv[1]) == "cbt") {
            argv[1] = argv[0];
            return keeply_cbt_main(argc - 1, argv + 1);}
        const AgentRuntimeOptions options = parseArgs(argc, argv);
#ifdef __linux__
        if (!options.foreground) daemonizeProcess();
#endif
        runAgentLoop(options);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Falha no agente Keeply: " << e.what() << "\n";
        return 1;}}

#define KEEPLY_DAEMON_PROGRAM 1
#define main keeply_cbt_main
#ifdef _WIN32
  #include "Backup/Rastreamento/Windows/daemon.cpp"
#else
  #include "Backup/Rastreamento/Linux/daemon.cpp"
#endif
#undef main
#undef KEEPLY_DAEMON_PROGRAM
#include "WebSocket/websocket_agente.hpp"
#include "Backup/Rastreamento/rastreamento_mudancas.hpp"
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
#ifdef _WIN32
#include <windows.h>
#include <tlhelp32.h>
#endif
#if defined(__linux__) || defined(__APPLE__)
#include <csignal>
#include <sys/file.h>
#include <sys/types.h>
#include <unistd.h>
#endif
#ifdef __linux__
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#endif
namespace fs = std::filesystem;
namespace eventos = keeply::rastreamento_eventos_base;
static std::string envOrEmptyAny(std::initializer_list<const char*> keys) {
    for (const char* key : keys) {
        auto value = eventos::envOrEmpty(key);
        if (!value.empty()) return value;}
    return {};}
static bool envTruthyAny(std::initializer_list<const char*> keys) {
    auto value = envOrEmptyAny(keys);
    for (auto& c : value) c = char(std::tolower((unsigned char)c));
    return value == "1" || value == "true" || value == "yes" || value == "on";}
static std::string detectOsName() {
#if defined(__linux__)
    return "linux";
#elif defined(_WIN32)
    return "windows";
#elif defined(__APPLE__)
    return "macos";
#else
    return "unknown";
#endif}
static std::string detectHostName() {
#if defined(_WIN32)
    auto v = eventos::envOrEmpty("COMPUTERNAME");
    return v.empty() ? "keeply-host" : v;
#else
    char buf[256]{};
    if (::gethostname(buf, sizeof(buf) - 1) == 0 && buf[0]) return std::string(buf);
    auto v = eventos::envOrEmpty("HOSTNAME");
    return v.empty() ? "keeply-host" : v;
#endif}
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
#ifdef _WIN32
        ';';
#else
        ':';
#endif
    std::vector<std::string> candidates;
    candidates.emplace_back(name);
#ifdef _WIN32
    const std::string baseName = name ? std::string(name) : std::string();
    if (baseName.find('.') == std::string::npos) {
        candidates.push_back(baseName + ".exe");
        candidates.push_back(baseName + ".cmd");
        candidates.push_back(baseName + ".bat");}
#endif
    std::size_t start = 0;
    while (start <= pathEnv.size()) {
        const std::size_t end = pathEnv.find(separator, start);
        const std::string entry = pathEnv.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (!entry.empty()) {
            for (const auto& candidateName : candidates) {
                fs::path candidate = fs::path(entry) / candidateName;
                std::error_code ec;
                if (!fs::exists(candidate, ec) || ec) continue;
#ifdef _WIN32
                if (fs::is_regular_file(candidate, ec) && !ec) return candidate.string();
#else
                const auto perms = fs::status(candidate, ec).permissions();
                if (!ec && (perms & fs::perms::owner_exec) != fs::perms::none) return candidate.string();
#endif}}
        if (end == std::string::npos) break;
        start = end + 1;}
    return {};}
static void writePidFile(const fs::path& pidFile) {
    std::error_code ec;
    fs::create_directories(pidFile.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretorio do PID file: " + ec.message());
    std::ofstream out(pidFile, std::ios::trunc);
    if (!out) throw std::runtime_error("Falha criando PID file: " + pidFile.string());
#ifdef _WIN32
    out << GetCurrentProcessId() << "\n";
#else
    out << getpid() << "\n";
#endif}
class SingleInstanceGuard {
public:
    explicit SingleInstanceGuard(const fs::path& pidFile) : pidFile_(pidFile) {
        std::error_code ec;
        fs::create_directories(pidFile.parent_path(), ec);
        if (ec) throw std::runtime_error("Falha criando diretorio do PID file: " + ec.message());
#ifdef _WIN32
        std::string mutexName = "keeply_agent_";
        const std::string rawPath = pidFile.string();
        mutexName.reserve(mutexName.size() + rawPath.size());
        for (unsigned char ch : rawPath) {
            mutexName.push_back(std::isalnum(ch) ? static_cast<char>(ch) : '_');}
        mutex_ = CreateMutexA(nullptr, FALSE, mutexName.c_str());
        if (!mutex_) throw std::runtime_error("Falha criando mutex de instancia unica do agente.");
        if (GetLastError() == ERROR_ALREADY_EXISTS) {
            CloseHandle(mutex_);
            mutex_ = nullptr;
            throw std::runtime_error("Ja existe outra instancia do Keeply Agent em execucao.");}
        writePidFile(pidFile_);
#else
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
            throw std::runtime_error(message);}
#endif}
    ~SingleInstanceGuard() {
#ifdef _WIN32
        std::error_code ec;
        fs::remove(pidFile_, ec);
        if (mutex_) {
            CloseHandle(mutex_);
            mutex_ = nullptr;}
#else
        std::error_code ec;
        fs::remove(pidFile_, ec);
        if (fd_ >= 0) {
            ::flock(fd_, LOCK_UN);
            ::close(fd_);
            fd_ = -1;}
#endif}
    SingleInstanceGuard(const SingleInstanceGuard&) = delete;
    SingleInstanceGuard& operator=(const SingleInstanceGuard&) = delete;
private:
    fs::path pidFile_;
#ifdef _WIN32
    HANDLE mutex_ = nullptr;
#else
    int fd_ = -1;
#endif
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
static inline std::string trimCopy(const std::string& value) { return keeply::trim(value); }
static std::string readSmallTextFile(const fs::path& path) {
    std::ifstream in(path);
    if (!in) return {};
    std::ostringstream oss;
    oss << in.rdbuf();
    return trimCopy(oss.str());}
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
struct AgentRuntimeOptions {
    std::string url;
    std::string deviceName;
    fs::path watchRoot;
    bool foreground = false;
    bool enableLocalCbt = true;
    bool enableTray = true;
#ifdef _WIN32
    bool allowInsecureTls = true;
    bool installService = false;
    bool uninstallService = false;
    bool background = false;
#else
    bool allowInsecureTls = false;
#endif
};
#ifdef _WIN32
static fs::path agentCurrentExePath() {
    std::wstring buf(MAX_PATH, L'\0');
    for (;;) {
        DWORD n = GetModuleFileNameW(nullptr, buf.data(), (DWORD)buf.size());
        if (n == 0) throw std::runtime_error("Falha resolvendo executavel.");
        if (n < buf.size() - 1) { buf.resize(n); return fs::path(buf); }
        buf.resize(buf.size() * 2, L'\0');}}
static constexpr const wchar_t* kRunKey   = L"SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Run";
static constexpr const wchar_t* kRunValue = L"KeeplyAgent";
static void agentLaunchBackground(const AgentRuntimeOptions& opts) {
    const fs::path exe = agentCurrentExePath();
    std::wstring cmd = L"\"" + exe.wstring() + L"\" --background";
    if (!opts.url.empty())        cmd += L" --url \"" + std::wstring(opts.url.begin(), opts.url.end()) + L"\"";
    if (!opts.deviceName.empty()) cmd += L" --device \"" + std::wstring(opts.deviceName.begin(), opts.deviceName.end()) + L"\"";
    if (!opts.watchRoot.empty())  cmd += L" --root \"" + opts.watchRoot.wstring() + L"\"";
    STARTUPINFOW si{}; si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};
    std::wstring cmdMut = cmd;
    if (CreateProcessW(nullptr, cmdMut.data(), nullptr, nullptr, FALSE,
                       DETACHED_PROCESS | CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi)) {
        CloseHandle(pi.hThread);
        CloseHandle(pi.hProcess);}}
static void agentRegisterAutorun(const AgentRuntimeOptions& opts) {
    const fs::path exe = agentCurrentExePath();
    std::wstring cmd = L"\"" + exe.wstring() + L"\" --background";
    if (!opts.url.empty())        cmd += L" --url \"" + std::wstring(opts.url.begin(), opts.url.end()) + L"\"";
    if (!opts.deviceName.empty()) cmd += L" --device \"" + std::wstring(opts.deviceName.begin(), opts.deviceName.end()) + L"\"";
    if (!opts.watchRoot.empty())  cmd += L" --root \"" + opts.watchRoot.wstring() + L"\"";
    HKEY hkey = nullptr;
    if (RegOpenKeyExW(HKEY_CURRENT_USER, kRunKey, 0, KEY_SET_VALUE, &hkey) == ERROR_SUCCESS) {
        RegSetValueExW(hkey, kRunValue, 0, REG_SZ,
            reinterpret_cast<const BYTE*>(cmd.c_str()),
            static_cast<DWORD>((cmd.size() + 1) * sizeof(wchar_t)));
        RegCloseKey(hkey);}}
static void agentInstallService(const AgentRuntimeOptions& opts) {
    agentRegisterAutorun(opts);
    agentLaunchBackground(opts);
    std::cout << "Keeply Agent instalado e iniciado em background.\n";}
static void agentUninstallService() {
    HKEY hkey = nullptr;
    LONG res = RegOpenKeyExW(HKEY_CURRENT_USER, kRunKey, 0, KEY_SET_VALUE, &hkey);
    if (res != ERROR_SUCCESS) throw std::runtime_error("Falha abrindo chave de registro. Erro=" + std::to_string(res));
    RegDeleteValueW(hkey, kRunValue);
    RegCloseKey(hkey);
    HANDLE snap = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
    if (snap != INVALID_HANDLE_VALUE) {
        PROCESSENTRY32W pe{}; pe.dwSize = sizeof(pe);
        if (Process32FirstW(snap, &pe)) {
            do {
                if (std::wstring(pe.szExeFile).find(L"keeply") != std::wstring::npos) {
                    HANDLE h = OpenProcess(PROCESS_TERMINATE, FALSE, pe.th32ProcessID);
                    if (h) { TerminateProcess(h, 0); CloseHandle(h); }}
            } while (Process32NextW(snap, &pe));}
        CloseHandle(snap);}
    std::cout << "Keeply Agent removido do inicio automatico.\n";}
#endif
static AgentRuntimeOptions parseArgs(int argc, char** argv) {
    AgentRuntimeOptions options;
    static const std::string kDefaultWsUrl = "wss://backend.keeply.app.br/ws/agent";
    options.url        = envOrEmptyAny({"KEEPLY_WS_URL", "KEEPly_WS_URL"});
    options.deviceName = envOrEmptyAny({"KEEPLY_DEVICE_NAME", "KEEPly_DEVICE_NAME"});
    options.watchRoot  = pickWatchRoot();
    options.enableLocalCbt = !envTruthyAny({"KEEPLY_DISABLE_CBT", "KEEPly_DISABLE_CBT"});
    options.enableTray     = !envTruthyAny({"KEEPLY_DISABLE_TRAY", "KEEPly_DISABLE_TRAY"});
#ifdef _WIN32
    options.allowInsecureTls = true;
#else
    options.allowInsecureTls = envTruthyAny({"KEEPLY_INSECURE_TLS", "KEEPly_INSECURE_TLS"});
#endif
    int positionalIndex = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argOrEmpty(argc, argv, i);
        if (arg == "--help") {
            std::cout
                << "Uso: keeply [--url <ws-url>] [--device <nome>] [--root <dir>]\n"
                << "            [--foreground] [--no-cbt] [--no-tray] [--insecure-tls]\n"
                << "     keeply cbt --root <dir> ...\n";
            std::exit(0);}
        if (arg == "--url"    && i + 1 < argc) { options.url        = argOrEmpty(argc, argv, ++i); continue; }
        if (arg == "--device" && i + 1 < argc) { options.deviceName = argOrEmpty(argc, argv, ++i); continue; }
        if (arg == "--root"   && i + 1 < argc) { options.watchRoot  = fs::path(argOrEmpty(argc, argv, ++i)); continue; }
        if (arg == "--foreground")   { options.foreground     = true; continue; }
        if (arg == "--no-cbt")       { options.enableLocalCbt = false; continue; }
        if (arg == "--no-tray")      { options.enableTray     = false; continue; }
        if (arg == "--insecure-tls") { options.allowInsecureTls = true; continue; }
#ifdef _WIN32
        if (arg == "--install-service")   { options.installService   = true; continue; }
        if (arg == "--uninstall-service") { options.uninstallService = true; continue; }
        if (arg == "--background")        { options.background       = true; continue; }
#endif
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
    bool cbtStarted = false;
#ifdef __linux__
    if (options.enableLocalCbt) {
        try {
            cbtStarted = ensureLinuxCbtDaemon(watchRoot);
        } catch (const std::exception& ex) {
            std::cerr << "[keeply][cbt][warn] daemon CBT indisponivel, usando watcher local: " << ex.what() << "\n";
            try { watcher.start(watchRoot); cbtStarted = watcher.running(); }
            catch (const std::exception& wx) { std::cerr << "[keeply][cbt][warn] watcher local desativado: " << wx.what() << "\n"; }}}
#else
    if (options.enableLocalCbt) {
        try { watcher.start(watchRoot); cbtStarted = watcher.running(); }
        catch (const std::exception& ex) { std::cerr << "[keeply][cbt][warn] watcher local desativado: " << ex.what() << "\n"; }}
#endif
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
#if defined(__linux__) || defined(__APPLE__)
        std::signal(SIGPIPE, SIG_IGN);
#endif
        if (argc >= 2 && std::string(argv[1]) == "cbt") {
            argv[1] = argv[0];
            return keeply_cbt_main(argc - 1, argv + 1);}
        const AgentRuntimeOptions options = parseArgs(argc, argv);
#ifdef _WIN32
        if (options.installService)   { agentInstallService(options); return 0; }
        if (options.uninstallService) { agentUninstallService(); return 0; }
        if (options.background) {
            runAgentLoop(options);
            return 0;}
        agentRegisterAutorun(options);{
            auto dataDir = pickDataDir();
            std::error_code ec;
            fs::create_directories(dataDir, ec);
            keeply::WsClientConfig config;
            config.url              = options.url;
            config.allowInsecureTls = options.allowInsecureTls;
            config.hostName         = detectHostName();
            config.deviceName       = config.hostName;
            if (!options.deviceName.empty()) config.deviceName = options.deviceName;
            config.osName           = detectOsName();
            config.identityDir      = dataDir / "agent_identity";
            auto existing = keeply::KeeplyAgentBootstrap::loadPersistedIdentity(config);
            if (existing.deviceId.empty()) {
                try {
                    keeply::KeeplyAgentBootstrap::ensureRegistered(config);
                } catch (const std::exception& ex) {
                    std::wstring msg = L"Falha ao registrar dispositivo:\n";
                    msg += std::wstring(ex.what(), ex.what() + ::strlen(ex.what()));
                    MessageBoxW(nullptr, msg.c_str(), L"Keeply - Erro", MB_OK | MB_ICONERROR);
                    return 1;}}}
        agentLaunchBackground(options);
        return 0;
#endif
#ifdef __linux__
        if (!options.foreground) daemonizeProcess();
#endif
        runAgentLoop(options);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Falha no agente Keeply: " << e.what() << "\n";
        return 1;}}

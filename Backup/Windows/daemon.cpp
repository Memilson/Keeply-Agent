#ifdef _WIN32
#include <windows.h>
#include <winsvc.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include "../cbt.hpp"

namespace fs = std::filesystem;

namespace {
std::atomic_bool gStopRequested{false};
SERVICE_STATUS_HANDLE gServiceStatusHandle = nullptr;
SERVICE_STATUS gServiceStatus{};
std::string gServiceRootUtf8;
constexpr const char* kServiceName = "KeeplyCbtDaemon";

BOOL WINAPI consoleHandler(DWORD ctrlType) {
    switch (ctrlType) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
        case CTRL_CLOSE_EVENT:
        case CTRL_LOGOFF_EVENT:
        case CTRL_SHUTDOWN_EVENT:
            gStopRequested = true;
            return TRUE;
        default:
            return FALSE;
    }
}

std::string normalizePath(const fs::path& p) {
    return p.lexically_normal().generic_string();
}

fs::path getDbPath() {
    return keeply::defaultEventStorePath();
}

fs::path getPidPath() {
    return keeply::defaultEventStorePidPath();
}

fs::path getRootPath() {
    return keeply::defaultEventStoreRootPath();
}

void writePidFile() {
    fs::create_directories(getPidPath().parent_path());
    std::ofstream pid(getPidPath(), std::ios::trunc);
    if (!pid) throw std::runtime_error("Falha criando PID file do daemon.");
    pid << GetCurrentProcessId() << "\n";
}

void writeRootFile(const fs::path& root) {
    fs::create_directories(getRootPath().parent_path());
    std::ofstream rootFile(getRootPath(), std::ios::trunc);
    if (!rootFile) throw std::runtime_error("Falha criando arquivo de root do daemon.");
    rootFile << fs::absolute(root).lexically_normal().generic_string() << "\n";
}

std::string envOrEmpty(const char* key) {
    const char* value = std::getenv(key);
    return value ? std::string(value) : std::string();
}

std::string wideToUtf8(const std::wstring& ws) {
    if (ws.empty()) return {};
    const int size = WideCharToMultiByte(CP_UTF8, 0, ws.data(), static_cast<int>(ws.size()), nullptr, 0, nullptr, nullptr);
    if (size <= 0) throw std::runtime_error("Falha convertendo UTF-16 para UTF-8.");
    std::string out(size, '\0');
    const int written = WideCharToMultiByte(CP_UTF8, 0, ws.data(), static_cast<int>(ws.size()), out.data(), size, nullptr, nullptr);
    if (written != size) throw std::runtime_error("Falha convertendo UTF-16 para UTF-8.");
    return out;
}

std::wstring utf8ToWide(const std::string& s) {
    if (s.empty()) return {};
    const int size = MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), nullptr, 0);
    if (size <= 0) throw std::runtime_error("Falha convertendo UTF-8 para UTF-16.");
    std::wstring out(static_cast<std::size_t>(size), L'\0');
    const int written = MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), out.data(), size);
    if (written != size) throw std::runtime_error("Falha convertendo UTF-8 para UTF-16.");
    return out;
}

fs::path currentExecutablePath() {
    std::wstring buffer(MAX_PATH, L'\0');
    for (;;) {
        const DWORD size = GetModuleFileNameW(nullptr, buffer.data(), static_cast<DWORD>(buffer.size()));
        if (size == 0) throw std::runtime_error("Falha resolvendo executavel atual.");
        if (size < buffer.size() - 1) {
            buffer.resize(size);
            return fs::path(buffer);
        }
        buffer.resize(buffer.size() * 2, L'\0');
    }
}

void setServiceState(DWORD state, DWORD win32ExitCode = NO_ERROR, DWORD waitHint = 0) {
    if (!gServiceStatusHandle) return;
    gServiceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    gServiceStatus.dwCurrentState = state;
    gServiceStatus.dwWin32ExitCode = win32ExitCode;
    gServiceStatus.dwWaitHint = waitHint;
    gServiceStatus.dwControlsAccepted = (state == SERVICE_START_PENDING) ? 0 : SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
    gServiceStatus.dwCheckPoint = (state == SERVICE_RUNNING || state == SERVICE_STOPPED) ? 0 : 1;
    SetServiceStatus(gServiceStatusHandle, &gServiceStatus);
}

class Handle {
    HANDLE h_ = INVALID_HANDLE_VALUE;
public:
    Handle() = default;
    explicit Handle(HANDLE h) : h_(h) {}
    ~Handle() {
        if (h_ != INVALID_HANDLE_VALUE) CloseHandle(h_);
    }
    Handle(const Handle&) = delete;
    Handle& operator=(const Handle&) = delete;
    Handle(Handle&& other) noexcept : h_(other.h_) {
        other.h_ = INVALID_HANDLE_VALUE;
    }
    Handle& operator=(Handle&& other) noexcept {
        if (this != &other) {
            if (h_ != INVALID_HANDLE_VALUE) CloseHandle(h_);
            h_ = other.h_;
            other.h_ = INVALID_HANDLE_VALUE;
        }
        return *this;
    }
    HANDLE get() const { return h_; }
    bool valid() const { return h_ != INVALID_HANDLE_VALUE; }
};

class WindowsDaemon {
    fs::path root_;
    keeply::EventStore store_;
    Handle dir_;

public:
    explicit WindowsDaemon(const fs::path& root)
        : root_(fs::absolute(root).lexically_normal()), store_(getDbPath()) {
        dir_ = Handle(CreateFileW(
            root_.wstring().c_str(),
            FILE_LIST_DIRECTORY,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            nullptr,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            nullptr
        ));
        if (!dir_.valid()) throw std::runtime_error("Falha abrindo diretorio para monitoramento.");
        store_.ensureRoot(root_);
    }

    void run() {
        std::vector<unsigned char> buffer(64 * 1024);
        while (!gStopRequested) {
            DWORD bytesReturned = 0;
            const BOOL ok = ReadDirectoryChangesW(
                dir_.get(),
                buffer.data(),
                static_cast<DWORD>(buffer.size()),
                TRUE,
                FILE_NOTIFY_CHANGE_FILE_NAME |
                FILE_NOTIFY_CHANGE_DIR_NAME |
                FILE_NOTIFY_CHANGE_ATTRIBUTES |
                FILE_NOTIFY_CHANGE_SIZE |
                FILE_NOTIFY_CHANGE_LAST_WRITE |
                FILE_NOTIFY_CHANGE_CREATION,
                &bytesReturned,
                nullptr,
                nullptr
            );
            if (!ok) {
                const DWORD err = GetLastError();
                if (gStopRequested) break;
                throw std::runtime_error("Falha em ReadDirectoryChangesW. Codigo=" + std::to_string(err));
            }
            if (bytesReturned == 0) continue;
            parseBuffer(buffer.data(), bytesReturned);
        }
    }

private:
    void parseBuffer(void* data, DWORD bytes) {
        unsigned char* ptr = static_cast<unsigned char*>(data);
        unsigned char* end = ptr + bytes;
        while (ptr < end) {
            auto* info = reinterpret_cast<FILE_NOTIFY_INFORMATION*>(ptr);
            handleNotification(*info);
            if (info->NextEntryOffset == 0) break;
            ptr += info->NextEntryOffset;
        }
    }

    void handleNotification(const FILE_NOTIFY_INFORMATION& info) {
        const std::wstring ws(info.FileName, info.FileNameLength / sizeof(WCHAR));
        if (ws.empty()) return;
        const fs::path fullPath = root_ / fs::path(ws);
        std::error_code relEc;
        fs::path relPath = fs::relative(fullPath, root_, relEc);
        if (relEc || relPath.empty()) return;
        const std::string rel = normalizePath(relPath);
        if (rel.rfind("..", 0) == 0) return;

        std::string type;
        switch (info.Action) {
            case FILE_ACTION_ADDED:
            case FILE_ACTION_RENAMED_NEW_NAME:
                type = "upsert";
                break;
            case FILE_ACTION_MODIFIED:
                type = "modify";
                break;
            case FILE_ACTION_REMOVED:
            case FILE_ACTION_RENAMED_OLD_NAME:
                type = "delete";
                break;
            default:
                return;
        }

        std::error_code typeEc;
        bool isDir = false;
        if (type != "delete") isDir = fs::is_directory(fullPath, typeEc) && !typeEc;

        store_.appendEvent(type, rel, isDir);
    }
};

DWORD WINAPI serviceControlHandler(DWORD ctrlCode, DWORD, LPVOID, LPVOID) {
    switch (ctrlCode) {
        case SERVICE_CONTROL_STOP:
        case SERVICE_CONTROL_SHUTDOWN:
            gStopRequested = true;
            setServiceState(SERVICE_STOP_PENDING, NO_ERROR, 2000);
            return NO_ERROR;
        default:
            return NO_ERROR;
    }
}

void runDaemonOrThrow(const fs::path& root) {
    if (root.empty()) throw std::runtime_error("Informe --root <diretorio>.");
    if (!fs::exists(root) || !fs::is_directory(root)) throw std::runtime_error("Root invalido para o daemon.");
    fs::create_directories(getPidPath().parent_path());
    writePidFile();
    writeRootFile(root);
    WindowsDaemon daemon(root);
    daemon.run();
}

void WINAPI serviceMain(DWORD, LPWSTR*) {
    gServiceStatusHandle = RegisterServiceCtrlHandlerExW(L"KeeplyCbtDaemon", serviceControlHandler, nullptr);
    if (!gServiceStatusHandle) return;
    setServiceState(SERVICE_START_PENDING, NO_ERROR, 3000);
    try {
        const fs::path root = gServiceRootUtf8.empty() ? fs::path(envOrEmpty("KEEPLY_ROOT")) : fs::path(gServiceRootUtf8);
        setServiceState(SERVICE_RUNNING);
        runDaemonOrThrow(root);
        setServiceState(SERVICE_STOPPED);
    } catch (...) {
        setServiceState(SERVICE_STOPPED, ERROR_SERVICE_SPECIFIC_ERROR);
    }
}

void installService(const fs::path& root) {
    if (root.empty()) throw std::runtime_error("Informe --root <diretorio> para instalar o servico.");
    const fs::path normalizedRoot = fs::absolute(root).lexically_normal();
    const fs::path exePath = currentExecutablePath();
    const std::wstring commandLine = L"\"" + exePath.wstring() + L"\" --service --root \"" + normalizedRoot.wstring() + L"\"";

    SC_HANDLE scm = OpenSCManagerW(nullptr, nullptr, SC_MANAGER_CREATE_SERVICE);
    if (!scm) throw std::runtime_error("Falha abrindo SCM.");
    SC_HANDLE service = CreateServiceW(
        scm,
        L"KeeplyCbtDaemon",
        L"Keeply CBT Daemon",
        SERVICE_ALL_ACCESS,
        SERVICE_WIN32_OWN_PROCESS,
        SERVICE_AUTO_START,
        SERVICE_ERROR_NORMAL,
        commandLine.c_str(),
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr
    );
    if (!service) {
        const DWORD err = GetLastError();
        CloseServiceHandle(scm);
        throw std::runtime_error("Falha criando servico Windows. Codigo=" + std::to_string(err));
    }
    CloseServiceHandle(service);
    CloseServiceHandle(scm);
}

void uninstallService() {
    SC_HANDLE scm = OpenSCManagerW(nullptr, nullptr, SC_MANAGER_CONNECT);
    if (!scm) throw std::runtime_error("Falha abrindo SCM.");
    SC_HANDLE service = OpenServiceW(scm, L"KeeplyCbtDaemon", DELETE | SERVICE_STOP | SERVICE_QUERY_STATUS);
    if (!service) {
        const DWORD err = GetLastError();
        CloseServiceHandle(scm);
        throw std::runtime_error("Falha abrindo servico Windows. Codigo=" + std::to_string(err));
    }
    SERVICE_STATUS status{};
    ControlService(service, SERVICE_CONTROL_STOP, &status);
    if (!DeleteService(service)) {
        const DWORD err = GetLastError();
        CloseServiceHandle(service);
        CloseServiceHandle(scm);
        throw std::runtime_error("Falha removendo servico Windows. Codigo=" + std::to_string(err));
    }
    CloseServiceHandle(service);
    CloseServiceHandle(scm);
}

} 

int main(int argc, char** argv) {
    try {
        fs::path root;
        bool install = false;
        bool uninstall = false;
        bool serviceMode = false;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--root" && i + 1 < argc) {
                root = argv[++i];
            } else if (arg == "--foreground") {
            } else if (arg == "--install-service") {
                install = true;
            } else if (arg == "--uninstall-service") {
                uninstall = true;
            } else if (arg == "--service") {
                serviceMode = true;
            } else if (arg == "--help") {
                std::cout << "Uso: keeply_cbt_daemon --root <diretorio> [--foreground] [--install-service|--uninstall-service|--service]\n";
                return 0;
            } else {
                throw std::runtime_error("Argumento invalido: " + arg);
            }
        }

        if (root.empty()) {
            const auto rootFromEnv = envOrEmpty("KEEPLY_ROOT");
            if (!rootFromEnv.empty()) root = fs::path(rootFromEnv);
        }

        if (install) {
            installService(root);
            std::cout << "Servico Windows instalado.\n";
            return 0;
        }
        if (uninstall) {
            uninstallService();
            std::cout << "Servico Windows removido.\n";
            return 0;
        }
        if (serviceMode) {
            gServiceRootUtf8 = root.string();
            SERVICE_TABLE_ENTRYW serviceTable[] = {
                { const_cast<LPWSTR>(L"KeeplyCbtDaemon"), serviceMain },
                { nullptr, nullptr }
            };
            if (!StartServiceCtrlDispatcherW(serviceTable)) {
                throw std::runtime_error("Falha iniciando dispatcher do servico Windows.");
            }
            return 0;
        }

        if (root.empty()) throw std::runtime_error("Informe --root <diretorio>.");
        if (!fs::exists(root) || !fs::is_directory(root)) throw std::runtime_error("Root invalido para o daemon.");

        if (!SetConsoleCtrlHandler(consoleHandler, TRUE)) {
            throw std::runtime_error("Falha registrando handler do console.");
        }

        runDaemonOrThrow(root);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "keeply_cbt_daemon: " << ex.what() << "\n";
        return 1;
    }
}
#else
int main() {
    return 1;
}
#endif

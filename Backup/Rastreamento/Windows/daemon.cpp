#include "../../../keeply.hpp"
#include "../rastreamento_mudancas.hpp"

#ifdef _WIN32
#include <windows.h>
#include <winsvc.h>
#include <atomic>
#include <memory>
#include <vector>
#endif

namespace fs = std::filesystem;

#ifdef _WIN32

namespace keeply::rastreamento_eventos_base {

class MotorMonitorWindows final : public MotorMonitorBase {
public:
    MotorMonitorWindows(const fs::path& rootPath, bool respectSystemExclusionPolicy)
        : MotorMonitorBase(rootPath, respectSystemExclusionPolicy),
          dir_(std::make_unique<Handle>()) {
        dir_->reset(CreateFileW(
            this->rootPath().wstring().c_str(),
            FILE_LIST_DIRECTORY,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            nullptr,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            nullptr
        ));
        if (!dir_->valid()) throw std::runtime_error("Falha abrindo diretorio para monitoramento.");
    }

    ~MotorMonitorWindows() override {
        requestStop();
    }

    void requestStop() noexcept override {
        if (dir_) dir_->reset();
    }

    void run(const std::function<bool()>& shouldStop) override {
        std::vector<unsigned char> buffer(64 * 1024);
        while (!shouldStop()) {
            if (!dir_ || !dir_->valid()) return;
            DWORD bytesReturned = 0;
            const BOOL ok = ReadDirectoryChangesW(
                dir_->get(),
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
                if (shouldStop()) return;
                const DWORD err = GetLastError();
                throw std::runtime_error("Falha em ReadDirectoryChangesW. Codigo=" + std::to_string(err));
            }
            if (bytesReturned == 0) continue;
            parseBuffer(buffer.data(), bytesReturned);
        }
    }

private:
    class Handle {
        HANDLE handle_ = INVALID_HANDLE_VALUE;

    public:
        Handle() = default;
        explicit Handle(HANDLE handle) : handle_(handle) {}

        ~Handle() {
            if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
        }

        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;

        HANDLE get() const { return handle_; }
        bool valid() const { return handle_ != INVALID_HANDLE_VALUE; }
        void reset(HANDLE next = INVALID_HANDLE_VALUE) {
            if (handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
            handle_ = next;
        }
    };

    static std::string normalizePath(const fs::path& path) {
        return path.lexically_normal().generic_string();
    }

    void parseBuffer(void* data, unsigned long bytes) {
        unsigned char* ptr = static_cast<unsigned char*>(data);
        unsigned char* end = ptr + bytes;
        while (ptr < end) {
            auto* info = reinterpret_cast<FILE_NOTIFY_INFORMATION*>(ptr);
            handleNotification(std::wstring(info->FileName, info->FileNameLength / sizeof(WCHAR)), info->Action);
            if (info->NextEntryOffset == 0) break;
            ptr += info->NextEntryOffset;
        }
    }

    void handleNotification(const std::wstring& relativePathUtf16, unsigned long action) {
        if (relativePathUtf16.empty()) return;
        const fs::path fullPath = rootPath() / fs::path(relativePathUtf16);
        if (shouldIgnorePath(fullPath)) return;

        const auto rel = buildRelativePath(fullPath);
        if (!rel) return;

        TipoEventoMonitorado eventType;
        switch (action) {
            case FILE_ACTION_ADDED:
            case FILE_ACTION_RENAMED_NEW_NAME:
                eventType = TipoEventoMonitorado::Upsert;
                break;
            case FILE_ACTION_MODIFIED:
                eventType = TipoEventoMonitorado::Modify;
                break;
            case FILE_ACTION_REMOVED:
            case FILE_ACTION_RENAMED_OLD_NAME:
                eventType = TipoEventoMonitorado::Delete;
                break;
            default:
                return;
        }

        std::error_code typeEc;
        bool isDir = false;
        if (eventType != TipoEventoMonitorado::Delete) {
            isDir = fs::is_directory(fullPath, typeEc) && !typeEc;
        }
        appendEvent(eventType, normalizePath(fs::path(*rel)), isDir);
    }

    std::unique_ptr<Handle> dir_;
};

std::unique_ptr<MotorMonitor> createWindowsMotorMonitor(const fs::path& rootPath,
                                                        bool respectSystemExclusionPolicy) {
    return std::make_unique<MotorMonitorWindows>(rootPath, respectSystemExclusionPolicy);
}

} // namespace keeply::rastreamento_eventos_base

namespace {

std::atomic_bool gStopRequested{false};
SERVICE_STATUS_HANDLE gServiceStatusHandle = nullptr;
SERVICE_STATUS gServiceStatus{};
std::string gServiceRootUtf8;
keeply::rastreamento_eventos_base::MonitorRunner* gActiveRunner = nullptr;

BOOL WINAPI consoleHandler(DWORD ctrlType) {
    switch (ctrlType) {
        case CTRL_C_EVENT:
        case CTRL_BREAK_EVENT:
        case CTRL_CLOSE_EVENT:
        case CTRL_LOGOFF_EVENT:
        case CTRL_SHUTDOWN_EVENT:
            gStopRequested = true;
            if (gActiveRunner) gActiveRunner->requestStop();
            return TRUE;
        default:
            return FALSE;
    }
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

DWORD WINAPI serviceControlHandler(DWORD ctrlCode, DWORD, LPVOID, LPVOID) {
    switch (ctrlCode) {
        case SERVICE_CONTROL_STOP:
        case SERVICE_CONTROL_SHUTDOWN:
            gStopRequested = true;
            if (gActiveRunner) gActiveRunner->requestStop();
            setServiceState(SERVICE_STOP_PENDING, NO_ERROR, 2000);
            return NO_ERROR;
        default:
            return NO_ERROR;
    }
}

void runDaemonOrThrow(const fs::path& root) {
    const fs::path normalizedRoot = keeply::rastreamento_eventos_base::resolveAndPrepareDaemonMonitorRootOrThrow(
        root,
        std::to_string(GetCurrentProcessId())
    );
    keeply::rastreamento_eventos_base::MonitorRunner daemon(normalizedRoot, false);
    gActiveRunner = &daemon;
    daemon.run([]() { return gStopRequested.load(); });
    gActiveRunner = nullptr;
}

void WINAPI serviceMain(DWORD, LPWSTR*) {
    gServiceStatusHandle = RegisterServiceCtrlHandlerExW(L"KeeplyCbtDaemon", serviceControlHandler, nullptr);
    if (!gServiceStatusHandle) return;
    setServiceState(SERVICE_START_PENDING, NO_ERROR, 3000);
    try {
        const fs::path root = gServiceRootUtf8.empty() ? fs::path() : fs::path(gServiceRootUtf8);
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

} // namespace

#ifdef KEEPLY_DAEMON_PROGRAM
int main(int argc, char** argv) {
    try {
        keeply::rastreamento_eventos_base::OpcoesDaemonMonitor options;
        try {
            options = keeply::rastreamento_eventos_base::parseDaemonMonitorArgs(argc, argv, true);
        } catch (const std::runtime_error& ex) {
            if (std::string(ex.what()) == "__KEEPLY_DAEMON_HELP__") {
                std::cout << "Uso: keeply_cbt_daemon --root <diretorio> [--foreground] [--install-service|--uninstall-service|--service]\n";
                return 0;
            }
            throw;
        }

        if (options.installService) {
            installService(keeply::rastreamento_eventos_base::resolveDaemonMonitorRootOrThrow(options.rootPath));
            std::cout << "Servico Windows instalado.\n";
            return 0;
        }
        if (options.uninstallService) {
            uninstallService();
            std::cout << "Servico Windows removido.\n";
            return 0;
        }
        if (options.serviceMode) {
            gServiceRootUtf8 = options.rootPath.string();
            SERVICE_TABLE_ENTRYW serviceTable[] = {
                { const_cast<LPWSTR>(L"KeeplyCbtDaemon"), serviceMain },
                { nullptr, nullptr }
            };
            if (!StartServiceCtrlDispatcherW(serviceTable)) {
                throw std::runtime_error("Falha iniciando dispatcher do servico Windows.");
            }
            return 0;
        }

        if (!SetConsoleCtrlHandler(consoleHandler, TRUE)) {
            throw std::runtime_error("Falha registrando handler do console.");
        }

        runDaemonOrThrow(options.rootPath);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "keeply_cbt_daemon: " << ex.what() << "\n";
        return 1;
    }
}
#endif

#else

namespace keeply::rastreamento_eventos_base {

std::unique_ptr<MotorMonitor> createWindowsMotorMonitor(const fs::path&, bool) {
    throw std::runtime_error("Motor Windows disponivel apenas no Windows.");
}

} // namespace keeply::rastreamento_eventos_base

#ifdef KEEPLY_DAEMON_PROGRAM
int main() {
    return 1;
}
#endif

#endif

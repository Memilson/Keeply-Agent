#ifdef _WIN32
#include <windows.h>
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

void writePidFile() {
    fs::create_directories(getPidPath().parent_path());
    std::ofstream pid(getPidPath(), std::ios::trunc);
    if (!pid) throw std::runtime_error("Falha criando PID file do daemon.");
    pid << GetCurrentProcessId() << "\n";
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

} 

int main(int argc, char** argv) {
    try {
        fs::path root;
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--root" && i + 1 < argc) {
                root = argv[++i];
            } else if (arg == "--foreground") {
            } else if (arg == "--help") {
                std::cout << "Uso: keeply_daemon --root <diretorio> [--foreground]\n";
                return 0;
            } else {
                throw std::runtime_error("Argumento invalido: " + arg);
            }
        }

        if (root.empty()) throw std::runtime_error("Informe --root <diretorio>.");
        if (!fs::exists(root) || !fs::is_directory(root)) throw std::runtime_error("Root invalido para o daemon.");

        fs::create_directories(getPidPath().parent_path());
        writePidFile();

        if (!SetConsoleCtrlHandler(consoleHandler, TRUE)) {
            throw std::runtime_error("Falha registrando handler do console.");
        }

        WindowsDaemon daemon(root);
        daemon.run();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "keeply_daemon: " << ex.what() << "\n";
        return 1;
    }
}
#else
int main() {
    return 1;
}
#endif

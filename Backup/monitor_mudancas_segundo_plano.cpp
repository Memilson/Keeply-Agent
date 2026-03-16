#include "monitor_mudancas_segundo_plano.hpp"

#include "../keeply.hpp"
#include "rastreamento_mudancas.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#include <poll.h>
#include <sys/inotify.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace keeply {

namespace fs = std::filesystem;

namespace {

std::string normalizePath(const fs::path& path) {
    return path.lexically_normal().generic_string();
}

#ifdef __linux__
constexpr uint32_t kWatchMask =
    IN_CREATE | IN_MODIFY | IN_ATTRIB | IN_DELETE | IN_DELETE_SELF |
    IN_MOVED_FROM | IN_MOVED_TO | IN_CLOSE_WRITE | IN_ONLYDIR | IN_MOVE_SELF;
#endif

#ifdef _WIN32
class WinHandle {
    HANDLE handle_ = nullptr;

public:
    WinHandle() = default;
    explicit WinHandle(HANDLE handle) : handle_(handle) {}

    ~WinHandle() {
        reset();
    }

    WinHandle(const WinHandle&) = delete;
    WinHandle& operator=(const WinHandle&) = delete;

    WinHandle(WinHandle&& other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    WinHandle& operator=(WinHandle&& other) noexcept {
        if (this != &other) {
            reset();
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    void reset(HANDLE next = nullptr) {
        if (handle_ && handle_ != INVALID_HANDLE_VALUE) CloseHandle(handle_);
        handle_ = next;
    }

    HANDLE get() const {
        return handle_;
    }

    bool valid() const {
        return handle_ && handle_ != INVALID_HANDLE_VALUE;
    }
};
#endif

} // namespace

#ifdef __linux__

class BackgroundCbtWatcher::Impl {
public:
    void start(const fs::path& rootPath) {
        stop();

        std::error_code ec;
        const fs::path normalizedRoot = fs::absolute(rootPath, ec).lexically_normal();
        if (ec || normalizedRoot.empty() || !fs::exists(normalizedRoot) || !fs::is_directory(normalizedRoot)) {
            throw std::runtime_error("Root invalido para o watcher CBT: " + rootPath.string());
        }

        root_ = normalizedRoot;
        store_ = std::make_unique<EventStore>(defaultEventStorePath());
        store_->ensureRoot(root_);

        fd_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        if (fd_ < 0) throw std::runtime_error("Falha criando inotify para o watcher CBT.");

        stopRequested_.store(false);
        addWatchRecursive(root_);
        worker_ = std::thread([this]() { runLoop(); });
        running_.store(true);
    }

    void stop() noexcept {
        stopRequested_.store(true);
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
        if (worker_.joinable()) worker_.join();
        {
            std::lock_guard<std::mutex> lock(mu_);
            wdToPath_.clear();
        }
        store_.reset();
        running_.store(false);
    }

    bool running() const noexcept {
        return running_.load();
    }

    ~Impl() {
        stop();
    }

private:
    void addWatch(const fs::path& dirPath) {
        const int fd = fd_;
        if (fd < 0) return;
        if (dirPath != root_ && isExcludedBySystemPolicy(root_, dirPath)) return;
        const int wd = inotify_add_watch(fd, dirPath.c_str(), kWatchMask);
        if (wd < 0) return;
        std::lock_guard<std::mutex> lock(mu_);
        wdToPath_[wd] = dirPath;
    }

    void addWatchRecursive(const fs::path& dirPath) {
        addWatch(dirPath);
        std::error_code ec;
        fs::recursive_directory_iterator it(dirPath, fs::directory_options::skip_permission_denied, ec);
        fs::recursive_directory_iterator end;
        for (; it != end; it.increment(ec)) {
            if (ec) {
                ec.clear();
                continue;
            }
            std::error_code typeEc;
            if (it->is_directory(typeEc) && !typeEc) {
                if (isExcludedBySystemPolicy(root_, it->path())) {
                    it.disable_recursion_pending();
                    continue;
                }
                addWatch(it->path());
            }
        }
    }

    void runLoop() {
        std::array<char, 64 * 1024> buffer{};
        try {
            while (!stopRequested_.load()) {
                pollfd pfd{};
                pfd.fd = fd_;
                pfd.events = POLLIN;
                const int prc = poll(&pfd, 1, 1000);
                if (prc < 0) {
                    if (errno == EINTR) continue;
                    if (stopRequested_.load()) return;
                    throw std::runtime_error("Falha no poll do watcher CBT.");
                }
                if (prc == 0) continue;

                const ssize_t bytes = read(fd_, buffer.data(), buffer.size());
                if (bytes < 0) {
                    if (errno == EAGAIN || errno == EINTR) continue;
                    if (stopRequested_.load()) return;
                    throw std::runtime_error("Falha lendo eventos do inotify.");
                }

                ssize_t offset = 0;
                while (offset < bytes) {
                    auto* ev = reinterpret_cast<inotify_event*>(buffer.data() + offset);
                    handleEvent(*ev);
                    offset += sizeof(inotify_event) + ev->len;
                }
            }
        } catch (const std::exception& ex) {
            std::cerr << "[keeply][cbt][warn] watcher Linux falhou: " << ex.what() << "\n";
        }
    }

    void handleEvent(const inotify_event& ev) {
        fs::path basePath;
        {
            std::lock_guard<std::mutex> lock(mu_);
            const auto it = wdToPath_.find(ev.wd);
            if (it == wdToPath_.end()) return;
            basePath = it->second;
        }

        fs::path fullPath = basePath;
        if (ev.len > 0 && ev.name[0] != '\0') fullPath /= ev.name;
        if (isExcludedBySystemPolicy(root_, fullPath)) return;
        std::error_code relEc;
        fs::path relPath = fs::relative(fullPath, root_, relEc);
        const std::string relRaw = relPath.generic_string();
        if (relEc || relPath.empty() || relRaw.rfind("..", 0) == 0) return;

        const bool isDir = (ev.mask & IN_ISDIR) != 0;
        const std::string rel = normalizePath(relPath);

        if ((ev.mask & (IN_CREATE | IN_MOVED_TO)) && isDir) addWatchRecursive(fullPath);

        if (ev.mask & (IN_CREATE | IN_MOVED_TO)) {
            store_->appendEvent("upsert", rel, isDir);
        } else if (ev.mask & (IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE)) {
            store_->appendEvent("modify", rel, isDir);
        } else if (ev.mask & (IN_DELETE | IN_MOVED_FROM | IN_DELETE_SELF | IN_MOVE_SELF)) {
            store_->appendEvent("delete", rel, isDir);
        } else if (ev.mask & IN_IGNORED) {
            std::lock_guard<std::mutex> lock(mu_);
            wdToPath_.erase(ev.wd);
        }
    }

    std::atomic<bool> stopRequested_{false};
    std::atomic<bool> running_{false};
    std::thread worker_;
    std::mutex mu_;
    int fd_ = -1;
    fs::path root_;
    std::unique_ptr<EventStore> store_;
    std::unordered_map<int, fs::path> wdToPath_;
};

#elif defined(_WIN32)

class BackgroundCbtWatcher::Impl {
public:
    void start(const fs::path& rootPath) {
        stop();

        std::error_code ec;
        const fs::path normalizedRoot = fs::absolute(rootPath, ec).lexically_normal();
        if (ec || normalizedRoot.empty() || !fs::exists(normalizedRoot) || !fs::is_directory(normalizedRoot)) {
            throw std::runtime_error("Root invalido para o watcher CBT: " + rootPath.string());
        }

        root_ = normalizedRoot;
        store_ = std::make_unique<EventStore>(defaultEventStorePath());
        store_->ensureRoot(root_);

        dir_.reset(CreateFileW(
            root_.wstring().c_str(),
            FILE_LIST_DIRECTORY,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            nullptr,
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
            nullptr
        ));
        if (!dir_.valid()) throw std::runtime_error("Falha abrindo diretorio para monitoramento CBT.");

        event_.reset(CreateEventW(nullptr, TRUE, FALSE, nullptr));
        if (!event_.valid()) throw std::runtime_error("Falha criando evento do watcher CBT.");

        stopRequested_.store(false);
        worker_ = std::thread([this]() { runLoop(); });
        running_.store(true);
    }

    void stop() noexcept {
        stopRequested_.store(true);
        if (dir_.valid()) CancelIoEx(dir_.get(), nullptr);
        if (event_.valid()) SetEvent(event_.get());
        if (worker_.joinable()) worker_.join();
        store_.reset();
        dir_.reset();
        event_.reset();
        running_.store(false);
    }

    bool running() const noexcept {
        return running_.load();
    }

    ~Impl() {
        stop();
    }

private:
    void runLoop() {
        std::vector<unsigned char> buffer(64 * 1024);
        try {
            while (!stopRequested_.load()) {
                OVERLAPPED overlapped{};
                overlapped.hEvent = event_.get();
                ResetEvent(event_.get());

                const BOOL started = ReadDirectoryChangesW(
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
                    nullptr,
                    &overlapped,
                    nullptr
                );
                if (!started) {
                    const DWORD err = GetLastError();
                    if (stopRequested_.load()) return;
                    throw std::runtime_error("Falha iniciando ReadDirectoryChangesW. Codigo=" + std::to_string(err));
                }

                const DWORD waitRc = WaitForSingleObject(event_.get(), INFINITE);
                if (waitRc != WAIT_OBJECT_0) {
                    if (stopRequested_.load()) return;
                    throw std::runtime_error("Falha aguardando eventos do watcher CBT Windows.");
                }

                DWORD bytesReturned = 0;
                if (!GetOverlappedResult(dir_.get(), &overlapped, &bytesReturned, FALSE)) {
                    const DWORD err = GetLastError();
                    if ((err == ERROR_OPERATION_ABORTED || err == ERROR_IO_INCOMPLETE) && stopRequested_.load()) return;
                    throw std::runtime_error("Falha consumindo eventos do watcher CBT Windows. Codigo=" + std::to_string(err));
                }

                if (bytesReturned == 0) continue;
                parseBuffer(buffer.data(), bytesReturned);
            }
        } catch (const std::exception& ex) {
            std::cerr << "[keeply][cbt][warn] watcher Windows falhou: " << ex.what() << "\n";
        }
    }

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
        if (isExcludedBySystemPolicy(root_, fullPath)) return;

        std::error_code relEc;
        const fs::path relPath = fs::relative(fullPath, root_, relEc);
        const std::string relRaw = relPath.generic_string();
        if (relEc || relPath.empty() || relRaw.rfind("..", 0) == 0) return;

        std::string eventType;
        switch (info.Action) {
            case FILE_ACTION_ADDED:
            case FILE_ACTION_RENAMED_NEW_NAME:
                eventType = "upsert";
                break;
            case FILE_ACTION_MODIFIED:
                eventType = "modify";
                break;
            case FILE_ACTION_REMOVED:
            case FILE_ACTION_RENAMED_OLD_NAME:
                eventType = "delete";
                break;
            default:
                return;
        }

        std::error_code typeEc;
        bool isDir = false;
        if (eventType != "delete") isDir = fs::is_directory(fullPath, typeEc) && !typeEc;

        store_->appendEvent(eventType, normalizePath(relPath), isDir);
    }

    std::atomic<bool> stopRequested_{false};
    std::atomic<bool> running_{false};
    std::thread worker_;
    fs::path root_;
    std::unique_ptr<EventStore> store_;
    WinHandle dir_;
    WinHandle event_;
};

#else

class BackgroundCbtWatcher::Impl {
public:
    void start(const fs::path&) {
        throw std::runtime_error("Watcher CBT em background nao esta disponivel nesta plataforma.");
    }

    void stop() noexcept {}

    bool running() const noexcept {
        return false;
    }
};

#endif

BackgroundCbtWatcher::BackgroundCbtWatcher() : impl_(new Impl()) {}

BackgroundCbtWatcher::~BackgroundCbtWatcher() = default;

void BackgroundCbtWatcher::start(const fs::path& rootPath) {
    impl_->start(rootPath);
}

void BackgroundCbtWatcher::stop() noexcept {
    impl_->stop();
}

bool BackgroundCbtWatcher::running() const noexcept {
    return impl_->running();
}

} // namespace keeply

#include "inotify_service.hpp"

#include "../../keeply.hpp"
#include "../cbt.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#ifdef __linux__
#include <fcntl.h>
#include <poll.h>
#include <sys/inotify.h>
#include <unistd.h>
#endif

namespace keeply {

namespace fs = std::filesystem;

#ifdef __linux__

namespace {

constexpr uint32_t kWatchMask =
    IN_CREATE | IN_MODIFY | IN_ATTRIB | IN_DELETE | IN_DELETE_SELF |
    IN_MOVED_FROM | IN_MOVED_TO | IN_CLOSE_WRITE | IN_ONLYDIR | IN_MOVE_SELF;

std::string normalizePath(const fs::path& path) {
    return path.lexically_normal().generic_string();
}

} // namespace

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

#else

class BackgroundCbtWatcher::Impl {
public:
    void start(const fs::path&) {
        throw std::runtime_error("Watcher CBT em background disponivel apenas no Linux.");
    }

    void stop() noexcept {}

    bool running() const noexcept { return false; }
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

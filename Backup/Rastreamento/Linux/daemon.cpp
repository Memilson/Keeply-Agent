#include "../../../keeply.hpp"
#include "../rastreamento_mudancas.hpp"

#ifdef __linux__
#include <array>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <mutex>
#include <poll.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

#ifdef __linux__

namespace keeply::rastreamento_eventos_base {

namespace {

constexpr uint32_t kWatchMask =
    IN_CREATE | IN_MODIFY | IN_ATTRIB | IN_DELETE | IN_DELETE_SELF |
    IN_MOVED_FROM | IN_MOVED_TO | IN_CLOSE_WRITE | IN_ONLYDIR | IN_MOVE_SELF;

class MotorMonitorLinux final : public MotorMonitorBase {
public:
    MotorMonitorLinux(const fs::path& rootPath, bool respectSystemExclusionPolicy)
        : MotorMonitorBase(rootPath, respectSystemExclusionPolicy) {
        fd_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        if (fd_ < 0) throw std::runtime_error("Falha criando inotify para o watcher CBT.");
        addWatchRecursive(this->rootPath());
    }

    ~MotorMonitorLinux() override {
        requestStop();
    }

    void requestStop() noexcept override {
        if (fd_ >= 0) {
            close(fd_);
            fd_ = -1;
        }
    }

    void run(const std::function<bool()>& shouldStop) override {
        std::array<char, 64 * 1024> buffer{};
        while (!shouldStop()) {
            if (fd_ < 0) return;
            pollfd pfd{};
            pfd.fd = fd_;
            pfd.events = POLLIN;
            const int prc = poll(&pfd, 1, 1000);
            if (prc < 0) {
                if (errno == EINTR) continue;
                if (shouldStop()) return;
                throw std::runtime_error("Falha no poll do watcher CBT.");
            }
            if (prc == 0) continue;

            const ssize_t bytes = read(fd_, buffer.data(), buffer.size());
            if (bytes < 0) {
                if (errno == EAGAIN || errno == EINTR) continue;
                if (shouldStop()) return;
                throw std::runtime_error("Falha lendo eventos do inotify.");
            }

            ssize_t offset = 0;
            while (offset < bytes) {
                auto* event = reinterpret_cast<inotify_event*>(buffer.data() + offset);
                handleEvent(*event);
                offset += sizeof(inotify_event) + event->len;
            }
        }
    }

private:
    void addWatch(const fs::path& dirPath) {
        const int fd = fd_;
        if (fd < 0) return;
        if (dirPath != rootPath() && shouldIgnorePath(dirPath)) return;
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
                if (shouldIgnorePath(it->path())) {
                    it.disable_recursion_pending();
                    continue;
                }
                addWatch(it->path());
            }
        }
    }

    void handleEvent(const inotify_event& event) {
        fs::path basePath;
        {
            std::lock_guard<std::mutex> lock(mu_);
            const auto it = wdToPath_.find(event.wd);
            if (it == wdToPath_.end()) return;
            basePath = it->second;
        }

        fs::path fullPath = basePath;
        if (event.len > 0 && event.name[0] != '\0') fullPath /= event.name;
        if (shouldIgnorePath(fullPath)) return;

        const auto rel = buildRelativePath(fullPath);
        if (!rel) return;

        const bool isDir = (event.mask & IN_ISDIR) != 0;
        if ((event.mask & (IN_CREATE | IN_MOVED_TO)) && isDir) addWatchRecursive(fullPath);

        if (event.mask & (IN_CREATE | IN_MOVED_TO)) {
            appendEvent(TipoEventoMonitorado::Upsert, *rel, isDir);
        } else if (event.mask & (IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE)) {
            appendEvent(TipoEventoMonitorado::Modify, *rel, isDir);
        } else if (event.mask & (IN_DELETE | IN_MOVED_FROM | IN_DELETE_SELF | IN_MOVE_SELF)) {
            appendEvent(TipoEventoMonitorado::Delete, *rel, isDir);
        } else if (event.mask & IN_IGNORED) {
            std::lock_guard<std::mutex> lock(mu_);
            wdToPath_.erase(event.wd);
        }
    }

    int fd_ = -1;
    std::mutex mu_;
    std::unordered_map<int, fs::path> wdToPath_;
};

volatile std::sig_atomic_t gStopRequested = 0;

void handleSignal(int) {
    gStopRequested = 1;
}

void daemonizeProcess() {
    pid_t pid = fork();
    if (pid < 0) throw std::runtime_error("fork falhou.");
    if (pid > 0) std::exit(0);

    if (setsid() < 0) throw std::runtime_error("setsid falhou.");

    pid = fork();
    if (pid < 0) throw std::runtime_error("fork 2 falhou.");
    if (pid > 0) std::exit(0);

    umask(0);
    if (chdir("/") != 0) throw std::runtime_error("chdir falhou.");

    const int nullFd = open("/dev/null", O_RDWR);
    if (nullFd >= 0) {
        dup2(nullFd, STDIN_FILENO);
        dup2(nullFd, STDOUT_FILENO);
        dup2(nullFd, STDERR_FILENO);
        if (nullFd > STDERR_FILENO) close(nullFd);
    }
}

} 

std::unique_ptr<MotorMonitor> createLinuxMotorMonitor(const fs::path& rootPath,
                                                      bool respectSystemExclusionPolicy) {
    return std::make_unique<MotorMonitorLinux>(rootPath, respectSystemExclusionPolicy);
}

} 

#ifdef KEEPLY_DAEMON_PROGRAM
int main(int argc, char** argv) {
    try {
        keeply::rastreamento_eventos_base::OpcoesDaemonMonitor options;
        try {
            options = keeply::rastreamento_eventos_base::parseDaemonMonitorArgs(argc, argv, false);
        } catch (const std::runtime_error& ex) {
            if (std::string(ex.what()) == "__KEEPLY_DAEMON_HELP__") {
                std::cout << "Uso: keeply_daemon --root <diretorio> [--foreground]\n";
                return 0;
            }
            throw;
        }

        std::signal(SIGINT, keeply::rastreamento_eventos_base::handleSignal);
        std::signal(SIGTERM, keeply::rastreamento_eventos_base::handleSignal);

        if (!options.foreground) keeply::rastreamento_eventos_base::daemonizeProcess();
        const fs::path root = keeply::rastreamento_eventos_base::resolveAndPrepareDaemonMonitorRootOrThrow(
            options.rootPath,
            std::to_string(getpid())
        );

        keeply::rastreamento_eventos_base::MonitorRunner daemon(root, false);
        daemon.run([]() { return keeply::rastreamento_eventos_base::gStopRequested != 0; });
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "keeply_daemon: " << ex.what() << "\n";
        return 1;
    }
}
#endif

#else

namespace keeply::rastreamento_eventos_base {

std::unique_ptr<MotorMonitor> createLinuxMotorMonitor(const fs::path&, bool) {
    throw std::runtime_error("Motor Linux disponivel apenas no Linux.");
}

} 

#ifdef KEEPLY_DAEMON_PROGRAM
int main() {
    std::cerr << "keeply_daemon esta disponivel apenas no Linux.\n";
    return 1;
}
#endif

#endif

#include <array>
#include <csignal>
#include <cstdlib>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#include <poll.h>
#include <sys/inotify.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "../cbt.hpp"

namespace fs = std::filesystem;

#ifdef __linux__

namespace {

constexpr uint32_t kWatchMask =
    IN_CREATE | IN_MODIFY | IN_ATTRIB | IN_DELETE | IN_DELETE_SELF |
    IN_MOVED_FROM | IN_MOVED_TO | IN_CLOSE_WRITE | IN_ONLYDIR | IN_MOVE_SELF;

volatile std::sig_atomic_t gStopRequested = 0;

void handleSignal(int) {
    gStopRequested = 1;
}

std::string normalizePath(const fs::path& p) {
    return p.lexically_normal().generic_string();
}

class InotifyDaemon {
    int fd_ = -1;
    fs::path root_;
    keeply::EventStore store_;
    std::unordered_map<int, fs::path> wdToPath_;

public:
    explicit InotifyDaemon(const fs::path& root)
        : root_(fs::absolute(root).lexically_normal()), store_(keeply::defaultEventStorePath()) {
        fd_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        if (fd_ < 0) throw std::runtime_error("Falha criando inotify.");
        store_.ensureRoot(root_);
        addWatchRecursive(root_);
    }

    ~InotifyDaemon() {
        if (fd_ >= 0) close(fd_);
    }

    void run() {
        std::array<char, 64 * 1024> buffer{};
        while (!gStopRequested) {
            pollfd pfd{};
            pfd.fd = fd_;
            pfd.events = POLLIN;
            const int prc = poll(&pfd, 1, 1000);
            if (prc < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error("Falha no poll do daemon.");
            }
            if (prc == 0) continue;

            const ssize_t bytes = read(fd_, buffer.data(), buffer.size());
            if (bytes < 0) {
                if (errno == EAGAIN || errno == EINTR) continue;
                throw std::runtime_error("Falha lendo eventos do inotify.");
            }

            ssize_t offset = 0;
            while (offset < bytes) {
                auto* ev = reinterpret_cast<inotify_event*>(buffer.data() + offset);
                handleEvent(*ev);
                offset += sizeof(inotify_event) + ev->len;
            }
        }
    }

private:
    void addWatch(const fs::path& dirPath) {
        const int wd = inotify_add_watch(fd_, dirPath.c_str(), kWatchMask);
        if (wd < 0) return;
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
            if (it->is_directory(typeEc) && !typeEc) addWatch(it->path());
        }
    }

    void handleEvent(const inotify_event& ev) {
        const auto it = wdToPath_.find(ev.wd);
        if (it == wdToPath_.end()) return;

        fs::path fullPath = it->second;
        if (ev.len > 0 && ev.name[0] != '\0') fullPath /= ev.name;
        std::error_code relEc;
        fs::path relPath = fs::relative(fullPath, root_, relEc);
        const std::string relRaw = relPath.generic_string();
        if (relEc || relPath.empty() || relRaw.rfind("..", 0) == 0) return;

        const bool isDir = (ev.mask & IN_ISDIR) != 0;
        const std::string rel = normalizePath(relPath);

        if ((ev.mask & (IN_CREATE | IN_MOVED_TO)) && isDir) addWatchRecursive(fullPath);

        if (ev.mask & (IN_CREATE | IN_MOVED_TO)) {
            store_.appendEvent("upsert", rel, isDir);
        } else if (ev.mask & (IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE)) {
            store_.appendEvent("modify", rel, isDir);
        } else if (ev.mask & (IN_DELETE | IN_MOVED_FROM | IN_DELETE_SELF | IN_MOVE_SELF)) {
            store_.appendEvent("delete", rel, isDir);
        } else if (ev.mask & IN_IGNORED) {
            wdToPath_.erase(ev.wd);
        }
    }
};

void writePidFile() {
    fs::create_directories(keeply::defaultEventStorePidPath().parent_path());
    std::ofstream pid(keeply::defaultEventStorePidPath(), std::ios::trunc);
    if (!pid) throw std::runtime_error("Falha criando PID file do daemon.");
    pid << getpid() << "\n";
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

} // namespace

int main(int argc, char** argv) {
    try {
        fs::path root;
        bool foreground = false;

        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--root" && i + 1 < argc) {
                root = argv[++i];
            } else if (arg == "--foreground") {
                foreground = true;
            } else if (arg == "--help") {
                std::cout << "Uso: keeply_daemon --root <diretorio> [--foreground]\n";
                return 0;
            } else {
                throw std::runtime_error("Argumento invalido: " + arg);
            }
        }

        if (root.empty()) throw std::runtime_error("Informe --root <diretorio>.");
        if (!fs::exists(root) || !fs::is_directory(root)) {
            throw std::runtime_error("Root invalido para o daemon.");
        }

        std::signal(SIGINT, handleSignal);
        std::signal(SIGTERM, handleSignal);

        if (!foreground) daemonizeProcess();
        writePidFile();

        InotifyDaemon daemon(root);
        daemon.run();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "keeply_daemon: " << ex.what() << "\n";
        return 1;
    }
}

#else

int main() {
    std::cerr << "keeply_daemon esta disponivel apenas no Linux.\n";
    return 1;
}

#endif

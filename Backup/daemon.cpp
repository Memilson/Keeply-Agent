#include <sqlite3.h>

#include <chrono>
#include <array>
#include <csignal>
#include <cstdlib>
#include <cstring>
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

namespace fs = std::filesystem;

#ifdef __linux__

namespace {

constexpr const char* kDbPath = "/tmp/keeply/keeplyintf.kipy";
constexpr const char* kPidPath = "/tmp/keeply/keeplyintf.pid";
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

sqlite3_int64 nowUnixSeconds() {
    using clock = std::chrono::system_clock;
    return static_cast<sqlite3_int64>(
        std::chrono::duration_cast<std::chrono::seconds>(
            clock::now().time_since_epoch()).count());
}

class Db {
    sqlite3* db_ = nullptr;

public:
    explicit Db(const fs::path& path) {
        if (sqlite3_open(path.string().c_str(), &db_) != SQLITE_OK) {
            std::string msg = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
            if (db_) sqlite3_close(db_);
            throw std::runtime_error("Falha abrindo banco do daemon: " + msg);
        }
        exec("PRAGMA journal_mode=WAL;");
        exec("PRAGMA synchronous=NORMAL;");
        exec("PRAGMA foreign_keys=ON;");
        exec("CREATE TABLE IF NOT EXISTS roots ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "root_path TEXT NOT NULL UNIQUE,"
             "created_at INTEGER NOT NULL,"
             "updated_at INTEGER NOT NULL);");
        exec("CREATE TABLE IF NOT EXISTS events ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "root_id INTEGER NOT NULL,"
             "seq INTEGER NOT NULL,"
             "event_type TEXT NOT NULL,"
             "rel_path TEXT NOT NULL,"
             "is_dir INTEGER NOT NULL DEFAULT 0,"
             "event_time INTEGER NOT NULL,"
             "FOREIGN KEY(root_id) REFERENCES roots(id) ON DELETE CASCADE);");
        exec("CREATE INDEX IF NOT EXISTS idx_events_root_seq ON events(root_id, seq);");
    }

    ~Db() {
        if (db_) sqlite3_close(db_);
    }

    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }

    sqlite3* raw() { return db_; }
};

class EventStore {
    Db db_;
    sqlite3_int64 rootId_ = 0;
    sqlite3_int64 nextSeq_ = 1;

public:
    explicit EventStore(const fs::path& dbPath) : db_(dbPath) {}

    void ensureRoot(const fs::path& rootPath) {
        const std::string root = normalizePath(fs::absolute(rootPath));
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db_.raw(),
                               "INSERT INTO roots(root_path, created_at, updated_at) "
                               "VALUES(?, ?, ?) "
                               "ON CONFLICT(root_path) DO UPDATE SET updated_at=excluded.updated_at;",
                               -1, &st, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Falha preparando roots.");
        }
        const sqlite3_int64 ts = nowUnixSeconds();
        sqlite3_bind_text(st, 1, root.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 2, ts);
        sqlite3_bind_int64(st, 3, ts);
        if (sqlite3_step(st) != SQLITE_DONE) {
            sqlite3_finalize(st);
            throw std::runtime_error("Falha gravando root do daemon.");
        }
        sqlite3_finalize(st);

        if (sqlite3_prepare_v2(db_.raw(),
                               "SELECT id, COALESCE(MAX(seq), 0) + 1 FROM roots "
                               "LEFT JOIN events ON events.root_id = roots.id "
                               "WHERE root_path=? GROUP BY roots.id;",
                               -1, &st, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Falha lendo root do daemon.");
        }
        sqlite3_bind_text(st, 1, root.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st) != SQLITE_ROW) {
            sqlite3_finalize(st);
            throw std::runtime_error("Root do daemon nao encontrado.");
        }
        rootId_ = sqlite3_column_int64(st, 0);
        nextSeq_ = sqlite3_column_int64(st, 1);
        sqlite3_finalize(st);
    }

    void appendEvent(const std::string& type, const std::string& relPath, bool isDir) {
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db_.raw(),
                               "INSERT INTO events(root_id, seq, event_type, rel_path, is_dir, event_time) "
                               "VALUES(?, ?, ?, ?, ?, ?);",
                               -1, &st, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Falha preparando evento do daemon.");
        }
        sqlite3_bind_int64(st, 1, rootId_);
        sqlite3_bind_int64(st, 2, nextSeq_++);
        sqlite3_bind_text(st, 3, type.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, relPath.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 5, isDir ? 1 : 0);
        sqlite3_bind_int64(st, 6, nowUnixSeconds());
        if (sqlite3_step(st) != SQLITE_DONE) {
            sqlite3_finalize(st);
            throw std::runtime_error("Falha gravando evento do daemon.");
        }
        sqlite3_finalize(st);
    }
};

class InotifyDaemon {
    int fd_ = -1;
    fs::path root_;
    EventStore store_;
    std::unordered_map<int, fs::path> wdToPath_;

public:
    explicit InotifyDaemon(const fs::path& root)
        : root_(fs::absolute(root).lexically_normal()), store_(kDbPath) {
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
    fs::create_directories("/tmp/keeply");
    std::ofstream pid(kPidPath, std::ios::trunc);
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

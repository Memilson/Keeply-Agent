#include "../keeply.hpp"
#include "cbt.hpp"
#include <sqlite3.h>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>
#endif

namespace keeply {
namespace {
enum class TokenBackend : std::uint64_t { Native = 1, Daemon = 2 };
constexpr std::uint64_t kTokenBackendShift = 60;
constexpr std::uint64_t kTokenBackendMask = 0xFULL << kTokenBackendShift;
constexpr std::uint64_t kTokenValueMask = ~kTokenBackendMask;
struct DecodedToken {
    TokenBackend backend = TokenBackend::Native;
    std::uint64_t value = 0;
};
std::uint64_t encodeToken(TokenBackend backend, std::uint64_t value) {
    return (static_cast<std::uint64_t>(backend) << kTokenBackendShift) | (value & kTokenValueMask);
}
DecodedToken decodeToken(std::uint64_t token) {
    if (token == 0) return {};
    const auto rawBackend = (token & kTokenBackendMask) >> kTokenBackendShift;
    if (rawBackend == static_cast<std::uint64_t>(TokenBackend::Daemon)) return {TokenBackend::Daemon, token & kTokenValueMask};
    return {TokenBackend::Native, token & kTokenValueMask};
}
std::string toLowerAscii(std::string s) {
    for (char& c : s) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return s;
}
std::string normalizeEventType(std::string type) {
    return toLowerAscii(std::move(type));
}
std::string normalizeDbPath(const fs::path& path) {
    return pathToUtf8(fs::absolute(path).lexically_normal());
}
long long fileTimeToUnixSecondsLocal(const fs::file_time_type& ftp) {
    using namespace std::chrono;
    const auto sctp = time_point_cast<system_clock::duration>(ftp - fs::file_time_type::clock::now() + system_clock::now());
    return duration_cast<seconds>(sctp.time_since_epoch()).count();
}
class SqlStmt {
    sqlite3_stmt* st_ = nullptr;
public:
    SqlStmt(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &st_, nullptr) != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db));
    }
    ~SqlStmt() {
        if (st_) sqlite3_finalize(st_);
    }
    SqlStmt(const SqlStmt&) = delete;
    SqlStmt& operator=(const SqlStmt&) = delete;
    SqlStmt(SqlStmt&& other) noexcept : st_(other.st_) {
        other.st_ = nullptr;
    }
    SqlStmt& operator=(SqlStmt&& other) noexcept {
        if (this != &other) {
            if (st_) sqlite3_finalize(st_);
            st_ = other.st_;
            other.st_ = nullptr;
        }
        return *this;
    }
    sqlite3_stmt* get() const { return st_; }
};
class SqlTransaction {
    sqlite3* db_ = nullptr;
    bool committed_ = false;
public:
    explicit SqlTransaction(sqlite3* db) : db_(db) {
        char* err = nullptr;
        if (sqlite3_exec(db_, "BEGIN IMMEDIATE;", nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }
    ~SqlTransaction() {
        if (!committed_) sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
    }
    void commit() {
        char* err = nullptr;
        if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);
        }
        committed_ = true;
    }
};
void execSql(sqlite3* db, const char* sql, const char* ctx) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "erro sqlite";
        if (err) sqlite3_free(err);
        throw std::runtime_error(std::string(ctx) + ": " + msg);
    }
}
#ifdef _WIN32
std::string normWinPath(std::string s) {
    for (char& c : s) if (c == '/') c = '\\';
    if (s.rfind("\\\\?\\", 0) == 0) s = s.substr(4);
    while (!s.empty() && s.back() == '\\') s.pop_back();
    return toLowerAscii(std::move(s));
}
struct WinHandle {
    HANDLE h = INVALID_HANDLE_VALUE;
    WinHandle() = default;
    explicit WinHandle(HANDLE x) : h(x) {}
    ~WinHandle() {
        if (h != INVALID_HANDLE_VALUE) CloseHandle(h);
    }
    WinHandle(const WinHandle&) = delete;
    WinHandle& operator=(const WinHandle&) = delete;
    WinHandle(WinHandle&& o) noexcept : h(o.h) {
        o.h = INVALID_HANDLE_VALUE;
    }
    WinHandle& operator=(WinHandle&& o) noexcept {
        if (this != &o) {
            if (h != INVALID_HANDLE_VALUE) CloseHandle(h);
            h = o.h;
            o.h = INVALID_HANDLE_VALUE;
        }
        return *this;
    }
    bool valid() const { return h != INVALID_HANDLE_VALUE; }
};
#endif
} 

class EventStore::Db {
    sqlite3* db_ = nullptr;
public:
    explicit Db(const fs::path& path) {
        if (sqlite3_open_path(path, &db_) != SQLITE_OK) {
            std::string msg = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
            if (db_) sqlite3_close(db_);
            throw std::runtime_error("Falha abrindo banco do daemon: " + msg);
        }
        execSql(db_, "PRAGMA journal_mode=WAL;", "Falha configurando journal_mode");
        execSql(db_, "PRAGMA synchronous=NORMAL;", "Falha configurando synchronous");
        execSql(db_, "PRAGMA foreign_keys=ON;", "Falha configurando foreign_keys");
        execSql(db_, "CREATE TABLE IF NOT EXISTS cbt_event_roots(id INTEGER PRIMARY KEY AUTOINCREMENT,root_path TEXT NOT NULL UNIQUE,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);", "Falha criando cbt_event_roots");
        execSql(db_, "CREATE TABLE IF NOT EXISTS cbt_events(id INTEGER PRIMARY KEY AUTOINCREMENT,root_id INTEGER NOT NULL,seq INTEGER NOT NULL,event_type TEXT NOT NULL,rel_path TEXT NOT NULL,is_dir INTEGER NOT NULL DEFAULT 0,event_time INTEGER NOT NULL,FOREIGN KEY(root_id) REFERENCES cbt_event_roots(id) ON DELETE CASCADE);", "Falha criando cbt_events");
        execSql(db_, "CREATE INDEX IF NOT EXISTS idx_cbt_events_root_seq ON cbt_events(root_id, seq);", "Falha criando idx_cbt_events_root_seq");
    }
    ~Db() {
        if (db_) sqlite3_close(db_);
    }
    sqlite3* raw() const { return db_; }
};

EventStore::EventStore(const fs::path& dbPath) : db_(new Db(dbPath)) {}
EventStore::~EventStore() { delete db_; }

std::string EventStore::normalizePath(const fs::path& path) {
    return normalizeDbPath(path);
}

std::int64_t EventStore::nowUnixSeconds() {
    using clock = std::chrono::system_clock;
    return static_cast<std::int64_t>(std::chrono::duration_cast<std::chrono::seconds>(clock::now().time_since_epoch()).count());
}

void EventStore::ensureRoot(const fs::path& rootPath) {
    const std::string root = normalizePath(rootPath);
    {
        SqlStmt st(db_->raw(), "INSERT INTO cbt_event_roots(root_path, created_at, updated_at) VALUES(?, ?, ?) ON CONFLICT(root_path) DO UPDATE SET updated_at=excluded.updated_at;");
        const auto ts = nowUnixSeconds();
        sqlite3_bind_text(st.get(), 1, root.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st.get(), 2, ts);
        sqlite3_bind_int64(st.get(), 3, ts);
        if (sqlite3_step(st.get()) != SQLITE_DONE) throw std::runtime_error("Falha gravando root do daemon.");
    }
    {
        SqlStmt st(db_->raw(), "SELECT r.id, COALESCE(MAX(e.seq), 0) + 1 FROM cbt_event_roots r LEFT JOIN cbt_events e ON e.root_id = r.id WHERE r.root_path=? GROUP BY r.id;");
        sqlite3_bind_text(st.get(), 1, root.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st.get()) != SQLITE_ROW) throw std::runtime_error("Root do daemon nao encontrado.");
        rootId_ = sqlite3_column_int64(st.get(), 0);
        nextSeq_ = sqlite3_column_int64(st.get(), 1);
    }
}

void EventStore::appendEvent(const std::string& type, const std::string& relPath, bool isDir) {
    SqlStmt st(db_->raw(), "INSERT INTO cbt_events(root_id, seq, event_type, rel_path, is_dir, event_time) VALUES(?, ?, ?, ?, ?, ?);");
    sqlite3_bind_int64(st.get(), 1, rootId_);
    sqlite3_bind_int64(st.get(), 2, nextSeq_++);
    sqlite3_bind_text(st.get(), 3, type.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st.get(), 4, relPath.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(st.get(), 5, isDir ? 1 : 0);
    sqlite3_bind_int64(st.get(), 6, nowUnixSeconds());
    if (sqlite3_step(st.get()) != SQLITE_DONE) throw std::runtime_error("Falha gravando evento do daemon.");
}

std::optional<std::uint64_t> EventStore::latestToken(const fs::path& rootPath) const {
    const std::string root = normalizePath(rootPath);
    SqlStmt st(db_->raw(), "SELECT COALESCE(MAX(e.seq), 0) FROM cbt_event_roots r LEFT JOIN cbt_events e ON e.root_id = r.id WHERE r.root_path=?;");
    sqlite3_bind_text(st.get(), 1, root.c_str(), -1, SQLITE_TRANSIENT);
    if (sqlite3_step(st.get()) != SQLITE_ROW) return std::nullopt;
    if (sqlite3_column_type(st.get(), 0) == SQLITE_NULL) return std::nullopt;
    const auto token = static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 0));
    if (token == 0) return std::nullopt;
    return token;
}

std::vector<ChangedFile> EventStore::loadChangesSince(const fs::path& rootPath, std::uint64_t lastToken, std::uint64_t& newToken) const {
    const std::string root = normalizePath(rootPath);
    sqlite3_int64 rootId = 0;
    {
        SqlStmt st(db_->raw(), "SELECT r.id, COALESCE(MAX(e.seq), 0) FROM cbt_event_roots r LEFT JOIN cbt_events e ON e.root_id = r.id WHERE r.root_path=? GROUP BY r.id;");
        sqlite3_bind_text(st.get(), 1, root.c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(st.get()) != SQLITE_ROW) throw std::runtime_error("Root do daemon nao encontrado.");
        rootId = sqlite3_column_int64(st.get(), 0);
        newToken = static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 1));
    }
    if (newToken <= lastToken) return {};
    SqlStmt st(db_->raw(), "SELECT e.rel_path, e.event_type FROM cbt_events e JOIN (SELECT rel_path, MAX(seq) AS max_seq FROM cbt_events WHERE root_id=? AND seq>? AND is_dir=0 GROUP BY rel_path) x ON x.rel_path = e.rel_path AND x.max_seq = e.seq WHERE e.root_id=? ORDER BY e.seq ASC;");
    sqlite3_bind_int64(st.get(), 1, rootId);
    sqlite3_bind_int64(st.get(), 2, static_cast<sqlite3_int64>(lastToken));
    sqlite3_bind_int64(st.get(), 3, rootId);
    std::vector<ChangedFile> out;
    while (sqlite3_step(st.get()) == SQLITE_ROW) {
        const auto* relText = sqlite3_column_text(st.get(), 0);
        if (!relText) continue;
        const auto* typeText = sqlite3_column_text(st.get(), 1);
        const std::string relPath = reinterpret_cast<const char*>(relText);
        const std::string eventType = typeText ? normalizeEventType(reinterpret_cast<const char*>(typeText)) : std::string{};
        out.push_back({relPath, eventType == "delete"});
    }
    return out;
}

fs::path defaultEventStorePath() {
    return defaultKeeplyStateDir() / "keeplyintf.kipy";
}

fs::path defaultEventStorePidPath() {
    return defaultEventStorePath().parent_path() / "keeplyintf.pid";
}

fs::path defaultEventStoreRootPath() {
    return defaultEventStorePath().parent_path() / "keeplyintf.root";
}

fs::path defaultNativeStateStorePath() {
    return defaultKeeplyStateDir() / "keeply_state.kipy";
}

#ifdef _WIN32
namespace {
class WindowsUSNTracker : public ChangeTracker {
    WinHandle hVol_;
    fs::path root_;
    std::string rootNorm_;
    fs::path resolvePathFromFileId(std::uint64_t fileId) {
        FILE_ID_DESCRIPTOR fd{};
        fd.dwSize = sizeof(FILE_ID_DESCRIPTOR);
        fd.Type = FileIdType;
        fd.FileId.QuadPart = static_cast<LONGLONG>(fileId);
        HANDLE hFile = OpenFileById(hVol_.h, &fd, 0, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, FILE_FLAG_BACKUP_SEMANTICS);
        if (hFile == INVALID_HANDLE_VALUE) return {};
        WinHandle hf(hFile);
        DWORD need = GetFinalPathNameByHandleW(hf.h, nullptr, 0, FILE_NAME_NORMALIZED);
        if (need == 0) return {};
        std::wstring buf(need, L'\0');
        DWORD got = GetFinalPathNameByHandleW(hf.h, buf.data(), need, FILE_NAME_NORMALIZED);
        if (got == 0 || got >= need) return {};
        buf.resize(got);
        return fs::path(buf).lexically_normal();
    }
public:
    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "usn_journal"; }
    void startTracking(const fs::path& rootPath) override {
        root_ = fs::absolute(rootPath).lexically_normal();
        rootNorm_ = normWinPath(pathToUtf8(root_));
        const auto rn = root_.root_name().string();
        if (rn.size() < 2 || rn[1] != ':') throw std::runtime_error("Root invalido para USN Tracker.");
        const std::wstring volPath = L"\\\\.\\" + root_.root_name().wstring();
        HANDLE hv = CreateFileW(volPath.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING, 0, nullptr);
        if (hv == INVALID_HANDLE_VALUE) throw std::runtime_error("Falha ao abrir volume NTFS. Keeply precisa rodar como Administrador.");
        hVol_ = WinHandle(hv);
        USN_JOURNAL_DATA jd{};
        DWORD br = 0;
        if (!DeviceIoControl(hVol_.h, FSCTL_QUERY_USN_JOURNAL, nullptr, 0, &jd, sizeof(jd), &br, nullptr)) throw std::runtime_error("Falha ao consultar USN Journal. O volume e NTFS?");
    }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        std::vector<ChangedFile> changes;
        std::unordered_set<std::string> seen;
        USN_JOURNAL_DATA jd{};
        DWORD br = 0;
        if (!DeviceIoControl(hVol_.h, FSCTL_QUERY_USN_JOURNAL, nullptr, 0, &jd, sizeof(jd), &br, nullptr)) throw std::runtime_error("Falha ao consultar USN Journal.");
        if (lastToken == 0) {
            newToken = static_cast<std::uint64_t>(jd.NextUsn);
            return changes;
        }
        READ_USN_JOURNAL_DATA rd{};
        rd.StartUsn = static_cast<USN>(lastToken);
        rd.ReasonMask = USN_REASON_DATA_OVERWRITE | USN_REASON_DATA_EXTEND | USN_REASON_DATA_TRUNCATION | USN_REASON_FILE_CREATE | USN_REASON_FILE_DELETE | USN_REASON_RENAME_OLD_NAME | USN_REASON_RENAME_NEW_NAME | USN_REASON_BASIC_INFO_CHANGE | USN_REASON_SECURITY_CHANGE;
        rd.ReturnOnlyOnClose = FALSE;
        rd.Timeout = 0;
        rd.BytesToWaitFor = 0;
        rd.UsnJournalID = jd.UsnJournalID;
        alignas(8) char buffer[1 << 16];
        DWORD bytesRead = 0;
        newToken = static_cast<std::uint64_t>(jd.NextUsn);
        while (true) {
            if (!DeviceIoControl(hVol_.h, FSCTL_READ_USN_JOURNAL, &rd, sizeof(rd), buffer, sizeof(buffer), &bytesRead, nullptr)) break;
            if (bytesRead <= sizeof(USN)) break;
            auto* nextUsn = reinterpret_cast<USN*>(buffer);
            rd.StartUsn = *nextUsn;
            newToken = static_cast<std::uint64_t>(*nextUsn);
            auto* rec = reinterpret_cast<USN_RECORD*>(reinterpret_cast<unsigned char*>(buffer) + sizeof(USN));
            while (reinterpret_cast<unsigned char*>(rec) < reinterpret_cast<unsigned char*>(buffer) + bytesRead) {
                if (rec->RecordLength == 0) break;
                if ((rec->FileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0) {
                    const bool isDeleted = (rec->Reason & USN_REASON_FILE_DELETE) != 0;
                    const fs::path full = resolvePathFromFileId(static_cast<std::uint64_t>(rec->FileReferenceNumber));
                    if (!full.empty()) {
                        std::string fullNorm = normWinPath(pathToUtf8(full));
                        if (fullNorm.size() >= rootNorm_.size() && fullNorm.compare(0, rootNorm_.size(), rootNorm_) == 0) {
                            std::error_code relEc;
                            fs::path relPath = fs::relative(full, root_, relEc);
                            if (!relEc && !relPath.empty()) {
                                const std::string rel = relPath.generic_string();
                                if (!rel.empty() && seen.insert(rel).second) changes.push_back({rel, isDeleted});
                            }
                        }
                    }
                }
                rec = reinterpret_cast<USN_RECORD*>(reinterpret_cast<unsigned char*>(rec) + rec->RecordLength);
            }
        }
        return changes;
    }
};
std::unique_ptr<ChangeTracker> makePlatformTracker() {
    return std::make_unique<WindowsUSNTracker>();
}
}
#elif defined(__linux__)
namespace {
struct LinuxTrackedFile {
    sqlite3_int64 size = 0;
    sqlite3_int64 mtime = 0;
};
class LinuxTrackerDb {
    sqlite3* db_ = nullptr;
public:
    explicit LinuxTrackerDb(const fs::path& path) {
        if (sqlite3_open_path(path, &db_) != SQLITE_OK) {
            std::string msg = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
            if (db_) sqlite3_close(db_);
            throw std::runtime_error("Falha abrindo banco CBT Linux: " + msg);
        }
        execSql(db_, "PRAGMA journal_mode=WAL;", "Falha configurando journal_mode CBT Linux");
        execSql(db_, "PRAGMA synchronous=NORMAL;", "Falha configurando synchronous CBT Linux");
        execSql(db_, "PRAGMA foreign_keys=ON;", "Falha configurando foreign_keys CBT Linux");
        execSql(db_, "CREATE TABLE IF NOT EXISTS cbt_state_roots(id INTEGER PRIMARY KEY AUTOINCREMENT,root_path TEXT NOT NULL UNIQUE,token INTEGER NOT NULL DEFAULT 0);", "Falha criando cbt_state_roots");
        execSql(db_, "CREATE TABLE IF NOT EXISTS cbt_state_files(root_id INTEGER NOT NULL,rel_path TEXT NOT NULL,size INTEGER NOT NULL,mtime INTEGER NOT NULL,PRIMARY KEY(root_id, rel_path),FOREIGN KEY(root_id) REFERENCES cbt_state_roots(id) ON DELETE CASCADE);", "Falha criando cbt_state_files");
    }
    ~LinuxTrackerDb() {
        if (db_) sqlite3_close(db_);
    }
    sqlite3* raw() const { return db_; }
};
bool isExcludedLinuxTrackerPath(const fs::path& rootPath, const fs::path& candidatePath) {
    const fs::path normalizedRoot = fs::absolute(rootPath).lexically_normal();
    if (normalizedRoot != fs::path("/")) return false;
    const fs::path candidate = fs::absolute(candidatePath).lexically_normal();
    static const std::vector<fs::path> excludedRoots = defaultSystemExcludedRoots();
    for (const auto& excludedRoot : excludedRoots) {
        auto excludedIt = excludedRoot.begin();
        auto candidateIt = candidate.begin();
        for (; excludedIt != excludedRoot.end(); ++excludedIt, ++candidateIt) {
            if (candidateIt == candidate.end() || *excludedIt != *candidateIt) break;
        }
        if (excludedIt == excludedRoot.end()) return true;
    }
    return false;
}
std::unordered_map<std::string, LinuxTrackedFile> scanCurrentLinuxFiles(const fs::path& rootPath) {
    std::unordered_map<std::string, LinuxTrackedFile> files;
    std::error_code itEc;
    fs::recursive_directory_iterator it(rootPath, fs::directory_options::skip_permission_denied, itEc);
    fs::recursive_directory_iterator end;
    for (; it != end; it.increment(itEc)) {
        if (itEc) {
            itEc.clear();
            continue;
        }
        std::error_code typeEc;
        if (it->is_directory(typeEc) && !typeEc && isExcludedLinuxTrackerPath(rootPath, it->path())) {
            it.disable_recursion_pending();
            continue;
        }
        typeEc.clear();
        if (!it->is_regular_file(typeEc)) continue;
        std::error_code relEc;
        fs::path relPath = fs::relative(it->path(), rootPath, relEc);
        if (relEc) continue;
        std::error_code sizeEc;
        auto fileSize = it->file_size(sizeEc);
        if (sizeEc) continue;
        std::error_code timeEc;
        auto mtimeRaw = fs::last_write_time(it->path(), timeEc);
        if (timeEc) continue;
        files.emplace(relPath.generic_string(), LinuxTrackedFile{static_cast<sqlite3_int64>(fileSize), static_cast<sqlite3_int64>(fileTimeToUnixSecondsLocal(mtimeRaw))});
    }
    return files;
}
class LinuxStateTracker : public ChangeTracker {
    fs::path root_;
    std::unique_ptr<LinuxTrackerDb> db_;
    sqlite3_int64 rootId_ = 0;
    sqlite3_int64 storedToken_ = 0;
    void ensureRootRow() {
        {
            SqlStmt st(db_->raw(), "INSERT OR IGNORE INTO cbt_state_roots(root_path, token) VALUES(?, 0);");
            const std::string root = normalizeDbPath(root_);
            sqlite3_bind_text(st.get(), 1, root.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st.get()) != SQLITE_DONE) throw std::runtime_error("Falha inserindo root no CBT Linux.");
        }
        {
            SqlStmt st(db_->raw(), "SELECT id, token FROM cbt_state_roots WHERE root_path=?;");
            const std::string root = normalizeDbPath(root_);
            sqlite3_bind_text(st.get(), 1, root.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(st.get()) != SQLITE_ROW) throw std::runtime_error("Root do CBT Linux nao encontrado.");
            rootId_ = sqlite3_column_int64(st.get(), 0);
            storedToken_ = sqlite3_column_int64(st.get(), 1);
        }
    }
    std::unordered_map<std::string, LinuxTrackedFile> loadTrackedFiles() const {
        std::unordered_map<std::string, LinuxTrackedFile> files;
        SqlStmt st(db_->raw(), "SELECT rel_path, size, mtime FROM cbt_state_files WHERE root_id=?;");
        sqlite3_bind_int64(st.get(), 1, rootId_);
        while (sqlite3_step(st.get()) == SQLITE_ROW) {
            const auto* relText = sqlite3_column_text(st.get(), 0);
            if (!relText) continue;
            files.emplace(reinterpret_cast<const char*>(relText), LinuxTrackedFile{sqlite3_column_int64(st.get(), 1), sqlite3_column_int64(st.get(), 2)});
        }
        return files;
    }
    void persistCurrentState(const std::unordered_map<std::string, LinuxTrackedFile>& files, sqlite3_int64 newToken) {
        SqlTransaction tx(db_->raw());
        {
            SqlStmt st(db_->raw(), "UPDATE cbt_state_roots SET token=? WHERE id=?;");
            sqlite3_bind_int64(st.get(), 1, newToken);
            sqlite3_bind_int64(st.get(), 2, rootId_);
            if (sqlite3_step(st.get()) != SQLITE_DONE) throw std::runtime_error("Falha atualizando token do CBT Linux.");
        }
        {
            SqlStmt st(db_->raw(), "DELETE FROM cbt_state_files WHERE root_id=?;");
            sqlite3_bind_int64(st.get(), 1, rootId_);
            if (sqlite3_step(st.get()) != SQLITE_DONE) throw std::runtime_error("Falha limpando arquivos do CBT Linux.");
        }
        {
            SqlStmt st(db_->raw(), "INSERT INTO cbt_state_files(root_id, rel_path, size, mtime) VALUES(?,?,?,?);");
            for (const auto& [relPath, info] : files) {
                sqlite3_reset(st.get());
                sqlite3_clear_bindings(st.get());
                sqlite3_bind_int64(st.get(), 1, rootId_);
                sqlite3_bind_text(st.get(), 2, relPath.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(st.get(), 3, info.size);
                sqlite3_bind_int64(st.get(), 4, info.mtime);
                if (sqlite3_step(st.get()) != SQLITE_DONE) throw std::runtime_error("Falha gravando arquivos do CBT Linux.");
            }
        }
        tx.commit();
        storedToken_ = newToken;
    }
public:
    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "linux_state_sqlite"; }
    void startTracking(const fs::path& rootPath) override {
        root_ = fs::absolute(rootPath).lexically_normal();
        std::error_code mkEc;
        fs::create_directories(defaultNativeStateStorePath().parent_path(), mkEc);
        if (mkEc) throw std::runtime_error("Falha criando diretorio do CBT Linux: " + mkEc.message());
        db_ = std::make_unique<LinuxTrackerDb>(defaultNativeStateStorePath());
        ensureRootRow();
    }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        if (!db_ || rootId_ == 0) throw std::runtime_error("CBT Linux nao inicializado.");
        if (lastToken != 0 && storedToken_ != static_cast<sqlite3_int64>(lastToken)) throw std::runtime_error("Estado temporario do CBT Linux fora de sincronia.");
        const auto previousFiles = loadTrackedFiles();
        const auto currentFiles = scanCurrentLinuxFiles(root_);
        std::vector<ChangedFile> changes;
        changes.reserve(currentFiles.size());
        for (const auto& [relPath, info] : currentFiles) {
            const auto prevIt = previousFiles.find(relPath);
            if (prevIt == previousFiles.end() || prevIt->second.size != info.size || prevIt->second.mtime != info.mtime) changes.push_back({relPath, false});
        }
        for (const auto& [relPath, _] : previousFiles) {
            if (currentFiles.find(relPath) == currentFiles.end()) changes.push_back({relPath, true});
        }
        const sqlite3_int64 nextToken = storedToken_ + 1;
        persistCurrentState(currentFiles, nextToken);
        newToken = static_cast<std::uint64_t>(nextToken);
        return changes;
    }
};
std::unique_ptr<ChangeTracker> makePlatformTracker() {
    return std::make_unique<LinuxStateTracker>();
}
}
#else
namespace {
class UnsupportedTracker : public ChangeTracker {
public:
    bool isAvailable() const override { return false; }
    const char* backendName() const override { return "unsupported"; }
    void startTracking(const fs::path&) override {}
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        newToken = lastToken;
        return {};
    }
};
std::unique_ptr<ChangeTracker> makePlatformTracker() {
    return std::make_unique<UnsupportedTracker>();
}
}
#endif

namespace {
class CompositeChangeTracker : public ChangeTracker {
    fs::path root_;
    std::unique_ptr<EventStore> daemonStore_;
    std::unique_ptr<ChangeTracker> platform_;
    bool daemonAvailable_ = false;
    std::string daemonError_;
    bool canUseDaemon(const DecodedToken& decoded, std::uint64_t lastToken) const {
        return daemonStore_ && (lastToken == 0 || decoded.backend == TokenBackend::Daemon);
    }
    void initDaemon() {
        daemonAvailable_ = false;
        daemonError_.clear();
        try {
            daemonStore_ = std::make_unique<EventStore>(defaultEventStorePath());
            daemonAvailable_ = daemonStore_->latestToken(root_).has_value();
        } catch (const std::exception& ex) {
            daemonStore_.reset();
            daemonError_ = ex.what();
        }
    }
    std::vector<ChangedFile> readFromDaemon(const DecodedToken& decoded, std::uint64_t lastToken, std::uint64_t& newToken) {
        std::uint64_t daemonToken = 0;
        auto changes = daemonStore_->loadChangesSince(root_, decoded.backend == TokenBackend::Daemon ? decoded.value : 0, daemonToken);
        if (daemonToken != 0 || lastToken == 0) {
            newToken = encodeToken(TokenBackend::Daemon, daemonToken);
            return changes;
        }
        newToken = encodeToken(TokenBackend::Daemon, decoded.value);
        return {};
    }
public:
    CompositeChangeTracker() : platform_(makePlatformTracker()) {}
    bool isAvailable() const override {
        return (platform_ && platform_->isAvailable()) || daemonAvailable_;
    }
    const char* backendName() const override {
        if (daemonAvailable_ && platform_ && platform_->isAvailable()) return "daemon+platform";
        if (daemonAvailable_) return "daemon_events";
        return platform_ ? platform_->backendName() : "unsupported";
    }
    void startTracking(const fs::path& rootPath) override {
        root_ = fs::absolute(rootPath).lexically_normal();
        initDaemon();
        if (platform_) platform_->startTracking(root_);
    }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        const auto decoded = decodeToken(lastToken);
        if (canUseDaemon(decoded, lastToken)) {
            try {
                return readFromDaemon(decoded, lastToken, newToken);
            } catch (const std::exception& ex) {
                daemonError_ = ex.what();
                if (!platform_ || !platform_->isAvailable()) throw;
            }
        }
        if (!platform_ || !platform_->isAvailable()) {
            if (!daemonError_.empty()) throw std::runtime_error("Nenhum backend CBT disponivel. Daemon: " + daemonError_);
            newToken = lastToken;
            return {};
        }
        std::uint64_t platformToken = 0;
        auto changes = platform_->getChanges(decoded.backend == TokenBackend::Native ? decoded.value : 0, platformToken);
        newToken = encodeToken(TokenBackend::Native, platformToken);
        return changes;
    }
};
}

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<CompositeChangeTracker>();
}
}

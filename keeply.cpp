#ifndef KEEPLY_CPP_INCLUDED
#define KEEPLY_CPP_INCLUDED

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sqlite3.h>

#ifdef _WIN32
#include <shlobj.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace keeply {

namespace fs = std::filesystem;

enum class KnownDirectory {
    Home,
    Desktop,
    Documents,
    Downloads,
    Pictures,
    Music,
    Videos,
    LocalData,
    StateData,
    CacheData,
    Temp
};

fs::path homeDirectoryPath();
std::optional<fs::path> knownDirectoryPath(KnownDirectory dir);

fs::path defaultKeeplyDataDir();
fs::path defaultKeeplyStateDir();
fs::path defaultKeeplyTempDir();
fs::path defaultCloudBundleExportRoot();
fs::path defaultSourceRootPath();
fs::path defaultArchivePath();
fs::path defaultRestoreRootPath();

std::vector<fs::path> defaultSystemExcludedRoots();
bool isFilesystemRootPath(const fs::path& path);

std::string pathToUtf8(const fs::path& path);
fs::path pathFromUtf8(const std::string& utf8Path);

FILE* fopenPath(const fs::path& path, const char* mode);
int sqlite3_open_path(const fs::path& path, sqlite3** db);
int sqlite3_open_v2_path(const fs::path& path, sqlite3** db, int flags, const char* vfs);

inline constexpr std::size_t CHUNK_SIZE = 4 * 1024 * 1024;
using ChunkHash = std::array<unsigned char, 32>;
using Blob = std::vector<unsigned char>;

struct ChunkHashHasher {
    std::size_t operator()(const ChunkHash& h) const noexcept {
        std::size_t out = 1469598103934665603ull;
        for (unsigned char b : h) {
            out ^= static_cast<std::size_t>(b);
            out *= 1099511628211ull;
        }
        return out;
    }
};

class ShardedChunkSet {
    static constexpr std::size_t NUM_SHARDS = 64;

    struct alignas(64) Shard {
        std::mutex mtx;
        std::unordered_set<ChunkHash, ChunkHashHasher> set;
    };

public:
    bool insertIfNew(const ChunkHash& hash) {
        const std::size_t hashVal = ChunkHashHasher{}(hash);
        const std::size_t shardIdx = hashVal & (NUM_SHARDS - 1);
        auto& shard = shards_[shardIdx];
        std::lock_guard<std::mutex> lock(shard.mtx);
        return shard.set.insert(hash).second;
    }

    void reserve(std::size_t totalCapacity) {
        const std::size_t capacityPerShard =
            (totalCapacity + NUM_SHARDS - 1) / NUM_SHARDS;
        for (auto& shard : shards_) {
            shard.set.reserve(capacityPerShard);
        }
    }

private:
    std::array<Shard, NUM_SHARDS> shards_;
};

// Utils
std::string trim(const std::string& s);
std::string nowIsoLocal();
long long fileTimeToUnixSeconds(const fs::file_time_type& ftp);
std::string hexOfBytes(const unsigned char* bytes, std::size_t n);
std::string normalizeRelPath(std::string s);
bool isSafeRelativePath(const std::string& p);
fs::path normalizeAbsolutePath(const fs::path& p);
bool sourceRootUsesSystemExclusionPolicy(const fs::path& sourceRoot);
bool isExcludedBySystemPolicy(const fs::path& sourceRoot, const fs::path& candidatePath);
void ensureDefaults();

// Compactacao / hash
class Compactador {
public:
    static std::string blake3Hex(const void* data, std::size_t len);

    static void zlibCompress(
        const unsigned char* data,
        std::size_t len,
        int level,
        std::vector<unsigned char>& out
    );

    static void zlibDecompress(
        const void* compData,
        std::size_t compLen,
        std::size_t rawSize,
        std::vector<unsigned char>& out
    );

    static void zstdCompress(
        const unsigned char* data,
        std::size_t len,
        int level,
        std::vector<unsigned char>& out
    );

    static void zstdDecompress(
        const void* compData,
        std::size_t compLen,
        std::size_t rawSize,
        std::vector<unsigned char>& out
    );
};

// SQLite RAII
class SqliteError : public std::runtime_error {
public:
    explicit SqliteError(const std::string& msg);
};

class Stmt {
public:
    Stmt(sqlite3* db, const std::string& sql);
    ~Stmt();

    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;
    Stmt(Stmt&&) = delete;
    Stmt& operator=(Stmt&&) = delete;

    sqlite3_stmt* get();

    void bindInt(int idx, int v);
    void bindInt64(int idx, sqlite3_int64 v);
    void bindText(int idx, const std::string& v);
    void bindBlob(int idx, const void* data, int len);
    void bindNull(int idx);

    bool stepRow();
    void stepDone();

private:
    sqlite3* db_{nullptr};
    sqlite3_stmt* stmt_{nullptr};
};

class DB {
public:
    explicit DB(const fs::path& p);
    ~DB();

    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;
    DB(DB&&) = delete;
    DB& operator=(DB&&) = delete;

    sqlite3* raw();
    void exec(const std::string& sql);
    sqlite3_int64 lastInsertId() const;
    int changes() const;

private:
    void initSchema();

    sqlite3* db_{nullptr};
};

// Modelos
struct FileInfo {
    sqlite3_int64 fileId{};
    sqlite3_int64 size{};
    sqlite3_int64 mtime{};
    Blob fileHash;
};

struct SnapshotRow {
    sqlite3_int64 id{};
    std::string createdAt;
    std::string sourceRoot;
    std::string label;
    sqlite3_int64 fileCount{};
};

struct ChangeEntry {
    std::string path;
    std::string status;
    sqlite3_int64 oldSize{};
    sqlite3_int64 newSize{};
    sqlite3_int64 oldMtime{};
    sqlite3_int64 newMtime{};
    Blob oldHash;
    Blob newHash;
};

struct StoredChunkRow {
    int chunkIndex{};
    std::size_t rawSize{};
    std::size_t compSize{};
    std::string algo;
    std::vector<unsigned char> blob;
};

struct RestorableFileRef {
    sqlite3_int64 fileId{};
    sqlite3_int64 size{};
    sqlite3_int64 mtime{};
};

struct BackupStats {
    std::size_t scanned = 0;
    std::size_t added = 0;
    std::size_t reused = 0;
    std::size_t chunks = 0;
    std::size_t uniqueChunksInserted = 0;
    std::uintmax_t bytesRead = 0;
    std::size_t warnings = 0;
};

struct BackupProgress {
    BackupStats stats;
    std::size_t filesQueued = 0;
    std::size_t filesCompleted = 0;
    bool discoveryComplete = false;
    std::string phase = "idle";
    std::string currentFile;
};

// Storage
class StorageArchive {
public:
    struct CloudBundleFile {
        fs::path path;
        std::string uploadName;
        std::string objectKey;
        std::string contentType = "application/octet-stream";
        std::uintmax_t size = 0;
        bool manifest = false;
        bool blobPart = false;
    };

    struct CloudBundleExport {
        std::string bundleId;
        fs::path rootDir;
        fs::path packPath;
        std::uint64_t blobMaxBytes = 0;
        std::size_t blobPartCount = 0;
        std::vector<CloudBundleFile> files;
    };

    explicit StorageArchive(const fs::path& path);
    ~StorageArchive();

    void begin();
    void beginRead();
    void commit();
    void endRead();
    void rollback();

    sqlite3_int64 createSnapshot(const std::string& sourceRoot, const std::string& label);

    std::optional<sqlite3_int64> latestSnapshotId();
    std::optional<std::uint64_t> latestSnapshotCbtToken();
    std::optional<sqlite3_int64> previousSnapshotId();
    sqlite3_int64 resolveSnapshotId(const std::string& userInput);

    std::map<std::string, FileInfo> loadSnapshotFileMap(sqlite3_int64 snapshotId);
    std::map<std::string, FileInfo> loadLatestSnapshotFileMap();

    sqlite3_int64 insertFilePlaceholder(sqlite3_int64 snapshotId,
                                        const std::string& relPath,
                                        sqlite3_int64 size,
                                        sqlite3_int64 mtime);

    void updateFileHash(sqlite3_int64 fileId, const Blob& fileHash);
    void deleteFileRecord(sqlite3_int64 fileId);

    sqlite3_int64 cloneFileFromPrevious(sqlite3_int64 snapshotId,
                                        const std::string& relPath,
                                        const FileInfo& prev);

    bool insertChunkIfMissing(const ChunkHash& sha,
                              std::size_t rawSize,
                              const std::vector<unsigned char>& comp,
                              const std::string& compAlgo);
    bool hasChunk(const ChunkHash& sha);

    struct PendingFileChunk {
        int chunkIdx{};
        ChunkHash chunkHash{};
        std::size_t rawSize{};
    };

    void addFileChunk(sqlite3_int64 fileId,
                      int chunkIdx,
                      const ChunkHash& chunkSha,
                      std::size_t rawSize);
    void addFileChunksBulk(sqlite3_int64 fileId, const std::vector<PendingFileChunk>& rows);

    std::vector<SnapshotRow> listSnapshots();
    std::vector<ChangeEntry> diffSnapshots(sqlite3_int64 olderSnapshotId, sqlite3_int64 newerSnapshotId);
    std::vector<ChangeEntry> diffLatestVsPrevious();

    std::optional<RestorableFileRef> findFileBySnapshotAndPath(sqlite3_int64 snapshotId,
                                                               const std::string& relPath);

    std::vector<StoredChunkRow> loadFileChunks(sqlite3_int64 fileId);
    std::vector<std::string> listSnapshotPaths(sqlite3_int64 snapshotId);
    void updateSnapshotCbtToken(sqlite3_int64 snapshotId, std::uint64_t token);
    CloudBundleExport exportCloudBundle(const fs::path& tempRoot,
                                        std::uint64_t blobMaxBytes = 16ull * 1024ull * 1024ull) const;
    CloudBundleFile materializeCloudBundleBlob(const CloudBundleExport& bundle,
                                               std::size_t partIndex) const;

private:
    struct HotStmts {
        sqlite3_stmt* insertSnapshot = nullptr;
        sqlite3_stmt* updateSnapshotCbtToken = nullptr;
        sqlite3_stmt* insertFilePlaceholder = nullptr;
        sqlite3_stmt* cloneFileInsert = nullptr;
        sqlite3_stmt* cloneFileChunks = nullptr;
        sqlite3_stmt* insertChunkIfMissing = nullptr;
        sqlite3_stmt* updateChunkOffset = nullptr;
        sqlite3_stmt* addFileChunk = nullptr;
    };
    fs::path path_;
    DB db_;
    HotStmts hot_;
    std::shared_ptr<void> backendOpaque_;
    mutable std::unique_ptr<std::ifstream> packIn_;
    bool readTxnActive_ = false;
    bool writeTxnActive_ = false;
    void prepareHotStatements();
    void finalizeHotStatements();
    std::vector<unsigned char> readPackAt(sqlite3_int64 recordOffset,
                                          const ChunkHash& expectedSha,
                                          std::size_t expectedRawSize,
                                          std::size_t expectedCompSize,
                                          const std::string& expectedAlgo) const;
};

// Engines
class ScanEngine {
public:
    static BackupStats backupFolderToKply(const fs::path& sourceRoot,
                                          const fs::path& archivePath,
                                          const std::string& label,
                                          const std::function<void(const BackupProgress&)>& progressCallback = {});
    static std::vector<std::string> listAvailableSourceRoots();
};

class RestoreEngine {
public:
    static void restoreFile(const fs::path& archivePath,
                            sqlite3_int64 snapshotId,
                            const std::string& relPath,
                            const fs::path& outRoot);

    static void restoreSnapshot(const fs::path& archivePath,
                                sqlite3_int64 snapshotId,
                                const fs::path& outRoot);
};

struct VerifyResult {
    bool ok = true;
    std::vector<std::string> errors;
    std::size_t chunksChecked = 0;
    std::size_t chunksOk = 0;
};

struct ScanScopeState {
    std::string id = "home";
    std::string label = "Home";
    std::string requestedPath;
    std::string resolvedPath = pathToUtf8(defaultSourceRootPath());
};

class VerifyEngine {
public:
    static VerifyResult verifyArchive(const fs::path& archivePath, bool verbose = false);
};

// API
struct AppState {
    std::string source = pathToUtf8(defaultSourceRootPath());
    std::string archive = pathToUtf8(defaultArchivePath());
    std::string restoreRoot = pathToUtf8(defaultRestoreRootPath());
    ScanScopeState scanScope{};
    bool archiveSplitEnabled = false;
    std::uint64_t archiveSplitMaxBytes = 0; // 0 = desabilitado (placeholder para futura divisao)
};

class KeeplyApi {
public:
    KeeplyApi();

    const AppState& state() const;
    void setSource(const std::string& source);
    void setScanScope(const std::string& scopeId);
    void setArchive(const std::string& archive);
    void setRestoreRoot(const std::string& restoreRoot);
    void setArchiveSplitMaxBytes(std::uint64_t maxBytes); // TODO: ligar no storage write path
    void disableArchiveSplit();
    bool archiveExists() const;

    BackupStats runBackup(const std::string& label,
                          const std::function<void(const BackupProgress&)>& progressCallback = {});
    std::vector<SnapshotRow> listSnapshots();
    std::vector<ChangeEntry> diffSnapshots(const std::string& olderSnapshotInput,
                                           const std::string& newerSnapshotInput);
    std::vector<ChangeEntry> diffLatestVsPrevious();
    std::vector<std::string> listSnapshotPaths(const std::string& snapshotInput);

    void restoreFile(const std::string& snapshotInput,
                     const std::string& relPath,
                     const std::optional<fs::path>& outRootOpt);

    void restoreSnapshot(const std::string& snapshotInput,
                         const std::optional<fs::path>& outRootOpt);

private:
    AppState state_;
};

namespace detail {

inline std::string trimAscii(std::string value) {
    const auto begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) return {};
    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

inline std::optional<std::string> envValue(const char* key) {
    const char* value = std::getenv(key);
    if (!value || !*value) return std::nullopt;
    return std::string(value);
}

inline fs::path normalizedOrEmpty(const fs::path& path) {
    if (path.empty()) return {};
    std::error_code ec;
    const fs::path absolute = path.is_absolute() ? path : fs::absolute(path, ec);
    if (ec) return path.lexically_normal();
    return absolute.lexically_normal();
}

inline void appendUnique(std::vector<fs::path>& out, const fs::path& path) {
    if (path.empty()) return;
    const fs::path normalized = normalizedOrEmpty(path);
    for (const auto& current : out) {
        if (normalized == current) return;
    }
    out.push_back(normalized);
}

#ifdef _WIN32
inline std::optional<fs::path> knownFolderPath(REFKNOWNFOLDERID folderId) {
    PWSTR raw = nullptr;
    const HRESULT hr = SHGetKnownFolderPath(folderId, KF_FLAG_DEFAULT, nullptr, &raw);
    if (FAILED(hr) || !raw) return std::nullopt;
    const fs::path path(raw);
    CoTaskMemFree(raw);
    return normalizedOrEmpty(path);
}

inline std::wstring widenAscii(const char* text) {
    std::wstring out;
    if (!text) return out;
    while (*text) out.push_back(static_cast<wchar_t>(*text++));
    return out;
}
#endif

#endif // KEEPLY_CPP_INCLUDED

#if !defined(_WIN32)
inline std::optional<std::string> readXdgUserDir(const fs::path& homeDir, const std::string& key) {
    const fs::path configPath = homeDir / ".config" / "user-dirs.dirs";
    std::ifstream input(configPath);
    if (!input) return std::nullopt;

    const std::string prefix = key + "=";
    std::string line;
    while (std::getline(input, line)) {
        line = trimAscii(line);
        if (line.empty() || line.front() == '#') continue;
        if (line.rfind(prefix, 0) != 0) continue;

        std::string value = trimAscii(line.substr(prefix.size()));
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        const std::string homeToken = "$HOME";
        const auto homePos = value.find(homeToken);
        if (homePos != std::string::npos) {
            value.replace(homePos, homeToken.size(), homeDir.string());
        }
        return value;
    }

    return std::nullopt;
}
#endif

inline std::optional<fs::path> homeFromEnvironment() {
#ifdef _WIN32
    if (const auto fromProfile = envValue("USERPROFILE")) return normalizedOrEmpty(pathFromUtf8(*fromProfile));
    const auto drive = envValue("HOMEDRIVE");
    const auto homePath = envValue("HOMEPATH");
    if (drive && homePath) return normalizedOrEmpty(pathFromUtf8(*drive + *homePath));
#else
    if (const auto fromHome = envValue("HOME")) return normalizedOrEmpty(pathFromUtf8(*fromHome));
#endif
    return std::nullopt;
}

inline fs::path fallbackCurrentPath() {
    std::error_code ec;
    const fs::path cwd = fs::current_path(ec);
    return ec ? fs::path(".") : cwd.lexically_normal();
}

} // namespace detail

inline std::string pathToUtf8(const fs::path& path) {
    return path.u8string();
}

inline fs::path pathFromUtf8(const std::string& utf8Path) {
    return fs::u8path(utf8Path);
}

inline std::optional<fs::path> knownDirectoryPath(KnownDirectory dir) {
#ifdef _WIN32
    switch (dir) {
        case KnownDirectory::Home:
            return detail::knownFolderPath(FOLDERID_Profile);
        case KnownDirectory::Desktop:
            return detail::knownFolderPath(FOLDERID_Desktop);
        case KnownDirectory::Documents:
            return detail::knownFolderPath(FOLDERID_Documents);
        case KnownDirectory::Downloads:
            return detail::knownFolderPath(FOLDERID_Downloads);
        case KnownDirectory::Pictures:
            return detail::knownFolderPath(FOLDERID_Pictures);
        case KnownDirectory::Music:
            return detail::knownFolderPath(FOLDERID_Music);
        case KnownDirectory::Videos:
            return detail::knownFolderPath(FOLDERID_Videos);
        case KnownDirectory::LocalData:
        case KnownDirectory::StateData:
        case KnownDirectory::CacheData:
            return detail::knownFolderPath(FOLDERID_LocalAppData);
        case KnownDirectory::Temp:
            if (const auto temp = detail::envValue("TEMP")) return detail::normalizedOrEmpty(pathFromUtf8(*temp));
            if (const auto tmp = detail::envValue("TMP")) return detail::normalizedOrEmpty(pathFromUtf8(*tmp));
            return std::nullopt;
    }
    return std::nullopt;
#else
    const fs::path home = homeDirectoryPath();
    switch (dir) {
        case KnownDirectory::Home:
            if (const auto value = detail::homeFromEnvironment()) return *value;
            return detail::fallbackCurrentPath();
        case KnownDirectory::Desktop:
            if (const auto value = detail::readXdgUserDir(home, "XDG_DESKTOP_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Desktop");
        case KnownDirectory::Documents:
            if (const auto value = detail::readXdgUserDir(home, "XDG_DOCUMENTS_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Documents");
        case KnownDirectory::Downloads:
            if (const auto value = detail::readXdgUserDir(home, "XDG_DOWNLOAD_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Downloads");
        case KnownDirectory::Pictures:
            if (const auto value = detail::readXdgUserDir(home, "XDG_PICTURES_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Pictures");
        case KnownDirectory::Music:
            if (const auto value = detail::readXdgUserDir(home, "XDG_MUSIC_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Music");
        case KnownDirectory::Videos:
            if (const auto value = detail::readXdgUserDir(home, "XDG_VIDEOS_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Videos");
        case KnownDirectory::LocalData:
#if defined(__APPLE__)
            return detail::normalizedOrEmpty(home / "Library" / "Application Support");
#else
            if (const auto value = detail::envValue("XDG_DATA_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / ".local" / "share");
#endif
        case KnownDirectory::StateData:
#if defined(__APPLE__)
            return detail::normalizedOrEmpty(home / "Library" / "Application Support");
#else
            if (const auto value = detail::envValue("XDG_STATE_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / ".local" / "state");
#endif
        case KnownDirectory::CacheData:
#if defined(__APPLE__)
            return detail::normalizedOrEmpty(home / "Library" / "Caches");
#else
            if (const auto value = detail::envValue("XDG_CACHE_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / ".cache");
#endif
        case KnownDirectory::Temp:
            if (const auto value = detail::envValue("TMPDIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return std::nullopt;
    }
    return std::nullopt;
#endif
}

inline fs::path homeDirectoryPath() {
#ifdef _WIN32
    if (const auto home = knownDirectoryPath(KnownDirectory::Home)) return *home;
#endif
    if (const auto home = detail::homeFromEnvironment()) return *home;
    return detail::fallbackCurrentPath();
}

inline fs::path defaultKeeplyDataDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::LocalData)) {
#if defined(__APPLE__)
        return *base / "Keeply";
#else
        return *base / "keeply";
#endif
    }
    return detail::fallbackCurrentPath() / ".keeply";
}

inline fs::path defaultKeeplyStateDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::StateData)) {
#if defined(__APPLE__)
        return *base / "Keeply" / "state";
#else
        return *base / "keeply";
#endif
    }
    return defaultKeeplyDataDir();
}

inline fs::path defaultKeeplyTempDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::Temp)) {
        return *base / "keeply";
    }
    std::error_code ec;
    const fs::path temp = fs::temp_directory_path(ec);
    if (!ec) return temp / "keeply";
    return defaultKeeplyDataDir() / "tmp";
}

inline fs::path defaultCloudBundleExportRoot() {
    return defaultKeeplyTempDir() / "cloud_bundle_export";
}

inline fs::path defaultSourceRootPath() {
    return homeDirectoryPath();
}

inline fs::path defaultArchivePath() {
    return defaultKeeplyDataDir() / "keeply.kipy";
}

inline fs::path defaultRestoreRootPath() {
    return defaultKeeplyDataDir() / "restore";
}

inline std::vector<fs::path> defaultSystemExcludedRoots() {
    std::vector<fs::path> out;
#ifdef _WIN32
    if (const auto systemRoot = detail::envValue("SystemRoot")) detail::appendUnique(out, pathFromUtf8(*systemRoot));
    if (const auto windir = detail::envValue("WINDIR")) detail::appendUnique(out, pathFromUtf8(*windir));
    if (const auto programFiles = detail::envValue("ProgramFiles")) detail::appendUnique(out, pathFromUtf8(*programFiles));
    if (const auto programFilesX86 = detail::envValue("ProgramFiles(x86)")) detail::appendUnique(out, pathFromUtf8(*programFilesX86));
    if (const auto programData = detail::envValue("ProgramData")) detail::appendUnique(out, pathFromUtf8(*programData));
    detail::appendUnique(out, defaultKeeplyTempDir());
    if (const auto home = homeDirectoryPath(); !home.root_path().empty()) {
        detail::appendUnique(out, home.root_path() / "$Recycle.Bin");
        detail::appendUnique(out, home.root_path() / "System Volume Information");
    }
#else
    detail::appendUnique(out, "/proc");
    detail::appendUnique(out, "/sys");
    detail::appendUnique(out, "/dev");
    detail::appendUnique(out, "/run");
    detail::appendUnique(out, "/tmp");
    detail::appendUnique(out, "/mnt");
    detail::appendUnique(out, "/media");
    detail::appendUnique(out, "/lost+found");
    detail::appendUnique(out, "/var/run");
    detail::appendUnique(out, "/var/tmp");
#endif
    return out;
}

inline bool isFilesystemRootPath(const fs::path& path) {
    if (path.empty()) return false;
    const fs::path normalized = detail::normalizedOrEmpty(path);
    const fs::path root = normalized.root_path();
    return !root.empty() && normalized == root;
}

inline FILE* fopenPath(const fs::path& path, const char* mode) {
#ifdef _WIN32
    const std::wstring wideMode = detail::widenAscii(mode);
    return _wfopen(path.wstring().c_str(), wideMode.c_str());
#else
    return std::fopen(path.c_str(), mode);
#endif
}

inline int sqlite3_open_path(const fs::path& path, sqlite3** db) {
    return sqlite3_open(pathToUtf8(path).c_str(), db);
}

inline int sqlite3_open_v2_path(const fs::path& path, sqlite3** db, int flags, const char* vfs) {
    return sqlite3_open_v2(pathToUtf8(path).c_str(), db, flags, vfs);
}

} // namespace keeply

#ifdef KEEPLY_CLI_IMPLEMENTATION

namespace {

using keeply::KeeplyApi;
using keeply::SnapshotRow;

constexpr std::size_t kFilePageSize = 25;

struct BrowserEntry {
    bool isDir{};
    std::string name;
    std::string relPath;
};

int cmdRestoreSnapshot(KeeplyApi& api, const std::string& snapshot);
int cmdRestoreFile(KeeplyApi& api, const std::string& relPath, const std::string& snapshot);

KeeplyApi makeCliApi() {
    KeeplyApi api;
    api.setScanScope("home");
    api.setArchive(keeply::pathToUtf8(keeply::defaultArchivePath()));
    api.setRestoreRoot(keeply::pathToUtf8(keeply::defaultRestoreRootPath()));
    return api;
}

std::string readLine(const std::string& prompt) {
    std::cout << prompt;
    std::cout.flush();
    std::string s;
    std::getline(std::cin, s);
    return keeply::trim(s);
}

std::optional<std::size_t> parseIndex1(const std::string& s, std::size_t max) {
    if (s.empty()) return std::nullopt;
    try {
        const long long v = std::stoll(s);
        if (v < 1) return std::nullopt;
        const std::size_t idx = static_cast<std::size_t>(v);
        if (idx > max) return std::nullopt;
        return idx - 1;
    } catch (...) {
        return std::nullopt;
    }
}

std::string snapshotTypeLabel(std::size_t idx, std::size_t total) {
    if (total == 0) return "-";
    if (total == 1) return "Completo (atual)";
    if (idx == 0) return "Completo (base)";
    if (idx + 1 == total) return "Incremental (atual)";
    return "Incremental";
}

std::vector<SnapshotRow> loadTimelineSnapshots(KeeplyApi& api) {
    auto snapshots = api.listSnapshots();
    std::reverse(snapshots.begin(), snapshots.end());
    return snapshots;
}

void printConfig(const KeeplyApi& api) {
    const auto& st = api.state();
    std::cout << "Origem (scan): " << st.source << "\n";
    std::cout << "Arquivo backup: " << st.archive << "  [DB com chunks compactados]\n";
    std::cout << "Restore root  : " << st.restoreRoot << "\n";
    std::cout << "Split archive : " << (st.archiveSplitEnabled ? "ON" : "OFF");
    if (st.archiveSplitEnabled) {
        std::cout << " (maxBytes=" << st.archiveSplitMaxBytes << ")";
    }
    std::cout << "\n";
}

void printUsage(const char* argv0) {
    std::cout
        << "Keeply CLI (basico)\n\n"
        << "Uso:\n"
        << "  " << argv0 << "                 # menu interativo\n"
        << "  " << argv0 << " menu\n"
        << "  " << argv0 << " config\n"
        << "  " << argv0 << " backup\n"
        << "  " << argv0 << " backup-source <diretorio> [arquivo_backup]\n"
        << "  " << argv0 << " timeline\n"
        << "  " << argv0 << " reset [--force]\n"
        << "  " << argv0 << " restore-ui\n"
        << "  " << argv0 << " restore [snapshot]\n"
        << "  " << argv0 << " files [snapshot]\n"
        << "  " << argv0 << " restore-file <arquivo_relativo> [snapshot]\n\n"
        << "Defaults:\n"
        << "  - Scan origem: pasta Home do usuario\n"
        << "  - Backup DB   : " << keeply::pathToUtf8(keeply::defaultArchivePath()) << "\n"
        << "  - Restore root: " << keeply::pathToUtf8(keeply::defaultRestoreRootPath()) << "\n";
}

int cmdConfig(KeeplyApi& api) {
    printConfig(api);
    return 0;
}

int cmdBackup(KeeplyApi& api) {
    printConfig(api);
    std::cout << "Executando backup da origem '" << api.state().source << "' (pode demorar)...\n";
    const keeply::BackupStats stats = api.runBackup("");
    std::cout
        << "Backup concluido"
        << " | scanned=" << stats.scanned
        << " added=" << stats.added
        << " reused=" << stats.reused
        << " chunks=" << stats.chunks
        << " uniq_chunks=" << stats.uniqueChunksInserted
        << " bytes=" << stats.bytesRead
        << " warnings=" << stats.warnings
        << "\n";
    return 0;
}

int cmdBackupSource(KeeplyApi& api, const std::string& source, const std::optional<std::string>& archivePath) {
    api.setSource(source);
    if (archivePath && !archivePath->empty()) api.setArchive(*archivePath);
    return cmdBackup(api);
}

int cmdTimeline(KeeplyApi& api) {
    const auto snapshots = loadTimelineSnapshots(api);
    if (snapshots.empty()) {
        std::cout << "Historico vazio.\n";
        return 0;
    }
    std::cout << "Historico de backups (" << snapshots.size() << ")\n";
    std::cout << std::left
              << std::setw(5)  << "N"
              << std::setw(6)  << "ID"
              << std::setw(22) << "Criado em"
              << std::setw(22) << "Tipo"
              << std::setw(10) << "Arquivos"
              << "Origem\n";
    for (std::size_t i = 0; i < snapshots.size(); ++i) {
        const auto& s = snapshots[i];
        std::cout << std::left
                  << std::setw(5)  << (i + 1)
                  << std::setw(6)  << s.id
                  << std::setw(22) << s.createdAt
                  << std::setw(22) << snapshotTypeLabel(i, snapshots.size())
                  << std::setw(10) << s.fileCount
                  << s.sourceRoot
                  << "\n";
    }
    return 0;
}

int cmdReset(KeeplyApi& api, bool force) {
    const auto& st = api.state();
    const std::string root = keeply::pathToUtf8(keeply::defaultKeeplyDataDir());
    if (!force) {
        std::cout << "ATENCAO: isso vai apagar TUDO em " << root << "\n";
        std::cout << "Inclui backup DB e restauracoes de teste.\n";
        const std::string confirm = readLine("Digite APAGAR para confirmar: ");
        if (confirm != "APAGAR") {
            std::cout << "Cancelado.\n";
            return 0;
        }
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    if (ec) {
        throw std::runtime_error("Falha ao apagar " + root + ": " + ec.message());
    }
    keeply::ensureDefaults();
    std::cout << "Reset concluido.\n";
    std::cout << "Backup DB   : " << st.archive << "\n";
    std::cout << "Restore root: " << st.restoreRoot << "\n";
    return 0;
}

std::optional<std::string> chooseSnapshotInteractive(KeeplyApi& api) {
    const auto snapshots = loadTimelineSnapshots(api);
    if (snapshots.empty()) {
        std::cout << "Nenhum snapshot disponivel.\n";
        return std::nullopt;
    }
    std::cout << "\nSnapshots (linha do tempo)\n";
    for (std::size_t i = 0; i < snapshots.size(); ++i) {
        const auto& s = snapshots[i];
        std::cout << "  [" << (i + 1) << "] "
                  << "#" << s.id
                  << " | " << s.createdAt
                  << " | " << snapshotTypeLabel(i, snapshots.size())
                  << " | arquivos=" << s.fileCount
                  << "\n";
    }
    for (;;) {
        const std::string in = readLine("Selecione o snapshot pelo numero (0 cancela): ");
        if (in == "0" || in == "q" || in == "Q") return std::nullopt;
        const auto idx = parseIndex1(in, snapshots.size());
        if (!idx) {
            std::cout << "Numero invalido.\n";
            continue;
        }
        return std::to_string(static_cast<long long>(snapshots[*idx].id));
    }
}

bool hasPathPrefix(const std::string& path, const std::string& prefix) {
    if (prefix.empty()) return true;
    return path.size() >= prefix.size() && path.compare(0, prefix.size(), prefix) == 0;
}

std::string parentPrefix(const std::string& prefix) {
    if (prefix.empty()) return {};
    std::string p = prefix;
    if (!p.empty() && p.back() == '/') p.pop_back();
    const auto pos = p.find_last_of('/');
    if (pos == std::string::npos) return {};
    return p.substr(0, pos + 1);
}

std::vector<BrowserEntry> listBrowserEntries(const std::vector<std::string>& paths, const std::string& prefix) {
    std::vector<BrowserEntry> dirs;
    std::vector<BrowserEntry> files;
    std::unordered_set<std::string> seenDirs;
    for (const auto& path : paths) {
        if (!hasPathPrefix(path, prefix)) continue;
        const std::string rest = path.substr(prefix.size());
        if (rest.empty()) continue;
        const auto slash = rest.find('/');
        if (slash == std::string::npos) {
            files.push_back(BrowserEntry{false, rest, path});
            continue;
        }
        const std::string dirName = rest.substr(0, slash);
        if (!seenDirs.insert(dirName).second) continue;
        dirs.push_back(BrowserEntry{true, dirName, prefix + dirName});
    }
    std::sort(dirs.begin(), dirs.end(), [](const BrowserEntry& a, const BrowserEntry& b) { return a.name < b.name; });
    std::sort(files.begin(), files.end(), [](const BrowserEntry& a, const BrowserEntry& b) { return a.name < b.name; });
    std::vector<BrowserEntry> out;
    out.reserve(dirs.size() + files.size());
    out.insert(out.end(), dirs.begin(), dirs.end());
    out.insert(out.end(), files.begin(), files.end());
    return out;
}

std::size_t countPathsInDir(const std::vector<std::string>& paths, const std::string& dirPrefix) {
    std::size_t n = 0;
    for (const auto& p : paths) {
        if (hasPathPrefix(p, dirPrefix)) ++n;
    }
    return n;
}

int restorePathsMatchingPrefix(KeeplyApi& api,
                               const std::vector<std::string>& paths,
                               const std::string& snapshot,
                               const std::string& dirPrefix) {
    std::size_t total = 0;
    for (const auto& p : paths) {
        if (hasPathPrefix(p, dirPrefix)) ++total;
    }
    if (total == 0) {
        std::cout << "Nenhum arquivo encontrado para restore.\n";
        return 0;
    }
    std::cout << "Restaurando " << total << " arquivo(s) de '" << (dirPrefix.empty() ? "/" : dirPrefix) << "'...\n";
    std::size_t done = 0;
    for (const auto& p : paths) {
        if (!hasPathPrefix(p, dirPrefix)) continue;
        api.restoreFile(snapshot, p, std::nullopt);
        ++done;
        if (done % 100 == 0 || done == total) {
            std::cout << "RESTORE " << done << "/" << total << " path=" << p << "\n";
        }
    }
    std::cout << "Restore concluido em: " << api.state().restoreRoot << "\n";
    return 0;
}

int cmdRestoreBrowse(KeeplyApi& api, const std::string& snapshot) {
    const auto paths = api.listSnapshotPaths(snapshot);
    if (paths.empty()) {
        std::cout << "Esse snapshot nao tem arquivos.\n";
        return 0;
    }
    std::string prefix;
    std::size_t page = 0;
    for (;;) {
        const auto entries = listBrowserEntries(paths, prefix);
        const std::size_t pages = std::max<std::size_t>(1, (entries.size() + kFilePageSize - 1) / kFilePageSize);
        if (page >= pages) page = pages - 1;
        const std::size_t start = page * kFilePageSize;
        const std::size_t end = std::min(entries.size(), start + kFilePageSize);
        std::cout << "\nNavegando snapshot #" << snapshot << " em /" << prefix
                  << " (itens=" << entries.size()
                  << ", arquivos_no_dir=" << countPathsInDir(paths, prefix)
                  << ", pagina " << (page + 1) << "/" << pages << ")\n";
        if (entries.empty()) {
            std::cout << "  (vazio)\n";
        } else {
            for (std::size_t i = start; i < end; ++i) {
                const auto& e = entries[i];
                std::cout << "  [" << (i + 1) << "] " << (e.isDir ? "[D] " : "[F] ") << e.name;
                if (e.isDir) {
                    const std::string dirPrefix = e.relPath + "/";
                    std::cout << " (" << countPathsInDir(paths, dirPrefix) << " arqs)";
                }
                std::cout << "\n";
            }
        }
        std::cout << "Comandos: numero=entrar/restaurar | a=restaurar pasta atual | u=subir | r=raiz | n/p=pagina | q=sair\n";
        const std::string in = readLine("> ");
        if (in == "q" || in == "Q" || in == "0") return 0;
        if (in == "n" || in == "N") {
            if (page + 1 < pages) ++page;
            continue;
        }
        if (in == "p" || in == "P") {
            if (page > 0) --page;
            continue;
        }
        if (in == "u" || in == "U") {
            prefix = parentPrefix(prefix);
            page = 0;
            continue;
        }
        if (in == "r" || in == "R") {
            prefix.clear();
            page = 0;
            continue;
        }
        if (in == "a" || in == "A") {
            if (prefix.empty()) {
                const std::string confirm = readLine("Restaurar snapshot completo? (digite RESTAURAR): ");
                if (confirm == "RESTAURAR") return cmdRestoreSnapshot(api, snapshot);
                std::cout << "Cancelado.\n";
                continue;
            }
            const std::string confirm = readLine("Restaurar pasta atual '" + prefix + "'? (s/N): ");
            if (confirm == "s" || confirm == "S") {
                return restorePathsMatchingPrefix(api, paths, snapshot, prefix);
            }
            std::cout << "Cancelado.\n";
            continue;
        }
        const auto idx = parseIndex1(in, entries.size());
        if (!idx) {
            std::cout << "Entrada invalida.\n";
            continue;
        }
        const auto& e = entries[*idx];
        if (e.isDir) {
            std::cout << "  [1] Entrar na pasta\n";
            std::cout << "  [2] Restaurar essa pasta (recursivo)\n";
            std::cout << "  [0] Voltar\n";
            const std::string op = readLine("Opcao: ");
            if (op == "1") {
                prefix = e.relPath + "/";
                page = 0;
                continue;
            }
            if (op == "2") {
                const std::string dirPrefix = e.relPath + "/";
                const std::string confirm = readLine("Restaurar '" + dirPrefix + "'? (s/N): ");
                if (confirm == "s" || confirm == "S") {
                    return restorePathsMatchingPrefix(api, paths, snapshot, dirPrefix);
                }
                std::cout << "Cancelado.\n";
                continue;
            }
            continue;
        }
        return cmdRestoreFile(api, e.relPath, snapshot);
    }
}

int cmdRestoreSnapshot(KeeplyApi& api, const std::string& snapshot) {
    api.restoreSnapshot(snapshot, std::nullopt);
    std::cout << "Snapshot restaurado em: " << api.state().restoreRoot << "\n";
    return 0;
}

int cmdFiles(KeeplyApi& api, const std::string& snapshot) {
    const auto paths = api.listSnapshotPaths(snapshot);
    std::cout << "Arquivos no snapshot '" << snapshot << "': " << paths.size() << "\n";
    for (std::size_t i = 0; i < paths.size(); ++i) {
        std::cout << "  [" << (i + 1) << "] " << paths[i] << "\n";
    }
    return 0;
}

int cmdRestoreFile(KeeplyApi& api, const std::string& relPath, const std::string& snapshot) {
    api.restoreFile(snapshot, relPath, std::nullopt);
    const auto target = std::filesystem::path(api.state().restoreRoot) / relPath;
    std::cout << "Arquivo restaurado em: " << target.string() << "\n";
    return 0;
}

int cmdRestoreUi(KeeplyApi& api) {
    const auto snapshot = chooseSnapshotInteractive(api);
    if (!snapshot) {
        std::cout << "Restore cancelado.\n";
        return 0;
    }
    for (;;) {
        std::cout << "\nRestore do snapshot #" << *snapshot << "\n";
        std::cout << "  [1] Restaurar snapshot completo\n";
        std::cout << "  [2] Navegar e restaurar (arquivo/pasta)\n";
        std::cout << "  [0] Cancelar\n";
        const std::string mode = readLine("Opcao: ");
        if (mode == "0") {
            std::cout << "Restore cancelado.\n";
            return 0;
        }
        if (mode == "1") {
            return cmdRestoreSnapshot(api, *snapshot);
        }
        if (mode == "2") {
            return cmdRestoreBrowse(api, *snapshot);
        }
        std::cout << "Opcao invalida.\n";
    }
}

int runMenu(KeeplyApi& api) {
    for (;;) {
        std::cout << "\n=== KEEPLY CLI (BASICO) ===\n";
        printConfig(api);
        std::cout << "\n";
        std::cout << "  [1] Executar backup (scan em " << api.state().source << ")\n";
        std::cout << "  [2] Ver historico (completo/incremental)\n";
        std::cout << "  [3] Restaurar (selecionar snapshot e arquivo)\n";
        std::cout << "  [4] Apagar tudo em " << keeply::pathToUtf8(keeply::defaultKeeplyDataDir()) << " (teste)\n";
        std::cout << "  [5] Listar arquivos do ultimo snapshot\n";
        std::cout << "  [0] Sair\n";
        const std::string op = readLine("Opcao: ");
        if (op == "0") return 0;
        if (op == "1") {
            try { cmdBackup(api); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "2") {
            try { cmdTimeline(api); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "3") {
            try { cmdRestoreUi(api); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "4") {
            try { cmdReset(api, false); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        if (op == "5") {
            try { cmdFiles(api, "latest"); } catch (const std::exception& e) { std::cout << "Erro: " << e.what() << "\n"; }
            continue;
        }
        std::cout << "Opcao invalida.\n";
    }
}

} // namespace

int main(int argc, char* argv[]) {
    try {
        KeeplyApi api = makeCliApi();
        if (argc < 2) {
            return runMenu(api);
        }
        const std::string cmd = argv[1];
        if (cmd == "help" || cmd == "-h" || cmd == "--help") {
            printUsage(argv[0]);
            return 0;
        }
        if (cmd == "menu") return runMenu(api);
        if (cmd == "config") return cmdConfig(api);
        if (cmd == "backup") return cmdBackup(api);
        if (cmd == "backup-source") {
            if (argc < 3) throw std::runtime_error("Uso: backup-source <diretorio> [arquivo_backup]");
            const std::optional<std::string> archive = (argc >= 4) ? std::optional<std::string>(argv[3]) : std::nullopt;
            return cmdBackupSource(api, argv[2], archive);
        }
        if (cmd == "timeline" || cmd == "list") return cmdTimeline(api);
        if (cmd == "restore-ui") return cmdRestoreUi(api);
        if (cmd == "restore-browse") {
            const std::string snapshot = (argc >= 3) ? argv[2] : "latest";
            return cmdRestoreBrowse(api, snapshot);
        }
        if (cmd == "reset") {
            const bool force = (argc >= 3 && std::string(argv[2]) == "--force");
            return cmdReset(api, force);
        }
        if (cmd == "restore" || cmd == "restore-snapshot") {
            const std::string snapshot = (argc >= 3) ? argv[2] : "latest";
            return cmdRestoreSnapshot(api, snapshot);
        }
        if (cmd == "files" || cmd == "paths") {
            const std::string snapshot = (argc >= 3) ? argv[2] : "latest";
            return cmdFiles(api, snapshot);
        }
        if (cmd == "restore-file") {
            if (argc < 3) throw std::runtime_error("Uso: restore-file <arquivo_relativo> [snapshot]");
            const std::string relPath = argv[2];
            const std::string snapshot = (argc >= 4) ? argv[3] : "latest";
            return cmdRestoreFile(api, relPath, snapshot);
        }
        throw std::runtime_error("Comando invalido: " + cmd);
    } catch (const std::exception& e) {
        std::cerr << "Erro: " << e.what() << "\n";
        return 1;
    }
}

#endif

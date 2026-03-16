#pragma once

#include "Backup/utilitarios_backup.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include <sqlite3.h>

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

std::string nowIsoLocal();
long long fileTimeToUnixSeconds(const fs::file_time_type& ftp);
std::string hexOfBytes(const unsigned char* bytes, std::size_t n);
std::string normalizeRelPath(std::string s);
bool isSafeRelativePath(const std::string& p);
fs::path normalizeAbsolutePath(const fs::path& p);
bool sourceRootUsesSystemExclusionPolicy(const fs::path& sourceRoot);
bool isExcludedBySystemPolicy(const fs::path& sourceRoot, const fs::path& candidatePath);
void ensureDefaults();

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
    void reset();

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
        std::unique_ptr<Stmt> insertSnapshot;
        std::unique_ptr<Stmt> updateSnapshotCbtToken;
        std::unique_ptr<Stmt> insertFilePlaceholder;
        std::unique_ptr<Stmt> cloneFileInsert;
        std::unique_ptr<Stmt> cloneFileChunks;
        std::unique_ptr<Stmt> insertChunkIfMissing;
        std::unique_ptr<Stmt> updateChunkOffset;
        std::unique_ptr<Stmt> addFileChunk;
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

struct AppState {
    std::string source = pathToUtf8(defaultSourceRootPath());
    std::string archive = pathToUtf8(defaultArchivePath());
    std::string restoreRoot = pathToUtf8(defaultRestoreRootPath());
    ScanScopeState scanScope{};
    bool archiveSplitEnabled = false;
    std::uint64_t archiveSplitMaxBytes = 0;
};

class KeeplyApi {
public:
    KeeplyApi();

    const AppState& state() const;
    void setSource(const std::string& source);
    void setScanScope(const std::string& scopeId);
    void setArchive(const std::string& archive);
    void setRestoreRoot(const std::string& restoreRoot);
    void setArchiveSplitMaxBytes(std::uint64_t maxBytes);
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

} // namespace keeply

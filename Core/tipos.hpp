#pragma once
#include "utilitarios.hpp"
#include "sqlite_util.hpp"  // Lightweight ad-hoc helpers complementing DB/Stmt below.
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
    Temp};
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
inline constexpr std::size_t CHUNK_SIZE = 8 * 1024 * 1024;
using ChunkHash = std::array<unsigned char, 32>;
using Blob = std::vector<unsigned char>;
struct ChunkHashHasher {
    std::size_t operator()(const ChunkHash& h) const noexcept {
        std::size_t out = 1469598103934665603ull;
        for (unsigned char b : h) {
            out ^= static_cast<std::size_t>(b);
            out *= 1099511628211ull;}
        return out;}
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
        return shard.set.insert(hash).second;}
    void reserve(std::size_t totalCapacity) {
        const std::size_t capacityPerShard =
            (totalCapacity + NUM_SHARDS - 1) / NUM_SHARDS;
        for (auto& shard : shards_) {
            shard.set.reserve(capacityPerShard);}}
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
    static ChunkHash blake3Hash(const void* data, std::size_t len);
    static void zlibCompress(
        const unsigned char* data,
        std::size_t len,
        int level,
        std::vector<unsigned char>& out);
    static void zlibDecompress(
        const void* compData,
        std::size_t compLen,
        std::size_t rawSize,
        std::vector<unsigned char>& out);
    static void zstdCompress(
        const unsigned char* data,
        std::size_t len,
        int level,
        std::vector<unsigned char>& out);
    static void zstdDecompress(
        const void* compData,
        std::size_t compLen,
        std::size_t rawSize,
        std::vector<unsigned char>& out);
    static Blob aesGcmEncrypt(
        const std::array<unsigned char, 32>& key,
        const std::array<unsigned char, 12>& iv,
        const unsigned char* data, std::size_t len);
    static bool aesGcmDecrypt(
        const std::array<unsigned char, 32>& key,
        const std::array<unsigned char, 12>& iv,
        const unsigned char* data, std::size_t len,
        Blob& plainOut);
    static std::array<unsigned char, 12> generateIv();
    static bool deriveKeyFromEnv(std::array<unsigned char, 32>& keyOut);};
class SqliteError : public std::runtime_error {
public:
    explicit SqliteError(const std::string& msg);};
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
    sqlite3_stmt* stmt_{nullptr};};
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
    sqlite3* db_{nullptr};};
struct FileInfo {
    sqlite3_int64 fileId{};
    sqlite3_int64 size{};
    sqlite3_int64 mtime{};
    Blob fileHash;};
struct SnapshotRow {
    sqlite3_int64 id{};
    std::string createdAt;
    std::string sourceRoot;
    std::string label;
    sqlite3_int64 fileCount{}};
struct ChangeEntry {
    std::string path;
    std::string status;
    sqlite3_int64 oldSize{};
    sqlite3_int64 newSize{};
    sqlite3_int64 oldMtime{};
    sqlite3_int64 newMtime{};
    Blob oldHash;
    Blob newHash;};
struct StoredChunkRow {
    int chunkIndex{};
    std::size_t rawSize{};
    std::size_t compSize{};
    std::string algo;
    std::vector<unsigned char> blob;
    Blob encryptIv;};
struct RestorableFileRef {
    sqlite3_int64 fileId{};
    sqlite3_int64 size{};
    sqlite3_int64 mtime{};
    Blob fileHash;};
struct BackupStats {
    std::size_t scanned = 0;
    std::size_t added = 0;
    std::size_t reused = 0;
    std::size_t chunks = 0;
    std::size_t uniqueChunksInserted = 0;
    std::uintmax_t bytesRead = 0;
    std::size_t warnings = 0;};
struct BackupProgress {
    BackupStats stats;
    std::size_t filesQueued = 0;
    std::size_t filesCompleted = 0;
    bool discoveryComplete = false;
    std::string phase = "idle";
    std::string currentFile;};}

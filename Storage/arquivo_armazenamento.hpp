#pragma once
#include "../Core/tipos.hpp"
namespace keeply {
class StorageArchive {
public:
    struct CloudChunkRef {
        ChunkHash hash{};
        std::uint64_t packOffset = 0;
        std::uint64_t recordSize = 0;
    };
    struct CloudUploadPlan {
        std::string bundleId;
        fs::path archiveDbPath;
        fs::path packPath;
        sqlite3_int64 latestSnapshotId = 0;
        std::vector<CloudChunkRef> chunks;
    };
    explicit StorageArchive(const fs::path& path);
    ~StorageArchive();
    void begin();
    void beginRead();
    void commit();
    void endRead();
    void rollback();
    sqlite3_int64 createSnapshot(const std::string& sourceRoot, const std::string& label, const std::string& backupType);
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
    void streamFileChunks(sqlite3_int64 fileId,
                          const std::function<void(const ChunkHash&, const Blob&)>& cb);
    std::vector<std::string> listSnapshotPaths(sqlite3_int64 snapshotId);
    void updateSnapshotCbtToken(sqlite3_int64 snapshotId, std::uint64_t token);
    void setChunkEncryptIv(const ChunkHash& hash, const Blob& iv);
    void saveFileSignature(sqlite3_int64 snapshotId, const std::string& path, const Blob& sigBlob);
    std::optional<Blob> loadFileSignature(const std::string& path);
    CloudUploadPlan prepareCloudUpload(const std::string& deviceExternalId);
    std::vector<unsigned char> readPackRecord(const CloudChunkRef& ref) const;
    std::vector<ChunkHash> snapshotChunkHashes(sqlite3_int64 snapshotId);
    void setChunkPackOffsetForRestore(const ChunkHash& hash, std::uint64_t newOffset);
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
                                          const std::string& expectedAlgo,
                                          const Blob& encryptIv = {}) const;
};}

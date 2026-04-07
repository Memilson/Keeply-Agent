#pragma once
#include "../Core/tipos.hpp"
namespace keeply {
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
    void setChunkEncryptIv(const ChunkHash& hash, const Blob& iv);
    void saveFileSignature(sqlite3_int64 snapshotId, const std::string& path, const Blob& sigBlob);
    std::optional<Blob> loadFileSignature(const std::string& path);
    void markPartUploaded(const std::string& bundleId, int partIndex,
                          const std::string& uploadId, const std::string& etag);
    std::vector<std::pair<int,std::string>> loadUploadedParts(const std::string& bundleId);
    void clearUploadParts(const std::string& bundleId);
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
                                          const std::string& expectedAlgo,
                                          const Blob& encryptIv = {}) const;
};}

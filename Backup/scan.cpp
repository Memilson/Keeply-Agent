#include "keeply.hpp"
#include "change_tracker.hpp"

#include <blake3.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <system_error>
#include <thread>
#include <unordered_set>
#include <variant>
#include <vector>

namespace keeply {

namespace fs = std::filesystem;

// =========================================================================
// ESTRUTURAS DE ALTA PERFORMANCE E CONCORRÊNCIA
// =========================================================================

class BloomFilter {
    std::vector<bool> bits;
    std::size_t size;
public:
    BloomFilter(std::size_t size_in_bits) : size(size_in_bits) {
        bits.resize(size, false);
    }

    void add(const ChunkHash& h) {
        std::size_t hash1 = 0, hash2 = 0;
        std::memcpy(&hash1, h.data(), sizeof(std::size_t));
        std::memcpy(&hash2, h.data() + sizeof(std::size_t), sizeof(std::size_t));
        bits[(hash1) % size] = true;
        bits[(hash2) % size] = true;
        bits[(hash1 + hash2) % size] = true;
    }

    bool possiblyContains(const ChunkHash& h) const {
        std::size_t hash1 = 0, hash2 = 0;
        std::memcpy(&hash1, h.data(), sizeof(std::size_t));
        std::memcpy(&hash2, h.data() + sizeof(std::size_t), sizeof(std::size_t));
        return bits[(hash1) % size] && bits[(hash2) % size] && bits[(hash1 + hash2) % size];
    }
};

template <typename T>
class ThreadSafeQueue {
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cond_;
    bool finished_ = false;
public:
    void push(T item) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            queue_.push(std::move(item));
        }
        cond_.notify_one();
    }
    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this]() { return !queue_.empty() || finished_; });
        if (queue_.empty() && finished_) return false;
        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }
    void finish() {
        { std::unique_lock<std::mutex> lock(mutex_); finished_ = true; }
        cond_.notify_all();
    }
};

// =========================================================================
// TAREFAS DO PIPELINE
// =========================================================================

struct FileProcessTask {
    fs::path fullPath;
    std::string relPath;
    sqlite3_int64 size;
    sqlite3_int64 mtime;
};

struct ChunkInsertDbTask {
    ChunkHash hash;
    std::size_t rawSize;
    std::vector<unsigned char> data;
    std::string compressionType;
};

struct FileCommitDbTask {
    std::string relPath;
    sqlite3_int64 size;
    sqlite3_int64 mtime;
    std::vector<StorageArchive::PendingFileChunk> chunks;
};

using DbOperation = std::variant<ChunkInsertDbTask, FileCommitDbTask>;

struct AtomicStats {
    std::atomic<std::size_t> scanned{0};
    std::atomic<std::size_t> added{0};
    std::atomic<std::size_t> reused{0};
    std::atomic<std::size_t> chunks{0};
    std::atomic<std::size_t> uniqueChunksInserted{0};
    std::atomic<std::size_t> bytesRead{0};
    std::atomic<std::size_t> warnings{0};

    BackupStats toBackupStats() const {
        BackupStats s;
        s.scanned = scanned.load();
        s.added = added.load();
        s.reused = reused.load();
        s.chunks = chunks.load();
        s.uniqueChunksInserted = uniqueChunksInserted.load();
        s.bytesRead = bytesRead.load();
        s.warnings = warnings.load();
        return s;
    }
};

static ChunkHash blake3_digest32(const unsigned char* data, std::size_t len) {
    ChunkHash out{};
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, out.data(), out.size());
    return out;
}

static std::string lowerAscii(std::string s) {
    for (char& c : s) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return s;
}

static bool shouldSkipDirByName(const std::string& lowerName) {
    static const std::unordered_set<std::string> kSkipDirNames = {
        ".cache", ".npm", "node_modules", ".m2", ".cargo", "target",
        ".venv", "venv", "__pycache__", ".idea", ".android",
        "build", "out", "obj", "bin", ".terraform", ".keeply"
    };
    if (kSkipDirNames.find(lowerName) != kSkipDirNames.end()) return true;
    if (lowerName.size() >= 6 && lowerName.substr(lowerName.size() - 6) == ".cache") return true;
    if (lowerName.find("-cache") != std::string::npos) return true;
    return false;
}

static bool pathContainsTrashSegment(const fs::path& relPath) {
    int state = 0;
    for (const auto& part : relPath) {
        const std::string seg = part.string();
        if (state == 0) state = (seg == ".local") ? 1 : 0;
        else if (state == 1) state = (seg == "share") ? 2 : 0;
        else {
            if (seg == "Trash") return true;
            state = 0;
        }
    }
    return false;
}

static bool shouldStoreRawByExtension(const fs::path& p) {
    static const std::unordered_set<std::string> kRawExt = {
        ".keeply", ".zip", ".7z", ".rar", ".gz", ".jpg", ".png", 
        ".mp3", ".mp4", ".mkv", ".pdf", ".iso", ".apk"
    };
    const std::string ext = lowerAscii(p.extension().string());
    return kRawExt.find(ext) != kRawExt.end();
}

static fs::path normalizeAbsBestEffort(const fs::path& p) {
    std::error_code ec;
    fs::path abs = p.is_absolute() ? p : fs::absolute(p, ec);
    if (ec) abs = p;
    return abs.lexically_normal();
}

static std::unordered_set<std::string> buildSelfBackupExclusions(const fs::path& archiveAbs) {
    std::unordered_set<std::string> out;
    const std::string base = normalizeAbsBestEffort(archiveAbs).string();
    out.insert(base);
    out.insert(base + ".keeply");
    out.insert(base + ".chunks.pack");
    out.insert(base + "-wal");
    out.insert(base + "-shm");
    out.insert(base + "-journal");
    return out;
}

static bool parseBoolConfigValue(const std::string& rawValue, bool defaultValue) {
    const std::string value = lowerAscii(trim(rawValue));
    if (value.empty()) return defaultValue;
    if (value == "1" || value == "true" || value == "on" || value == "yes") return true;
    if (value == "0" || value == "false" || value == "off" || value == "no") return false;
    return defaultValue;
}

static bool readArchiveBoolConfig(const fs::path& archivePath, const char* key, bool defaultValue) {
    sqlite3* rawDb = nullptr;
    const int openRc = sqlite3_open_v2(
        archivePath.string().c_str(),
        &rawDb,
        SQLITE_OPEN_READONLY,
        nullptr
    );
    if (openRc != SQLITE_OK) {
        if (rawDb) sqlite3_close(rawDb);
        return defaultValue;
    }

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "SELECT v FROM meta WHERE k = ? LIMIT 1;";
    if (sqlite3_prepare_v2(rawDb, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        sqlite3_close(rawDb);
        return defaultValue;
    }

    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    bool out = defaultValue;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const unsigned char* text = sqlite3_column_text(stmt, 0);
        if (text) out = parseBoolConfigValue(reinterpret_cast<const char*>(text), defaultValue);
    }

    sqlite3_finalize(stmt);
    sqlite3_close(rawDb);
    return out;
}

static bool shouldSkipRelativeFileByPolicy(const std::string& relPathStr) {
    const fs::path relPath(relPathStr);
    if (pathContainsTrashSegment(relPath)) return true;
    for (const auto& part : relPath.parent_path()) {
        if (shouldSkipDirByName(lowerAscii(part.string()))) return true;
    }
    return false;
}

static void queueFileForBackup(
    const fs::path& fullPath,
    const fs::path& sourceRoot,
    const std::unordered_set<std::string>& selfExclusions,
    std::unordered_set<std::string>& seenPaths,
    const std::map<std::string, FileInfo>& prevMap,
    StorageArchive& arc,
    sqlite3_int64 snapshotId,
    ThreadSafeQueue<FileProcessTask>& jobQueue,
    AtomicStats& stats)
{
    std::error_code qec;
    if (!fs::is_regular_file(fullPath, qec)) {
        stats.warnings++;
        return;
    }

    const fs::path fullAbs = normalizeAbsBestEffort(fullPath);
    if (selfExclusions.find(fullAbs.string()) != selfExclusions.end()) return;

    stats.scanned++;
    const std::string rel = normalizeRelPath(fs::relative(fullPath, sourceRoot, qec).string());
    if (qec || !isSafeRelativePath(rel) || shouldSkipRelativeFileByPolicy(rel) || !seenPaths.insert(rel).second) {
        stats.warnings++;
        return;
    }

    const auto size1 = fs::file_size(fullPath, qec);
    const auto mtimeRaw1 = fs::last_write_time(fullPath, qec);
    if (qec) {
        stats.warnings++;
        return;
    }
    const auto mtime1 = fileTimeToUnixSeconds(mtimeRaw1);

    auto prevIt = prevMap.find(rel);
    if (prevIt != prevMap.end() &&
        prevIt->second.size == static_cast<sqlite3_int64>(size1) &&
        prevIt->second.mtime == static_cast<sqlite3_int64>(mtime1)) {
        arc.cloneFileFromPrevious(snapshotId, rel, prevIt->second);
        stats.reused++;
        return;
    }

    jobQueue.push({fullPath, rel, static_cast<sqlite3_int64>(size1), static_cast<sqlite3_int64>(mtime1)});
}

static bool tryQueueFilesFromChangeTracker(
    const fs::path& sourceRoot,
    const std::unordered_set<std::string>& selfExclusions,
    const std::map<std::string, FileInfo>& prevMap,
    StorageArchive& arc,
    sqlite3_int64 snapshotId,
    ThreadSafeQueue<FileProcessTask>& jobQueue,
    AtomicStats& stats,
    std::uint64_t lastToken,
    std::uint64_t& newToken)
{
    auto tracker = createPlatformChangeTracker();
    if (!tracker) return false;
    if (!tracker->isAvailable()) {
        std::cout
            << "CBT habilitado, mas o backend nativo "
            << tracker->backendName()
            << " ainda nao esta implementado. Usando full scan.\n";
        return false;
    }

    tracker->startTracking(sourceRoot);
    const std::vector<ChangedFile> changes = tracker->getChanges(lastToken, newToken);

    std::unordered_set<std::string> changedPaths;
    std::unordered_set<std::string> deletedPaths;
    changedPaths.reserve(changes.size());
    deletedPaths.reserve(changes.size());

    for (const auto& change : changes) {
        const std::string rel = normalizeRelPath(change.relPath);
        if (!isSafeRelativePath(rel) || shouldSkipRelativeFileByPolicy(rel)) {
            stats.warnings++;
            continue;
        }
        if (change.isDeleted) {
            deletedPaths.insert(rel);
            continue;
        }
        changedPaths.insert(rel);
    }

    std::cout
        << "CBT ativo (" << tracker->backendName() << "): "
        << changedPaths.size() << " mudanca(s), "
        << deletedPaths.size() << " remocao(oes).\n";

    for (const auto& [relPath, prev] : prevMap) {
        if (changedPaths.find(relPath) != changedPaths.end()) continue;
        if (deletedPaths.find(relPath) != deletedPaths.end()) continue;
        arc.cloneFileFromPrevious(snapshotId, relPath, prev);
        stats.reused++;
    }

    std::unordered_set<std::string> seenPaths;
    seenPaths.reserve(changedPaths.size());
    for (const auto& relPath : changedPaths) {
        queueFileForBackup(
            sourceRoot / fs::path(relPath),
            sourceRoot,
            selfExclusions,
            seenPaths,
            prevMap,
            arc,
            snapshotId,
            jobQueue,
            stats
        );
    }

    return true;
}

static std::optional<std::uint64_t> tryCaptureChangeTrackerBaselineToken(const fs::path& sourceRoot) {
    auto tracker = createPlatformChangeTracker();
    if (!tracker || !tracker->isAvailable()) return std::nullopt;

    tracker->startTracking(sourceRoot);
    std::uint64_t newToken = 0;
    static_cast<void>(tracker->getChanges(0, newToken));
    if (newToken == 0) return std::nullopt;
    return newToken;
}

// =========================================================================
// WORKERS DO PIPELINE
// =========================================================================

void processFilesWorker(
    ThreadSafeQueue<FileProcessTask>& jobQueue,
    ThreadSafeQueue<DbOperation>& dbQueue,
    BloomFilter& globalBloom,
    std::mutex& bloomMutex,
    AtomicStats& stats) 
{
    FileProcessTask task;
    std::vector<unsigned char> buffer(CHUNK_SIZE);

    while (jobQueue.pop(task)) {
        std::ifstream in(task.fullPath, std::ios::binary);
        if (!in) { stats.warnings++; continue; }

        const bool storeRaw = shouldStoreRawByExtension(task.fullPath);
        std::vector<StorageArchive::PendingFileChunk> pendingChunks;
        int chunkIndex = 0;

        while (in) {
            in.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
            std::streamsize n = in.gcount();
            if (n <= 0) break;

            const ChunkHash chunkHash = blake3_digest32(buffer.data(), static_cast<std::size_t>(n));
            stats.bytesRead += static_cast<std::size_t>(n);
            stats.chunks++;

            bool isNewChunk = false;
            {
                std::lock_guard<std::mutex> lock(bloomMutex);
                if (!globalBloom.possiblyContains(chunkHash)) {
                    globalBloom.add(chunkHash);
                    isNewChunk = true;
                }
            }

            if (isNewChunk) {
                ChunkInsertDbTask chunkTask;
                chunkTask.hash = chunkHash;
                chunkTask.rawSize = static_cast<std::size_t>(n);
                
                if (storeRaw) {
                    chunkTask.data.assign(buffer.begin(), buffer.begin() + n);
                    chunkTask.compressionType = "raw";
                } else {
                    // OTIMIZAÇÃO APLICADA: Zero-Allocation Copy
                    // Passamos chunkTask.data por referência. O zstdCompress aloca a memória internamente.
                    Compactador::zstdCompress(buffer.data(), n, 1, chunkTask.data);
                    chunkTask.compressionType = "zstd";
                }
                dbQueue.push(std::move(chunkTask));
            }

            pendingChunks.push_back({chunkIndex, chunkHash, static_cast<std::size_t>(n)});
            ++chunkIndex;
        }

        std::error_code qec;
        const auto size2 = fs::file_size(task.fullPath, qec);
        const auto mtimeRaw2 = fs::last_write_time(task.fullPath, qec);
        
        if (!qec && task.size == static_cast<sqlite3_int64>(size2) && 
            task.mtime == fileTimeToUnixSeconds(mtimeRaw2)) {
            FileCommitDbTask commitTask{task.relPath, task.size, task.mtime, std::move(pendingChunks)};
            dbQueue.push(std::move(commitTask));
            stats.added++;
        } else {
            stats.warnings++;
        }
    }
}

void databaseWriterWorker(
    StorageArchive& arc,
    sqlite3_int64 snapshotId,
    ThreadSafeQueue<DbOperation>& dbQueue,
    AtomicStats& stats) 
{
    DbOperation op;
    while (dbQueue.pop(op)) {
        if (std::holds_alternative<ChunkInsertDbTask>(op)) {
            auto& chunk = std::get<ChunkInsertDbTask>(op);
            bool inserted = arc.insertChunkIfMissing(chunk.hash, chunk.rawSize, chunk.data, chunk.compressionType);
            if (inserted) stats.uniqueChunksInserted++;
            
        } else if (std::holds_alternative<FileCommitDbTask>(op)) {
            auto& file = std::get<FileCommitDbTask>(op);
            const sqlite3_int64 fileId = arc.insertFilePlaceholder(snapshotId, file.relPath, file.size, file.mtime);
            arc.addFileChunksBulk(fileId, file.chunks);
        }
    }
}

// =========================================================================
// ORQUESTRADOR PRINCIPAL
// =========================================================================

BackupStats runBackup(StorageArchive& arc, const fs::path& sourceRoot, const fs::path& archivePath, const std::string& label) {
    AtomicStats stats;
    if (!fs::exists(sourceRoot) || !fs::is_directory(sourceRoot)) throw std::runtime_error("Origem invalida.");

    std::cout << "Iniciando backup Enterprise-Grade...\n";
    const auto selfExclusions = buildSelfBackupExclusions(archivePath);
    const bool cbtEnabled = readArchiveBoolConfig(archivePath, "scan.cbt.enabled", false);
    std::uint64_t newCbtToken = 0;
    
    arc.begin();

    try {
        auto prevSnapshotOpt = arc.latestSnapshotId();
        const std::uint64_t lastCbtToken = arc.latestSnapshotCbtToken().value_or(0);
        std::map<std::string, FileInfo> prevMap;
        if (prevSnapshotOpt) prevMap = arc.loadSnapshotFileMap(*prevSnapshotOpt);

        const sqlite3_int64 snapshotId = arc.createSnapshot(sourceRoot.string(), label);
        std::unordered_set<std::string> seenPaths;
        seenPaths.reserve(100000);

        BloomFilter globalBloom(16 * 1024 * 1024 * 8); // 16MB de RAM para proteger contra bilhões de colisões
        std::mutex bloomMutex;

        ThreadSafeQueue<FileProcessTask> jobQueue;
        ThreadSafeQueue<DbOperation> dbQueue;

        std::thread dbThread(databaseWriterWorker, std::ref(arc), snapshotId, std::ref(dbQueue), std::ref(stats));

        const unsigned hwThreads = std::thread::hardware_concurrency();
        const unsigned workerCount = std::max<unsigned>(1, hwThreads > 2 ? hwThreads - 2 : 1);
        std::vector<std::thread> workers;
        for (unsigned i = 0; i < workerCount; ++i) {
            workers.emplace_back(processFilesWorker, std::ref(jobQueue), std::ref(dbQueue), 
                                 std::ref(globalBloom), std::ref(bloomMutex), std::ref(stats));
        }

        bool queuedFromTracker = false;
        if (cbtEnabled && prevSnapshotOpt.has_value()) {
            queuedFromTracker = tryQueueFilesFromChangeTracker(
                sourceRoot,
                selfExclusions,
                prevMap,
                arc,
                snapshotId,
                jobQueue,
                stats,
                lastCbtToken,
                newCbtToken
            );
        }

        if (!queuedFromTracker) {
            std::error_code itEc;
            fs::recursive_directory_iterator it(sourceRoot, fs::directory_options::skip_permission_denied, itEc);
            fs::recursive_directory_iterator end;

            for (; it != end; it.increment(itEc)) {
                if (itEc) { stats.warnings++; itEc.clear(); continue; }

                std::error_code qec;
                if (it->is_directory(qec)) {
                    if (qec) { stats.warnings++; continue; }
                    const std::string lowerName = lowerAscii(it->path().filename().string());
                    if (shouldSkipDirByName(lowerName)) { it.disable_recursion_pending(); continue; }
                    fs::path relDir = fs::relative(it->path(), sourceRoot, qec);
                    if (!qec && pathContainsTrashSegment(relDir)) it.disable_recursion_pending();
                    continue;
                }

                if (!it->is_regular_file(qec)) { if (qec) stats.warnings++; continue; }

                queueFileForBackup(
                    it->path(),
                    sourceRoot,
                    selfExclusions,
                    seenPaths,
                    prevMap,
                    arc,
                    snapshotId,
                    jobQueue,
                    stats
                );
            }
        }

        jobQueue.finish();
        for (auto& w : workers) if (w.joinable()) w.join();

        dbQueue.finish();
        if (dbThread.joinable()) dbThread.join();

        if (queuedFromTracker && newCbtToken != 0) {
            arc.updateSnapshotCbtToken(snapshotId, newCbtToken);
        } else if (cbtEnabled) {
            const auto baselineToken = tryCaptureChangeTrackerBaselineToken(sourceRoot);
            if (baselineToken.has_value()) {
                arc.updateSnapshotCbtToken(snapshotId, *baselineToken);
            }
        }

        arc.commit();
        BackupStats finalStats = stats.toBackupStats();
        return finalStats;

    } catch (...) {
        arc.rollback();
        throw;
    }
}

BackupStats ScanEngine::backupFolderToKply(const fs::path& sourceRoot, const fs::path& archivePath, const std::string& label) {
    const fs::path sourceAbs = fs::absolute(sourceRoot).lexically_normal();
    if (sourceAbs != fs::path("/home")) throw std::runtime_error("Origem permitida: /home");

    const fs::path archiveAbs = fs::absolute(archivePath).lexically_normal();
    std::error_code ec;
    if (!archiveAbs.parent_path().empty()) fs::create_directories(archiveAbs.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando pasta: " + ec.message());

    StorageArchive arc(archiveAbs);
    return runBackup(arc, sourceAbs, archiveAbs, label);
}
} // namespace keeply

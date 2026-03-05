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
#include <exception>
#include <filesystem>
#include <functional>
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

struct ChunkHashHasher {
    std::size_t operator()(const ChunkHash& h) const noexcept {
        std::size_t out = 1469598103934665603ull; // FNV-1a 64-bit
        for (unsigned char b : h) {
            out ^= static_cast<std::size_t>(b);
            out *= 1099511628211ull;
        }
        return out;
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

struct ProgressState {
    std::atomic<std::size_t> filesQueued{0};
    std::atomic<std::size_t> filesCompleted{0};
    std::atomic<std::size_t> activeFiles{0};
    std::atomic<bool> discoveryComplete{false};
    std::atomic<bool> finished{false};
    std::atomic<bool> failed{false};
    mutable std::mutex currentFileMutex;
    std::string currentFile;

    void setCurrentFile(const std::string& value) {
        std::lock_guard<std::mutex> lock(currentFileMutex);
        currentFile = value;
    }

    std::string currentFileSnapshot() const {
        std::lock_guard<std::mutex> lock(currentFileMutex);
        return currentFile;
    }
};

static BackupProgress buildProgressSnapshot(const AtomicStats& stats,
                                            const ProgressState& progressState) {
    BackupProgress progress;
    progress.stats = stats.toBackupStats();
    progress.filesQueued = progressState.filesQueued.load();
    progress.filesCompleted = progressState.filesCompleted.load();
    progress.discoveryComplete = progressState.discoveryComplete.load();
    progress.currentFile = progressState.currentFileSnapshot();
    if (progressState.failed.load()) progress.phase = "failed";
    else if (progressState.finished.load()) progress.phase = "finished";
    else if (!progress.discoveryComplete) progress.phase = "discovering";
    else progress.phase = "processing";
    return progress;
}

static void setWorkerErrorOnce(
    std::exception_ptr ep,
    std::exception_ptr& sharedError,
    std::mutex& sharedErrorMutex)
{
    std::lock_guard<std::mutex> lock(sharedErrorMutex);
    if (!sharedError) sharedError = ep;
}

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
        "build", "out", "obj", "bin", ".terraform", ".klyp"
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
        ".klyp", ".zip", ".7z", ".rar", ".gz", ".jpg", ".png", 
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
    {
        fs::path packPath = archiveAbs;
        packPath.replace_extension(".klyp");
        out.insert(normalizeAbsBestEffort(packPath).string());
        out.insert(normalizeAbsBestEffort(fs::path(packPath.string() + ".idx")).string());
    }
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
    AtomicStats& stats,
    ProgressState& progressState)
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
    progressState.filesQueued++;
}

static bool tryQueueFilesFromChangeTracker(
    const fs::path& sourceRoot,
    const std::unordered_set<std::string>& selfExclusions,
    const std::map<std::string, FileInfo>& prevMap,
    StorageArchive& arc,
    sqlite3_int64 snapshotId,
    ThreadSafeQueue<FileProcessTask>& jobQueue,
    AtomicStats& stats,
    ProgressState& progressState,
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
            stats,
            progressState
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
    std::unordered_set<ChunkHash, ChunkHashHasher>& seenChunkHashes,
    std::mutex& chunkSetMutex,
    AtomicStats& stats,
    ProgressState& progressState,
    std::atomic<bool>& cancelRequested,
    std::exception_ptr& workerError,
    std::mutex& workerErrorMutex) 
{
    try {
        FileProcessTask task;
        std::vector<unsigned char> buffer(CHUNK_SIZE);

        while (!cancelRequested.load() && jobQueue.pop(task)) {
            progressState.activeFiles++;
            progressState.setCurrentFile(task.relPath);
            std::ifstream in(task.fullPath, std::ios::binary);
            if (!in) {
                stats.warnings++;
                progressState.filesCompleted++;
                if (progressState.activeFiles.fetch_sub(1) == 1) {
                    progressState.setCurrentFile("");
                }
                continue;
            }

            const bool storeRaw = shouldStoreRawByExtension(task.fullPath);
            std::vector<StorageArchive::PendingFileChunk> pendingChunks;
            int chunkIndex = 0;

            while (!cancelRequested.load() && in) {
                in.read(reinterpret_cast<char*>(buffer.data()), buffer.size());
                std::streamsize n = in.gcount();
                if (n <= 0) break;

                const ChunkHash chunkHash = blake3_digest32(buffer.data(), static_cast<std::size_t>(n));
                stats.bytesRead += static_cast<std::size_t>(n);
                stats.chunks++;

                bool isNewChunk = false;
                {
                    std::lock_guard<std::mutex> lock(chunkSetMutex);

                    if (!globalBloom.possiblyContains(chunkHash)) {
                        globalBloom.add(chunkHash);
                        isNewChunk = seenChunkHashes.insert(chunkHash).second;
                    } else {
                        // Bloom pode ter falso positivo; o set exato remove esse risco.
                        const auto inserted = seenChunkHashes.insert(chunkHash).second;
                        if (inserted) {
                            globalBloom.add(chunkHash);
                            isNewChunk = true;
                        }
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
            
            if (!cancelRequested.load() &&
                !qec && task.size == static_cast<sqlite3_int64>(size2) && 
                task.mtime == fileTimeToUnixSeconds(mtimeRaw2)) {
                FileCommitDbTask commitTask{task.relPath, task.size, task.mtime, std::move(pendingChunks)};
                dbQueue.push(std::move(commitTask));
                stats.added++;
            } else {
                stats.warnings++;
            }
            progressState.filesCompleted++;
            if (progressState.activeFiles.fetch_sub(1) == 1) {
                progressState.setCurrentFile("");
            }
        }
    } catch (...) {
        cancelRequested.store(true);
        progressState.failed.store(true);
        setWorkerErrorOnce(std::current_exception(), workerError, workerErrorMutex);
        jobQueue.finish();
        dbQueue.finish();
    }
}

void databaseWriterWorker(
    StorageArchive& arc,
    sqlite3_int64 snapshotId,
    ThreadSafeQueue<DbOperation>& dbQueue,
    ThreadSafeQueue<FileProcessTask>& jobQueue,
    AtomicStats& stats,
    ProgressState& progressState,
    std::atomic<bool>& cancelRequested,
    std::exception_ptr& workerError,
    std::mutex& workerErrorMutex) 
{
    try {
        DbOperation op;
        while (!cancelRequested.load() && dbQueue.pop(op)) {
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
    } catch (...) {
        cancelRequested.store(true);
        progressState.failed.store(true);
        setWorkerErrorOnce(std::current_exception(), workerError, workerErrorMutex);
        jobQueue.finish();
        dbQueue.finish();
    }
}

// =========================================================================
// ORQUESTRADOR PRINCIPAL
// =========================================================================

BackupStats runBackup(StorageArchive& arc,
                      const fs::path& sourceRoot,
                      const fs::path& archivePath,
                      const std::string& label,
                      const std::function<void(const BackupProgress&)>& progressCallback) {
    AtomicStats stats;
    ProgressState progressState;
    if (!fs::exists(sourceRoot) || !fs::is_directory(sourceRoot)) throw std::runtime_error("Origem invalida.");

    std::cout << "Iniciando backup Enterprise-Grade...\n";
    const auto selfExclusions = buildSelfBackupExclusions(archivePath);
    const bool cbtEnabled = readArchiveBoolConfig(archivePath, "scan.cbt.enabled", false);
    std::uint64_t newCbtToken = 0;
    std::unique_ptr<std::thread> progressThread;
    
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
        std::unordered_set<ChunkHash, ChunkHashHasher> seenChunkHashes;
        seenChunkHashes.reserve(262144);
        std::mutex chunkSetMutex;

        ThreadSafeQueue<FileProcessTask> jobQueue;
        ThreadSafeQueue<DbOperation> dbQueue;
        std::atomic<bool> cancelRequested{false};
        std::exception_ptr workerError;
        std::mutex workerErrorMutex;
        if (progressCallback) {
            progressCallback(buildProgressSnapshot(stats, progressState));
            progressThread = std::make_unique<std::thread>([&stats, &progressState, &progressCallback]() {
                using namespace std::chrono_literals;
                while (!progressState.finished.load() && !progressState.failed.load()) {
                    progressCallback(buildProgressSnapshot(stats, progressState));
                    std::this_thread::sleep_for(500ms);
                }
                progressCallback(buildProgressSnapshot(stats, progressState));
            });
        }

        std::thread dbThread(
            databaseWriterWorker,
            std::ref(arc),
            snapshotId,
            std::ref(dbQueue),
            std::ref(jobQueue),
            std::ref(stats),
            std::ref(progressState),
            std::ref(cancelRequested),
            std::ref(workerError),
            std::ref(workerErrorMutex));

        const unsigned hwThreads = std::thread::hardware_concurrency();
        const unsigned workerCount = std::max<unsigned>(1, hwThreads > 2 ? hwThreads - 2 : 1);
        std::vector<std::thread> workers;
        for (unsigned i = 0; i < workerCount; ++i) {
            workers.emplace_back(processFilesWorker, std::ref(jobQueue), std::ref(dbQueue), 
                                 std::ref(globalBloom), std::ref(seenChunkHashes),
                                 std::ref(chunkSetMutex), std::ref(stats),
                                 std::ref(progressState),
                                 std::ref(cancelRequested),
                                 std::ref(workerError),
                                 std::ref(workerErrorMutex));
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
                progressState,
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
                    stats,
                    progressState
                );
            }
        }

        progressState.discoveryComplete.store(true);
        jobQueue.finish();
        for (auto& w : workers) if (w.joinable()) w.join();

        dbQueue.finish();
        if (dbThread.joinable()) dbThread.join();

        if (workerError) std::rethrow_exception(workerError);

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
        progressState.finished.store(true);
        progressState.setCurrentFile("");
        if (progressThread && progressThread->joinable()) progressThread->join();
        return finalStats;

    } catch (...) {
        progressState.failed.store(true);
        progressState.setCurrentFile("");
        if (progressThread && progressThread->joinable()) progressThread->join();
        arc.rollback();
        throw;
    }
}

// =========================================================================
// DETECÇÃO CROSS-PLATFORM DE RAÍZES DISPONÍVEIS
// =========================================================================

/**
 * Retorna todas as raízes de sistema de arquivos disponíveis para backup.
 *
 * Windows : Enumera todos os drives montados (C:\, D:\, E:\, ...).
 *           Usa GetLogicalDriveStringsW para cobrir drives de rede mapeados
 *           e volumes adicionais além do disco principal.
 *
 * Linux   : Parseia /proc/mounts e retorna pontos de montagem reais
 *           (ext4, btrfs, xfs, f2fs, ntfs, vfat, exfat, fuse).
 *           Exclui pseudo-filesystems (proc, sysfs, devtmpfs, cgroup, etc.)
 *           que nunca devem ser alvo de backup.
 *
 * macOS   : Lê /proc/mounts não existe; usa /etc/mtab ou fallback para /
 *           e /Volumes para detectar drives externos.
 */
static std::vector<fs::path> getAvailableSourceRoots() {
    std::vector<fs::path> roots;

#if defined(_WIN32)
    // --- Windows: enumera letras de drive via WinAPI ---
    wchar_t buf[512] = {};
    const DWORD len = GetLogicalDriveStringsW(static_cast<DWORD>(std::size(buf)), buf);
    if (len > 0 && len < std::size(buf)) {
        const wchar_t* p = buf;
        while (*p) {
            const std::wstring drive(p);
            const UINT driveType = GetDriveTypeW(drive.c_str());
            // Aceita: Fixed, Removable, Remote (rede), RAM disk
            // Rejeita: CD-ROM (5) e Unknown (0) / No root dir (1)
            if (driveType != DRIVE_CDROM &&
                driveType != DRIVE_UNKNOWN &&
                driveType != DRIVE_NO_ROOT_DIR)
            {
                roots.emplace_back(drive);
            }
            p += drive.size() + 1;
        }
    }
    // Garante ao menos C:\ como fallback caso a API falhe
    if (roots.empty()) roots.emplace_back(L"C:\\");

#elif defined(__APPLE__)
    // --- macOS: / sempre existe; /Volumes/* para externos ---
    roots.emplace_back("/");
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator("/Volumes", ec)) {
        if (!ec && entry.is_directory()) roots.emplace_back(entry.path());
    }

#else
    // --- Linux: lê /proc/mounts para descobrir pontos de montagem reais ---
    static const std::unordered_set<std::string> kRealFsTypes = {
        "ext2", "ext3", "ext4", "btrfs", "xfs", "jfs", "reiserfs",
        "f2fs", "nilfs2", "ntfs", "ntfs3", "vfat", "exfat", "msdos",
        "fuse", "fuseblk", "overlay", "zfs", "bcachefs"
    };

    std::ifstream mounts("/proc/mounts");
    if (mounts.is_open()) {
        std::string device, mountpoint, fstype, rest;
        while (mounts >> device >> mountpoint >> fstype) {
            std::getline(mounts, rest); // descarta opções e dump/pass
            if (kRealFsTypes.find(fstype) != kRealFsTypes.end()) {
                std::error_code ec;
                const fs::path mp(mountpoint);
                if (fs::is_directory(mp, ec) && !ec) {
                    roots.emplace_back(mp.lexically_normal());
                }
            }
        }
    }

    // Fallback: se /proc/mounts não existir (container, chroot), usa /
    if (roots.empty()) roots.emplace_back("/");
#endif

    return roots;
}

/**
 * Verifica se `candidate` está contido dentro de alguma raiz conhecida.
 * Aceita tanto a própria raiz (backup do drive inteiro) quanto qualquer
 * subdiretório dentro dela.
 */
static bool isUnderKnownRoot(const fs::path& candidate,
                              const std::vector<fs::path>& roots)
{
    for (const auto& root : roots) {
        // lexically_relative retorna caminho vazio ou com ".." se não for subdir
        std::error_code ec;
        const fs::path rel = candidate.lexically_relative(root);
        if (!rel.empty() && rel.native().find(fs::path("..").native()) == std::string::npos) {
            return true;
        }
    }
    return false;
}

// =========================================================================
// PONTO DE ENTRADA PÚBLICO
// =========================================================================

BackupStats ScanEngine::backupFolderToKply(const fs::path& sourceRoot,
                                            const fs::path& archivePath,
                                            const std::string& label,
                                            const std::function<void(const BackupProgress&)>& progressCallback)
{
    const fs::path sourceAbs = fs::absolute(sourceRoot).lexically_normal();

    // --- Valida que a origem existe e é um diretório ---
    if (!fs::exists(sourceAbs) || !fs::is_directory(sourceAbs)) {
        throw std::runtime_error("Origem invalida ou nao e um diretorio: " + sourceAbs.string());
    }

    // --- Detecta raízes disponíveis na plataforma atual ---
    const std::vector<fs::path> availableRoots = getAvailableSourceRoots();

    if (availableRoots.empty()) {
        throw std::runtime_error("Nenhuma raiz de sistema de arquivos detectada na plataforma.");
    }

    // --- Garante que a origem pertence a um volume conhecido ---
    if (!isUnderKnownRoot(sourceAbs, availableRoots)) {
        std::string rootList;
        for (const auto& r : availableRoots) rootList += "\n  " + r.string();
        throw std::runtime_error(
            "Origem '" + sourceAbs.string() +
            "' nao esta em nenhum volume detectado." +
            "\nVolumes disponiveis:" + rootList
        );
    }

    // --- Cria o diretório do arquivo se necessário ---
    const fs::path archiveAbs = fs::absolute(archivePath).lexically_normal();
    std::error_code ec;
    if (!archiveAbs.parent_path().empty()) {
        fs::create_directories(archiveAbs.parent_path(), ec);
    }
    if (ec) throw std::runtime_error("Falha criando pasta do arquivo: " + ec.message());

    StorageArchive arc(archiveAbs);
    return runBackup(arc, sourceAbs, archiveAbs, label, progressCallback);
}

/**
 * Utilitário público: lista todos os volumes disponíveis para backup.
 * Útil para GUIs ou CLIs exibirem opções ao usuário antes do backup.
 */
std::vector<std::string> ScanEngine::listAvailableSourceRoots() {
    const auto roots = getAvailableSourceRoots();
    std::vector<std::string> out;
    out.reserve(roots.size());
    for (const auto& r : roots) out.push_back(r.string());
    return out;
}
} // namespace keeply

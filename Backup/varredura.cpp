#include "../keeply.hpp"
#include "Rastreamento/rastreamento_mudancas.hpp"
#include "../Core/utilitarios.hpp"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

namespace keeply {
namespace {

// hexNibble/hexToBlob removidos — usar keeply::hexNibble/keeply::hexDecode de utilitarios_backup.hpp

static Blob hexToBlob(const std::string& hex) {
    return keeply::hexDecode(hex);
}

static ChunkHash hexToChunkHash(const std::string& hex) {
    if (hex.size() != 64) throw std::runtime_error("Chunk hash hex invalido.");
    ChunkHash out{};
    for (std::size_t i = 0; i < out.size(); ++i) {
        out[i] = static_cast<unsigned char>(
            (keeply::hexNibble(hex[i * 2]) << 4) | keeply::hexNibble(hex[i * 2 + 1]));
    }
    return out;
}

static Blob buildFileHashFromChunkSequence(const std::vector<ChunkHash>& hashes) {
    Blob seq;
    seq.reserve(hashes.size() * ChunkHash{}.size());
    for (const auto& h : hashes) seq.insert(seq.end(), h.begin(), h.end());
    return hexToBlob(Compactador::blake3Hex(seq.data(), seq.size()));
}

static void setFileMtime(const fs::path& p, sqlite3_int64 unixSecs) {
    std::error_code ec;
    const auto tp = fs::file_time_type::clock::now() + std::chrono::seconds(unixSecs - fileTimeToUnixSeconds(fs::file_time_type::clock::now()));
    fs::last_write_time(p, tp, ec);
}

static void progressEmit(const std::function<void(const BackupProgress&)>& cb, const BackupProgress& p) {
    if (cb) cb(p);
}

static bool parseBoolConfigValue(const std::string& rawValue, bool defaultValue) {
    std::string value = trim(rawValue);
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (value.empty()) return defaultValue;
    if (value == "1" || value == "true" || value == "on" || value == "yes") return true;
    if (value == "0" || value == "false" || value == "off" || value == "no") return false;
    return defaultValue;
}

static bool readArchiveBoolConfig(const fs::path& archivePath, const char* key, bool defaultValue) {
    sqlite3* rawDb = nullptr;
    const int openRc = sqlite3_open_v2_path(
        archivePath,
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

static std::vector<fs::path> discoverFiles(const fs::path& root, BackupProgress& progress, const std::function<void(const BackupProgress&)>& cb) {
    std::vector<fs::path> files;
    std::error_code ec;
    fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied, ec), end;
    for (; it != end; it.increment(ec)) {
        if (ec) {
            ++progress.stats.warnings;
            ec.clear();
            continue;
        }
        std::error_code pathEc;
        const fs::path currentPath = it->path();
        if (it->is_directory(pathEc) && !pathEc && isExcludedBySystemPolicy(root, currentPath)) {
            it.disable_recursion_pending();
            continue;
        }
        std::error_code tec;
        if (!it->is_regular_file(tec) || tec) continue;
        files.push_back(it->path());
    }
    progress.filesQueued = files.size();
    progress.discoveryComplete = true;
    progress.phase = "backup";
    progressEmit(cb, progress);
    return files;
}

static std::vector<fs::path> discoverFilesFromTracker(
    const fs::path& root,
    const std::map<std::string, FileInfo>& prevMap,
    StorageArchive& arc,
    sqlite3_int64 snapshotId,
    std::uint64_t lastToken,
    std::uint64_t& newToken,
    BackupProgress& progress,
    const std::function<void(const BackupProgress&)>& cb)
{
    auto tracker = createPlatformChangeTracker();
    if (!tracker || !tracker->isAvailable()) {
        throw std::runtime_error("CBT indisponivel.");
    }

    tracker->startTracking(root);
    auto changes = tracker->getChanges(lastToken, newToken);

    std::unordered_set<std::string> changedPaths;
    std::unordered_set<std::string> deletedPaths;
    changedPaths.reserve(changes.size());
    deletedPaths.reserve(changes.size());

    for (const auto& change : changes) {
        const std::string rel = normalizeRelPath(change.relPath);
        if (!isSafeRelativePath(rel)) {
            ++progress.stats.warnings;
            continue;
        }
        if (change.isDeleted) deletedPaths.insert(rel);
        else changedPaths.insert(rel);
    }

    for (const auto& [relPath, prev] : prevMap) {
        if (changedPaths.find(relPath) != changedPaths.end()) continue;
        if (deletedPaths.find(relPath) != deletedPaths.end()) continue;
        arc.cloneFileFromPrevious(snapshotId, relPath, prev);
        ++progress.stats.reused;
    }

    std::vector<fs::path> files;
    files.reserve(changedPaths.size());
    for (const auto& relPath : changedPaths) {
        const fs::path fullPath = root / fs::path(relPath);
        std::error_code ec;
        if (!fs::exists(fullPath, ec) || !fs::is_regular_file(fullPath, ec) || ec) {
            ++progress.stats.warnings;
            continue;
        }
        files.push_back(fullPath);
    }

    progress.filesQueued = files.size();
    progress.discoveryComplete = true;
    progress.phase = "backup";
    progressEmit(cb, progress);
    return files;
}

static std::optional<std::uint64_t> captureBaselineToken(const fs::path& root) {
    auto tracker = createPlatformChangeTracker();
    if (!tracker || !tracker->isAvailable()) return std::nullopt;
    try {
        tracker->startTracking(root);
        std::uint64_t newToken = 0;
        static_cast<void>(tracker->getChanges(0, newToken));
        if (newToken == 0) return std::nullopt;
        return newToken;
    } catch (...) {
        return std::nullopt;
    }
}

static Blob readWholeFile(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    if (!in) throw std::runtime_error("Falha abrindo arquivo: " + p.string());
    in.seekg(0, std::ios::end);
    const auto sz = in.tellg();
    in.seekg(0, std::ios::beg);
    if (sz < 0) throw std::runtime_error("Falha lendo tamanho do arquivo: " + p.string());
    Blob out(static_cast<std::size_t>(sz));
    if (!out.empty()) in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size()));
    if (!in && !out.empty()) throw std::runtime_error("Falha lendo arquivo inteiro: " + p.string());
    return out;
}

static void restoreChunksToFile(StorageArchive& arc, sqlite3_int64 fileId, const fs::path& outPath) {
    auto chunks = arc.loadFileChunks(fileId);
    std::error_code ec;
    fs::create_directories(outPath.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretorio de restore: " + ec.message());
    std::ofstream out(outPath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Falha criando arquivo restaurado: " + outPath.string());
    for (const auto& c : chunks) {
        if (!c.blob.empty()) out.write(reinterpret_cast<const char*>(c.blob.data()), static_cast<std::streamsize>(c.blob.size()));
        if (!out) throw std::runtime_error("Falha escrevendo arquivo restaurado: " + outPath.string());
    }
}

} // namespace

BackupStats ScanEngine::backupFolderToKply(const fs::path& sourceRoot, const fs::path& archivePath, const std::string& label, const std::function<void(const BackupProgress&)>& progressCallback) {
    ensureDefaults();
    if (!fs::exists(sourceRoot) || !fs::is_directory(sourceRoot)) throw std::runtime_error("sourceRoot invalido.");
    StorageArchive arc(archivePath);
    BackupProgress progress;
    progress.phase = "discovery";
    progressEmit(progressCallback, progress);

    const bool cbtEnabled = readArchiveBoolConfig(archivePath, "scan.cbt.enabled", false);
    const auto prevSnapshotId = arc.latestSnapshotId();
    const auto lastCbtToken = arc.latestSnapshotCbtToken().value_or(0);
    const auto prevMap = arc.loadLatestSnapshotFileMap();

    arc.begin();
    try {
        const sqlite3_int64 snapshotId = arc.createSnapshot(sourceRoot.string(), label);
        std::uint64_t newCbtToken = 0;
        std::vector<fs::path> files;

        if (cbtEnabled && prevSnapshotId.has_value()) {
            try {
                files = discoverFilesFromTracker(
                    sourceRoot,
                    prevMap,
                    arc,
                    snapshotId,
                    lastCbtToken,
                    newCbtToken,
                    progress,
                    progressCallback
                );
            } catch (const std::exception& ex) {
                std::cout << "CBT indisponivel, usando full scan: " << ex.what() << "\n";
                files = discoverFiles(sourceRoot, progress, progressCallback);
            }
        } else {
            files = discoverFiles(sourceRoot, progress, progressCallback);
        }

        for (const auto& filePath : files) {
            progress.currentFile = filePath.string();
            ++progress.stats.scanned;

            std::error_code ec1, ec2, ec3;
            const auto rel = normalizeRelPath(fs::relative(filePath, sourceRoot, ec1).generic_string());
            if (ec1 || !isSafeRelativePath(rel)) {
                ++progress.stats.warnings;
                ++progress.filesCompleted;
                progressEmit(progressCallback, progress);
                continue;
            }

            const auto size = static_cast<sqlite3_int64>(fs::file_size(filePath, ec2));
            const auto mtime = static_cast<sqlite3_int64>(fileTimeToUnixSeconds(fs::last_write_time(filePath, ec3)));
            if (ec2 || ec3) {
                ++progress.stats.warnings;
                ++progress.filesCompleted;
                progressEmit(progressCallback, progress);
                continue;
            }

            const auto prevIt = prevMap.find(rel);
            if (prevIt != prevMap.end() && prevIt->second.size == size && prevIt->second.mtime == mtime) {
                arc.cloneFileFromPrevious(snapshotId, rel, prevIt->second);
                ++progress.stats.reused;
                ++progress.filesCompleted;
                progressEmit(progressCallback, progress);
                continue;
            }

            sqlite3_int64 fileId = 0;
            try {
                fileId = arc.insertFilePlaceholder(snapshotId, rel, size, mtime);
                std::ifstream in(filePath, std::ios::binary);
                if (!in) throw std::runtime_error("Falha abrindo arquivo para backup: " + filePath.string());

                Blob raw(CHUNK_SIZE);
                std::vector<StorageArchive::PendingFileChunk> rows;
                std::vector<ChunkHash> chunkSeq;

                int chunkIdx = 0;
                while (in) {
                    in.read(reinterpret_cast<char*>(raw.data()), static_cast<std::streamsize>(raw.size()));
                    const std::streamsize got = in.gcount();
                    if (got <= 0) break;

                    ++progress.stats.chunks;
                    progress.stats.bytesRead += static_cast<std::uintmax_t>(got);

                    const std::string chunkHex = Compactador::blake3Hex(raw.data(), static_cast<std::size_t>(got));
                    const ChunkHash ch = hexToChunkHash(chunkHex);
                    chunkSeq.push_back(ch);

                    Blob comp;
                    Compactador::zstdCompress(raw.data(), static_cast<std::size_t>(got), 3, comp);
                    if (arc.insertChunkIfMissing(ch, static_cast<std::size_t>(got), comp, "zstd")) {
                        ++progress.stats.uniqueChunksInserted;
                    }

                    StorageArchive::PendingFileChunk row;
                    row.chunkIdx = chunkIdx++;
                    row.chunkHash = ch;
                    row.rawSize = static_cast<std::size_t>(got);
                    rows.push_back(row);
                }

                const Blob fileHash = buildFileHashFromChunkSequence(chunkSeq);
                arc.addFileChunksBulk(fileId, rows);
                arc.updateFileHash(fileId, fileHash);
                ++progress.stats.added;
            } catch (...) {
                if (fileId != 0) {
                    try { arc.deleteFileRecord(fileId); } catch (...) {}
                }
                ++progress.stats.warnings;
            }

            ++progress.filesCompleted;
            progressEmit(progressCallback, progress);
        }

        progress.phase = "commit";
        progressEmit(progressCallback, progress);
        if (cbtEnabled) {
            if (newCbtToken != 0) {
                arc.updateSnapshotCbtToken(snapshotId, newCbtToken);
            } else if (auto baseline = captureBaselineToken(sourceRoot); baseline.has_value()) {
                arc.updateSnapshotCbtToken(snapshotId, *baseline);
            }
        }
        arc.commit();
        progress.phase = "done";
        progressEmit(progressCallback, progress);
        return progress.stats;
    } catch (...) {
        try { arc.rollback(); } catch (...) {}
        throw;
    }
}

std::vector<std::string> ScanEngine::listAvailableSourceRoots() {
    std::vector<std::string> out;
#ifdef _WIN32
    for (char d = 'A'; d <= 'Z'; ++d) {
        std::string root;
        root += d;
        root += ":\\";
        if (fs::exists(root)) out.push_back(root);
    }
#else
    if (fs::exists("/")) out.push_back("/");
    const std::vector<KnownDirectory> knownDirs = {
        KnownDirectory::Home,
        KnownDirectory::Documents,
        KnownDirectory::Desktop,
        KnownDirectory::Downloads,
        KnownDirectory::Pictures,
        KnownDirectory::Music,
        KnownDirectory::Videos
    };
    for (const auto dir : knownDirs) {
        if (const auto path = knownDirectoryPath(dir); path && fs::exists(*path)) {
            const std::string value = path->string();
            if (std::find(out.begin(), out.end(), value) == out.end()) out.push_back(value);
        }
    }
#endif
    if (out.empty()) out.push_back(defaultSourceRootPath().string());
    return out;
}
} // namespace keeply

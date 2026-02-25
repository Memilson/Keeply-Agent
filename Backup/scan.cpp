#include "keeply.hpp"

#include <blake3.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <system_error>
#include <cstddef>
#include <thread>
#include <unordered_set>
#include <vector>

namespace keeply {

namespace fs = std::filesystem;

struct ChunkHashHasher {
    std::size_t operator()(const ChunkHash& h) const noexcept {
        std::size_t v = 1469598103934665603ULL;
        for (unsigned char b : h) {
            v ^= static_cast<std::size_t>(b);
            v *= 1099511628211ULL;
        }
        return v;
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

static bool shouldSkipDirByName(const fs::path& p) {
    const std::string name = p.filename().string();
    return name == ".cache" ||
           name == "node_modules" ||
           name == ".keeply" ||
           name == ".npm" ||
           name == ".pnpm-store" ||
           name == ".yarn" ||
           name == ".gradle" ||
           name == ".m2" ||
           name == ".cargo" ||
           name == ".rustup" ||
           name == ".venv" ||
           name == "venv";
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

static std::string lowerAscii(std::string s) {
    for (char& c : s) {
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    }
    return s;
}

static bool shouldStoreRawByExtension(const fs::path& p) {
    static const std::unordered_set<std::string> kRawExt = {
        ".keeply", ".zip", ".7z", ".rar", ".gz", ".bz2", ".xz", ".zst", ".tgz",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif",
        ".mp3", ".aac", ".ogg", ".flac", ".m4a",
        ".mp4", ".mkv", ".avi", ".mov", ".webm",
        ".pdf", ".iso", ".apk", ".deb", ".rpm"
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
    out.reserve(10);

    const fs::path db = normalizeAbsBestEffort(archiveAbs);
    out.insert(db.string());
    // Compatibilidade com variantes de pack do projeto:
    // - formato atual de alguns branches: "<archive>.keeply"
    // - formato alternativo: "<archive>.chunks.pack"
    out.insert(fs::path(db.string() + ".keeply").string());
    out.insert(fs::path(db.string() + ".chunks.pack").string());
    out.insert(fs::path(db.string() + "-wal").string());
    out.insert(fs::path(db.string() + "-shm").string());
    out.insert(fs::path(db.string() + "-journal").string());
    out.insert(fs::path(db.string() + ".keeply-wal").string());
    out.insert(fs::path(db.string() + ".keeply-shm").string());
    out.insert(fs::path(db.string() + ".keeply-journal").string());
    return out;
}

// ============================= SCAN BACKUP ===============================

BackupStats runBackup(StorageArchive& arc,
                      const fs::path& sourceRoot,
                      const fs::path& archivePath,
                      const std::string& label) {
    BackupStats stats;

    if (!fs::exists(sourceRoot) || !fs::is_directory(sourceRoot)) {
        throw std::runtime_error("Diretorio de origem invalido.");
    }

    std::cout << "Iniciando backup...\n";

    const auto selfBackupExclusions = buildSelfBackupExclusions(archivePath);

    arc.begin();

    try {
        auto prevSnapshotOpt = arc.latestSnapshotId();
        std::map<std::string, FileInfo> prevMap;
        if (prevSnapshotOpt) {
            std::cout << "Carregando snapshot anterior #" << *prevSnapshotOpt << "...\n";
            prevMap = arc.loadSnapshotFileMap(*prevSnapshotOpt);
            std::cout << "Snapshot anterior carregado: " << prevMap.size() << " arquivos\n";
        } else {
            std::cout << "Sem snapshot anterior.\n";
        }
        const sqlite3_int64 snapshotId =
            arc.createSnapshot(sourceRoot.string(), label);
        std::unordered_set<std::string> seenPathsInSnapshot;
        seenPathsInSnapshot.reserve(65536);
        std::unordered_set<ChunkHash, ChunkHashHasher> seenChunkHashes;
        seenChunkHashes.reserve(100000);
        const unsigned hw = std::thread::hardware_concurrency();
        const std::size_t maxCompressInFlight = std::max<std::size_t>(
            1, std::min<std::size_t>(8, hw > 1 ? (hw - 1) : 1)
        );
        std::size_t visitedEntries = 0;
        auto lastBeat = std::chrono::steady_clock::now();
        std::size_t lastBeatScanned = 0;
        auto maybePrintProgress = [&](const fs::path& current, bool force = false) {
            const auto now = std::chrono::steady_clock::now();
            if (!force) {
                if (stats.scanned != 0 && (stats.scanned % 1000 == 0) && stats.scanned != lastBeatScanned) {
                } else if (now - lastBeat < std::chrono::seconds(2)) {
                    return;
                }
            }
            lastBeat = now;
            lastBeatScanned = stats.scanned;
            std::cout << "PROGRESS visited=" << visitedEntries
                      << " scanned=" << stats.scanned
                      << " added=" << stats.added
                      << " reused=" << stats.reused
                      << " chunks=" << stats.chunks
                      << " bytes=" << stats.bytesRead
                      << " warnings=" << stats.warnings
                      << " path=" << current.string()
                      << "\n" << std::flush;
        };
        std::cout << "Iniciando varredura...\n";
        maybePrintProgress(sourceRoot, true);
        std::error_code itEc;
        fs::recursive_directory_iterator it(
            sourceRoot,
            fs::directory_options::skip_permission_denied,
            itEc
        );
        if (itEc) {
            throw std::runtime_error("Erro iniciando scan: " + itEc.message());
        }
        fs::recursive_directory_iterator end;
        for (; it != end; it.increment(itEc)) {
            if (itEc) {
                ++stats.warnings;
                itEc.clear();
                continue;
            }
            ++visitedEntries;
            std::error_code qec;
            if (it->is_directory(qec)) {
                if (qec) {
                    ++stats.warnings;
                    continue;
                }
                const fs::path dirPath = it->path();
                if (shouldSkipDirByName(dirPath)) {
                    it.disable_recursion_pending();
                    maybePrintProgress(dirPath);
                    continue;
                }
                std::error_code recEc;
                fs::path relDir = fs::relative(dirPath, sourceRoot, recEc);
                if (!recEc && pathContainsTrashSegment(relDir)) {
                    it.disable_recursion_pending();
                    maybePrintProgress(dirPath);
                    continue;
                }
                maybePrintProgress(dirPath);
                continue;
            }
            if (!it->is_regular_file(qec)) {
                if (qec) {
                    ++stats.warnings;
                }
                continue;
            }
            const fs::path fullPath = it->path();
            const fs::path fullAbs = normalizeAbsBestEffort(fullPath);
            if (selfBackupExclusions.find(fullAbs.string()) != selfBackupExclusions.end()) {
                maybePrintProgress(fullPath);
                continue;
            }

            ++stats.scanned;
            const std::string rel =
                normalizeRelPath(fs::relative(fullPath, sourceRoot).string());

            if (!isSafeRelativePath(rel)) continue;
            if (!seenPathsInSnapshot.insert(rel).second) {
                ++stats.warnings;
                continue;
            }

            qec.clear();
            const auto size1 = fs::file_size(fullPath, qec);
            if (qec) {
                ++stats.warnings;
                continue;
            }
            const auto mtimeRaw1 = fs::last_write_time(fullPath, qec);
            if (qec) {
                ++stats.warnings;
                continue;
            }
            const auto mtime1 = fileTimeToUnixSeconds(mtimeRaw1);

            // Verificar se arquivo é igual ao snapshot anterior
            auto prevIt = prevMap.find(rel);
            if (prevIt != prevMap.end()) {
                const auto& prev = prevIt->second;

                if (prev.size == static_cast<sqlite3_int64>(size1) &&
                    prev.mtime == static_cast<sqlite3_int64>(mtime1)) {

                    // clone direto (sem ler disco)
                    arc.cloneFileFromPrevious(snapshotId, rel, prev);
                    ++stats.reused;
                    continue;
                }
            }

            // Novo ou modificado
            const sqlite3_int64 fileId =
                arc.insertFilePlaceholder(snapshotId,
                                          rel,
                                          static_cast<sqlite3_int64>(size1),
                                          static_cast<sqlite3_int64>(mtime1));

            std::ifstream in(fullPath, std::ios::binary);
            if (!in) {
                arc.deleteFileRecord(fileId);
                ++stats.warnings;
                continue;
            }
            const bool storeRaw = shouldStoreRawByExtension(fullPath);

            std::vector<unsigned char> buffer(CHUNK_SIZE);

            std::vector<StorageArchive::PendingFileChunk> pendingChunks;
            pendingChunks.reserve(256);
            struct PendingCompressedChunk {
                ChunkHash hash{};
                std::size_t rawSize{};
                std::future<std::vector<unsigned char>> compFuture;
            };
            std::deque<PendingCompressedChunk> pendingCompressed;
            auto flushOneCompressed = [&]() {
                if (pendingCompressed.empty()) return;
                PendingCompressedChunk job = std::move(pendingCompressed.front());
                pendingCompressed.pop_front();
                auto comp = job.compFuture.get();
                const bool inserted = arc.insertChunkIfMissing(job.hash, job.rawSize, comp, "zstd");
                if (inserted) {
                    ++stats.uniqueChunksInserted;
                }
            };

            int chunkIndex = 0;

            while (in) {
                in.read(reinterpret_cast<char*>(buffer.data()),
                        static_cast<std::streamsize>(buffer.size()));

                std::streamsize n = in.gcount();
                if (n <= 0) break;

                const ChunkHash chunkHash =
                    blake3_digest32(buffer.data(), static_cast<std::size_t>(n));
                stats.bytesRead += static_cast<std::size_t>(n);
                ++stats.chunks;
                maybePrintProgress(fullPath);

                // Se chunk já apareceu nesta execução, não comprimir de novo
                if (seenChunkHashes.find(chunkHash) == seenChunkHashes.end()) {
                    if (storeRaw) {
                        std::vector<unsigned char> rawChunk(
                            buffer.begin(),
                            buffer.begin() + static_cast<std::size_t>(n)
                        );
                        const bool inserted = arc.insertChunkIfMissing(
                            chunkHash,
                            static_cast<std::size_t>(n),
                            rawChunk,
                            "raw"
                        );
                        if (inserted) {
                            ++stats.uniqueChunksInserted;
                        }
                    } else {
                        std::vector<unsigned char> chunkCopy(
                            buffer.begin(),
                            buffer.begin() + static_cast<std::size_t>(n)
                        );
                        PendingCompressedChunk job;
                        job.hash = chunkHash;
                        job.rawSize = static_cast<std::size_t>(n);
                        job.compFuture = std::async(
                            std::launch::async,
                            [data = std::move(chunkCopy)]() mutable {
                                return Compactador::zstdCompress(data.data(), data.size(), 1);
                            }
                        );
                        pendingCompressed.push_back(std::move(job));
                        if (pendingCompressed.size() >= maxCompressInFlight) {
                            flushOneCompressed();
                        }
                    }

                    seenChunkHashes.insert(chunkHash);
                }

                pendingChunks.push_back({
                    chunkIndex,
                    chunkHash,
                    static_cast<std::size_t>(n)
                });

                ++chunkIndex;
            }
            while (!pendingCompressed.empty()) {
                flushOneCompressed();
            }

            in.close();

            // Revalidação anti-race condition
            qec.clear();
            const auto size2 = fs::file_size(fullPath, qec);
            if (qec) {
                arc.deleteFileRecord(fileId);
                ++stats.warnings;
                continue;
            }
            const auto mtimeRaw2 = fs::last_write_time(fullPath, qec);
            if (qec) {
                arc.deleteFileRecord(fileId);
                ++stats.warnings;
                continue;
            }
            const auto mtime2 = fileTimeToUnixSeconds(mtimeRaw2);

            if (size1 != size2 || mtime1 != mtime2) {
                // Arquivo mudou durante leitura
                arc.deleteFileRecord(fileId);
                ++stats.warnings;
                continue;
            }

            // Inserção bulk dos file_chunks
            arc.addFileChunksBulk(fileId, pendingChunks);

            ++stats.added;
            maybePrintProgress(fullPath);
        }

        arc.commit();

        maybePrintProgress(sourceRoot, true);
        std::cout << "Backup finalizado. Arquivos processados: "
                  << (stats.added + stats.reused) << "\n";

    } catch (...) {
        arc.rollback();
        throw;
    }

    return stats;
}

BackupStats ScanEngine::backupFolderToKply(const fs::path& sourceRoot,
                                           const fs::path& archivePath,
                                           const std::string& label) {
    const fs::path sourceAbs = fs::absolute(sourceRoot).lexically_normal();
    if (sourceAbs != fs::path("/home")) {
        throw std::runtime_error(
            "Origem permitida neste build: /home (recebido: " + sourceAbs.string() + ")"
        );
    }

    const fs::path archiveAbs = fs::absolute(archivePath).lexically_normal();
    std::error_code ec;
    if (!archiveAbs.parent_path().empty()) {
        fs::create_directories(archiveAbs.parent_path(), ec);
        if (ec) {
            throw std::runtime_error("Falha criando pasta do arquivo KPLY: " + ec.message());
        }
    }

    StorageArchive arc(archiveAbs);
    return runBackup(arc, sourceAbs, archiveAbs, label);
}

} // namespace keeply

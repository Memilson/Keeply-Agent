#include "keeply.hpp"

#include <fstream>
#include <system_error>

namespace keeply {

void RestoreEngine::restoreFile(const fs::path& archivePath,
                                sqlite3_int64 snapshotId,
                                const std::string& relPath,
                                const fs::path& outRoot) {
    const std::string rp = normalizeRelPath(relPath);
    if (!isSafeRelativePath(rp)) {
        throw std::runtime_error("Path relativo inseguro.");
    }

    StorageArchive arc(archivePath);

    auto found = arc.findFileBySnapshotAndPath(snapshotId, rp);
    if (!found) {
        throw std::runtime_error("Arquivo nao encontrado no snapshot: " + rp);
    }

    sqlite3_int64 fileId = found->first;
    (void)found->second; // size (não usado no momento)

    fs::path target = fs::absolute(outRoot) / fs::path(rp);

    std::error_code ec;
    fs::create_directories(target.parent_path(), ec);

    std::ofstream out(target, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Falha ao abrir destino de restore: " + target.string());
    }

    auto chunks = arc.loadFileChunks(fileId);
    for (const auto& c : chunks) {
        if (c.blob.size() != c.compSize) {
            throw std::runtime_error("Blob de chunk invalido/corrompido.");
        }
        std::vector<unsigned char> raw;
        if (c.algo == "raw") {
            if (c.blob.size() != c.rawSize) {
                throw std::runtime_error("Chunk raw corrompido (tamanho invalido).");
            }
            raw = c.blob;
        } else if (c.algo == "zstd") {
            raw = Compactador::zstdDecompress(c.blob.data(), c.compSize, c.rawSize);
        } else if (c.algo == "zlib") {
            raw = Compactador::zlibDecompress(c.blob.data(), c.compSize, c.rawSize);
        } else {
            throw std::runtime_error("Algoritmo de compressao nao suportado: " + c.algo);
        }
        out.write(reinterpret_cast<const char*>(raw.data()),
                  static_cast<std::streamsize>(raw.size()));

        if (!out) {
            throw std::runtime_error("Erro escrevendo arquivo restaurado.");
        }
    }

    out.close();
}

void RestoreEngine::restoreSnapshot(const fs::path& archivePath,
                                    sqlite3_int64 snapshotId,
                                    const fs::path& outRoot) {
    StorageArchive arc(archivePath);

    auto paths = arc.listSnapshotPaths(snapshotId);
    for (const auto& p : paths) {
        restoreFile(archivePath, snapshotId, p, outRoot);
    }
}

} // namespace keeply

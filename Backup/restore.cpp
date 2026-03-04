#include "keeply.hpp"

#include <fstream>
#include <system_error>
#include <vector>

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

    fs::path target = fs::absolute(outRoot) / fs::path(rp);

    std::error_code ec;
    fs::create_directories(target.parent_path(), ec);

    std::ofstream out(target, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Falha ao abrir destino de restore: " + target.string());
    }

    auto chunks = arc.loadFileChunks(fileId);
    
    // =========================================================================
    // OTIMIZAÇÃO ENTERPRISE: ZERO-ALLOCATION LOOP
    // Criamos o buffer de descompressão UMA VEZ fora do loop.
    // Ele estica até o tamanho do maior chunk e é reaproveitado, zerando o uso do 'new/malloc' na CPU.
    // =========================================================================
    std::vector<unsigned char> decompressBuffer;
    decompressBuffer.reserve(CHUNK_SIZE);

    for (const auto& c : chunks) {
        if (c.blob.size() != c.compSize) {
            throw std::runtime_error("Blob de chunk invalido/corrompido.");
        }

        if (c.algo == "raw") {
            if (c.blob.size() != c.rawSize) {
                throw std::runtime_error("Chunk raw corrompido (tamanho invalido).");
            }
            // Grava direto do blob
            out.write(reinterpret_cast<const char*>(c.blob.data()),
                      static_cast<std::streamsize>(c.rawSize));
        } else {
            // Descomprime aproveitando o buffer pré-alocado
            if (c.algo == "zstd") {
                Compactador::zstdDecompress(c.blob.data(), c.compSize, c.rawSize, decompressBuffer);
            } else if (c.algo == "zlib") {
                Compactador::zlibDecompress(c.blob.data(), c.compSize, c.rawSize, decompressBuffer);
            } else {
                throw std::runtime_error("Algoritmo de compressao nao suportado: " + c.algo);
            }
            
            // Grava o buffer reciclado no disco
            out.write(reinterpret_cast<const char*>(decompressBuffer.data()),
                      static_cast<std::streamsize>(decompressBuffer.size()));
        }

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
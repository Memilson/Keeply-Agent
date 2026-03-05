// =============================================================================
// restore.cpp  —  Keeply Restore Engine
//
// Correções aplicadas:
//  [FIX-8]  restoreSnapshot abria StorageArchive (SQLite + pack file) uma vez
//           POR ARQUIVO — 50.000 arquivos = 50.000 aberturas de banco.
//           Agora abre UMA VEZ e passa a instância para restoreFileFromArc().
//  [FIX-8b] restoreFile público mantido para restore de arquivo único,
//           mas internamente delega para restoreFileFromArc(arc, ...) para
//           evitar duplicação de código.
// =============================================================================

#include "keeply.hpp"

#include <fstream>
#include <system_error>
#include <vector>

namespace keeply {

// ---------------------------------------------------------------------------
// Implementação interna — recebe StorageArchive já aberto
// Chamado tanto por restoreFile (single) quanto por restoreSnapshot (batch).
// ---------------------------------------------------------------------------
static void restoreFileFromArc(StorageArchive& arc,
                                sqlite3_int64 snapshotId,
                                const std::string& relPath,
                                const fs::path& outRoot) {
    const std::string rp = normalizeRelPath(relPath);
    if (!isSafeRelativePath(rp))
        throw std::runtime_error("Path relativo inseguro: " + rp);

    auto found = arc.findFileBySnapshotAndPath(snapshotId, rp);
    if (!found)
        throw std::runtime_error("Arquivo nao encontrado no snapshot: " + rp);

    const sqlite3_int64 fileId = found->first;
    const fs::path target = fs::absolute(outRoot) / fs::path(rp);

    std::error_code ec;
    fs::create_directories(target.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretório de destino: " + ec.message());

    std::ofstream out(target, std::ios::binary);
    if (!out)
        throw std::runtime_error("Falha ao abrir destino de restore: " + target.string());

    auto chunks = arc.loadFileChunks(fileId);

    // Zero-allocation loop: buffer reaproveitado entre chunks do mesmo arquivo
    std::vector<unsigned char> decompressBuffer;
    decompressBuffer.reserve(CHUNK_SIZE);

    for (const auto& c : chunks) {
        if (c.blob.size() != c.compSize)
            throw std::runtime_error("Blob de chunk invalido/corrompido.");

        if (c.algo == "raw") {
            if (c.blob.size() != c.rawSize)
                throw std::runtime_error("Chunk raw corrompido (tamanho invalido).");
            out.write(reinterpret_cast<const char*>(c.blob.data()),
                      static_cast<std::streamsize>(c.rawSize));
        } else {
            if (c.algo == "zstd") {
                Compactador::zstdDecompress(c.blob.data(), c.compSize, c.rawSize,
                                            decompressBuffer);
            } else if (c.algo == "zlib") {
                Compactador::zlibDecompress(c.blob.data(), c.compSize, c.rawSize,
                                            decompressBuffer);
            } else {
                throw std::runtime_error(
                    "Algoritmo de compressao nao suportado: " + c.algo);
            }
            out.write(reinterpret_cast<const char*>(decompressBuffer.data()),
                      static_cast<std::streamsize>(decompressBuffer.size()));
        }

        if (!out)
            throw std::runtime_error("Erro escrevendo arquivo restaurado: " +
                                     target.string());
    }

    out.close();
}

// ---------------------------------------------------------------------------
// API pública — restore de arquivo único
// Abre o StorageArchive e delega para restoreFileFromArc.
// ---------------------------------------------------------------------------
void RestoreEngine::restoreFile(const fs::path& archivePath,
                                sqlite3_int64 snapshotId,
                                const std::string& relPath,
                                const fs::path& outRoot) {
    StorageArchive arc(archivePath);
    restoreFileFromArc(arc, snapshotId, relPath, outRoot);
}

// ---------------------------------------------------------------------------
// [FIX-8] API pública — restore de snapshot completo
//
// ANTES: abria StorageArchive dentro de restoreFile() → N abertas de DB
//   for (p : paths) restoreFile(archivePath, ...) // abre/fecha N vezes
//
// DEPOIS: abre StorageArchive UMA VEZ e reutiliza para todos os arquivos.
//   StorageArchive arc(archivePath);  // 1 abertura
//   for (p : paths) restoreFileFromArc(arc, ...)  // sem overhead de I/O
// ---------------------------------------------------------------------------
void RestoreEngine::restoreSnapshot(const fs::path& archivePath,
                                    sqlite3_int64 snapshotId,
                                    const fs::path& outRoot) {
    // [FIX-8] Uma única instância para todo o snapshot
    StorageArchive arc(archivePath);

    const auto paths = arc.listSnapshotPaths(snapshotId);
    for (const auto& p : paths) {
        restoreFileFromArc(arc, snapshotId, p, outRoot);
    }
}

} // namespace keeply

// =============================================================================
// verificacao_integridade.cpp  —  Verificação de Integridade do Arquivo Keeply
//
// Implementa o comando 'verify' que estava ausente no projeto.
// Verifica 4 camadas de integridade:
//
//  [V1] Consistência interna do SQLite (PRAGMA integrity_check)
//  [V2] Todos os pack_offsets do DB apontam para registros válidos no pack
//  [V3] Hash BLAKE3 de cada chunk no pack bate com o registrado no DB
//  [V4] Todos os file_chunks referenciam chunks existentes em chunks (FK)
//
// Uso:
//   VerifyEngine::verifyArchive(archivePath, verbose)
//   → retorna VerifyResult com lista de erros encontrados
// =============================================================================

#include "../keeply.hpp"
#include <blake3.h>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

namespace keeply {

namespace {

// Lê exatamente `len` bytes; retorna false em EOF/erro sem lançar exceção.
static bool tryReadExact(std::istream& is, void* data, std::size_t len) {
    if (len == 0) return true;
    is.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(len));
    return static_cast<std::size_t>(is.gcount()) == len;
}

// Calcula BLAKE3 de um buffer
static ChunkHash blake3Of(const unsigned char* data, std::size_t len) {
    ChunkHash out{};
    blake3_hasher h;
    blake3_hasher_init(&h);
    blake3_hasher_update(&h, data, len);
    blake3_hasher_finalize(&h, out.data(), out.size());
    return out;
}

static constexpr char kChunkPackFileMagic[8] = {'K','P','C','H','U','N','K','1'};
static constexpr char kChunkPackRecMagic[8]  = {'K','P','R','E','C','0','0','1'};

#pragma pack(push,1)
struct PackRecHeader {
    char          magic[8];
    std::uint64_t rawSize;
    std::uint64_t compSize;
    unsigned char hash[32];
    std::uint32_t algoLen;
    std::uint32_t version;
};
#pragma pack(pop)

} // anonymous namespace

// =============================================================================
// VerifyEngine::verifyArchive
// =============================================================================
VerifyResult VerifyEngine::verifyArchive(const fs::path& archivePath, bool verbose) {
    VerifyResult result;
    result.ok = true;

    auto fail = [&](const std::string& msg) {
        result.ok = false;
        result.errors.push_back(msg);
        if (verbose) std::cerr << "[ERRO] " << msg << "\n";
    };
    auto info = [&](const std::string& msg) {
        if (verbose) std::cout << "[INFO] " << msg << "\n";
    };

    // -------------------------------------------------------------------------
    // [V1] Integridade do SQLite
    // -------------------------------------------------------------------------
    info("V1: Verificando integridade do SQLite...");
    {
        sqlite3* rawDb = nullptr;
        if (sqlite3_open_v2_path(archivePath, &rawDb,
                            SQLITE_OPEN_READONLY, nullptr) != SQLITE_OK) {
            fail("Nao foi possivel abrir o banco SQLite: " +
                 std::string(sqlite3_errmsg(rawDb)));
            if (rawDb) sqlite3_close(rawDb);
            return result; // sem banco, não podemos continuar
        }

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(rawDb,
                "PRAGMA integrity_check;", -1, &stmt, nullptr) == SQLITE_OK) {
            bool first = true;
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char* txt = sqlite3_column_text(stmt, 0);
                std::string line = txt ? reinterpret_cast<const char*>(txt) : "";
                if (line != "ok") {
                    fail("SQLite integrity_check: " + line);
                } else if (first) {
                    info("SQLite: ok");
                }
                first = false;
            }
            sqlite3_finalize(stmt);
        } else {
            fail("Falha ao executar PRAGMA integrity_check.");
        }
        sqlite3_close(rawDb);
    }

    // -------------------------------------------------------------------------
    // Abre StorageArchive para as verificações seguintes
    // -------------------------------------------------------------------------
    StorageArchive arc(archivePath);
    fs::path packPath = archivePath;
    packPath.replace_extension(".klyp");

    if (!fs::exists(packPath)) {
        fail("Arquivo pack nao encontrado: " + packPath.string());
        return result;
    }

    std::ifstream pack(packPath, std::ios::binary);
    if (!pack) {
        fail("Falha ao abrir pack para leitura: " + packPath.string());
        return result;
    }

    // Valida magic do pack
    char fileMagic[8]{};
    if (!tryReadExact(pack, fileMagic, sizeof(fileMagic)) ||
        std::memcmp(fileMagic, kChunkPackFileMagic, sizeof(fileMagic)) != 0) {
        fail("Magic invalido no arquivo pack.");
        return result;
    }
    info("Magic do pack: OK");

    // -------------------------------------------------------------------------
    // [V2 + V3] Verifica cada chunk registrado no DB
    // -------------------------------------------------------------------------
    info("V2+V3: Verificando offsets e hashes dos chunks...");

    std::size_t chunksChecked = 0;
    std::size_t chunksOk      = 0;

    // Lê todos os chunks do DB
    {
        DB db(archivePath); // leitura direta para não usar begin()
        Stmt st(db.raw(),
            "SELECT chunk_hash, raw_size, comp_size, comp_algo, pack_offset "
            "FROM chunks ORDER BY pack_offset ASC;");

        std::vector<unsigned char> compBuf;

        while (st.stepRow()) {
            ++chunksChecked;

            // Lê metadados do DB
            const void* hashPtr   = sqlite3_column_blob(st.get(), 0);
            const int   hashBytes = sqlite3_column_bytes(st.get(), 0);
            const auto  rawSize   = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 1));
            const auto  compSize  = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 2));
            const std::string algo = [&]{
                const unsigned char* p = sqlite3_column_text(st.get(), 3);
                return p ? reinterpret_cast<const char*>(p) : std::string{};
            }();
            const sqlite3_int64 packOffset = sqlite3_column_int64(st.get(), 4);

            if (!hashPtr || hashBytes != static_cast<int>(ChunkHash{}.size())) {
                fail("Chunk com chunk_hash invalido no DB (offset=" +
                     std::to_string(packOffset) + ")");
                continue;
            }

            ChunkHash dbHash{};
            std::memcpy(dbHash.data(), hashPtr, dbHash.size());

            // [V2] Verifica se o offset aponta para um registro válido
            pack.clear();
            pack.seekg(static_cast<std::streamoff>(packOffset), std::ios::beg);
            if (!pack) {
                fail("Offset invalido no pack: " + std::to_string(packOffset));
                continue;
            }

            PackRecHeader hdr{};
            if (!tryReadExact(pack, &hdr, sizeof(hdr))) {
                fail("Leitura do header falhou no offset " + std::to_string(packOffset));
                continue;
            }
            if (std::memcmp(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic)) != 0) {
                fail("Magic de registro invalido no offset " + std::to_string(packOffset));
                continue;
            }
            if (hdr.rawSize  != rawSize) {
                fail("rawSize divergente no offset " + std::to_string(packOffset) +
                     " (DB=" + std::to_string(rawSize) +
                     " pack=" + std::to_string(hdr.rawSize) + ")");
                continue;
            }
            if (hdr.compSize != compSize) {
                fail("compSize divergente no offset " + std::to_string(packOffset));
                continue;
            }

            // Pula algoLen bytes (nome do algoritmo)
            if (hdr.algoLen > 64) {
                fail("algoLen suspeito no offset " + std::to_string(packOffset));
                continue;
            }
            pack.seekg(static_cast<std::streamoff>(hdr.algoLen), std::ios::cur);

            // Lê dados comprimidos
            compBuf.resize(compSize);
            if (!tryReadExact(pack, compBuf.data(), compSize)) {
                fail("Dados do chunk truncados no offset " + std::to_string(packOffset));
                continue;
            }

            // [V3] Descomprime e recalcula o hash BLAKE3
            std::vector<unsigned char> rawBuf;
            rawBuf.reserve(rawSize);
            bool decompOk = true;
            try {
                if (algo == "zstd") {
                    Compactador::zstdDecompress(compBuf.data(), compSize, rawSize, rawBuf);
                } else if (algo == "zlib") {
                    Compactador::zlibDecompress(compBuf.data(), compSize, rawSize, rawBuf);
                } else if (algo == "raw") {
                    rawBuf = compBuf;
                } else {
                    fail("Algoritmo desconhecido '" + algo + "' no offset " +
                         std::to_string(packOffset));
                    decompOk = false;
                }
            } catch (const std::exception& ex) {
                fail("Falha na descompressao no offset " + std::to_string(packOffset) +
                     ": " + ex.what());
                decompOk = false;
            }

            if (!decompOk) continue;

            const ChunkHash computedHash = blake3Of(rawBuf.data(), rawBuf.size());
            if (computedHash != dbHash) {
                fail("Hash BLAKE3 divergente no offset " + std::to_string(packOffset) +
                     " — dados corrompidos!");
                continue;
            }

            ++chunksOk;
        }
    }

    info("Chunks verificados: " + std::to_string(chunksChecked) +
         ", OK: " + std::to_string(chunksOk));

    if (chunksChecked != chunksOk) {
        // erros já registrados acima
    }

    // -------------------------------------------------------------------------
    // [V4] Referências de file_chunks → chunks (integridade referencial)
    // -------------------------------------------------------------------------
    info("V4: Verificando integridade referencial file_chunks→chunks...");
    {
        DB db(archivePath);
        // Conta file_chunks que apontam para chunk_hash inexistente
        Stmt st(db.raw(),
            "SELECT COUNT(*) FROM file_chunks fc "
            "LEFT JOIN chunks c ON c.chunk_hash = fc.chunk_hash "
            "WHERE c.chunk_hash IS NULL;");
        if (st.stepRow()) {
            const sqlite3_int64 orphans = sqlite3_column_int64(st.get(), 0);
            if (orphans > 0) {
                fail(std::to_string(orphans) +
                     " file_chunk(s) referenciam chunks inexistentes no DB.");
            } else {
                info("Integridade referencial: OK");
            }
        }
    }

    result.chunksChecked = chunksChecked;
    result.chunksOk      = chunksOk;

    if (result.ok)
        info("Verificacao concluida: ARQUIVO INTEGRO.");
    else
        std::cerr << "[FALHA] Verificacao encontrou " << result.errors.size()
                  << " problema(s).\n";

    return result;
}

} // namespace keeply

#include "fabrica_objetos_armazenamento.hpp"

#include <cstring>
#include <fstream>
#include <stdexcept>

namespace keeply {
namespace {

constexpr char kChunkPackFileMagic[8] = {'K','P','C','H','U','N','K','1'};
constexpr char kChunkPackRecMagic[8]  = {'K','P','R','E','C','0','0','1'};
constexpr std::size_t kMaxChunkRawSize = static_cast<std::size_t>(CHUNK_SIZE);
constexpr std::size_t kMaxChunkCompSize = (static_cast<std::size_t>(CHUNK_SIZE) * 2u) + 4096u;
constexpr std::uint32_t kMaxCompAlgoLen   = 64;

#pragma pack(push,1)
struct ChunkPackRecHeader {
    char          magic[8];
    std::uint64_t rawSize;
    std::uint64_t compSize;
    unsigned char hash[32];
    std::uint32_t algoLen;
    std::uint32_t version;
};
#pragma pack(pop)

void readExact(std::istream& is, void* data, std::size_t len) {
    if (len == 0) return;
    is.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(len));
    if (!is || static_cast<std::size_t>(is.gcount()) != len) {
        throw std::runtime_error("Leitura curta/corrompida no pack.");
    }
}

} // namespace

void StorageArchive::begin() {
    db_.exec("BEGIN IMMEDIATE;");
    backendOpaque_ = StorageObjectFactory::makeLocalBackendOpaque(path_);
    auto backend = StorageObjectFactory::asBackend(backendOpaque_);
    backend->beginSession();
}

void StorageArchive::commit() {
    auto backend = StorageObjectFactory::asBackend(backendOpaque_);
    if (backend) {
        backend->commitSession();
        backendOpaque_.reset();
    }
    db_.exec("COMMIT;");
}

void StorageArchive::rollback() {
    auto backend = StorageObjectFactory::asBackend(backendOpaque_);
    if (backend) {
        backend->rollbackSession();
        backendOpaque_.reset();
    }
    packIn_.reset();
    try { db_.exec("ROLLBACK;"); } catch (...) {}
}

std::vector<unsigned char> StorageArchive::readPackAt(
    sqlite3_int64 recordOffset,
    const ChunkHash& expectedHash,
    std::size_t expectedRawSize,
    std::size_t expectedCompSize,
    const std::string& expectedAlgo) const {
    const fs::path packPath = StorageObjectFactory::describeArchive(path_).packPath;
    if (!packIn_) {
        packIn_ = std::make_unique<std::ifstream>(packPath, std::ios::binary);
        if (!*packIn_) {
            throw std::runtime_error("Falha abrindo pack para leitura: " + packPath.string());
        }
        char magic[8]{};
        readExact(*packIn_, magic, sizeof(magic));
        if (std::memcmp(magic, kChunkPackFileMagic, sizeof(magic)) != 0) {
            throw std::runtime_error("Magic invalido no arquivo pack.");
        }
    }
    std::ifstream& in = *packIn_;
    in.clear();
    in.seekg(static_cast<std::streamoff>(recordOffset), std::ios::beg);
    if (!in) throw std::runtime_error("Offset invalido no pack de chunks.");

    ChunkPackRecHeader hdr{};
    readExact(in, &hdr, sizeof(hdr));
    if (std::memcmp(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic)) != 0) {
        throw std::runtime_error("Magic invalido no registro do pack.");
    }
    if (hdr.algoLen > kMaxCompAlgoLen) throw std::runtime_error("algoLen invalido no pack.");
    if (hdr.rawSize > kMaxChunkRawSize) throw std::runtime_error("rawSize invalido no pack.");
    if (hdr.compSize > kMaxChunkCompSize) throw std::runtime_error("compSize invalido no pack.");
    if (hdr.rawSize != expectedRawSize) throw std::runtime_error("rawSize divergente entre SQLite e pack.");
    if (hdr.compSize != expectedCompSize) throw std::runtime_error("compSize divergente entre SQLite e pack.");
    if (std::memcmp(hdr.hash, expectedHash.data(), expectedHash.size()) != 0) {
        throw std::runtime_error("hash divergente entre SQLite e pack.");
    }

    std::string algo(static_cast<std::size_t>(hdr.algoLen), '\0');
    if (hdr.algoLen > 0) readExact(in, algo.data(), hdr.algoLen);
    if (algo != expectedAlgo) throw std::runtime_error("comp_algo divergente entre SQLite e pack.");

    std::vector<unsigned char> comp(static_cast<std::size_t>(hdr.compSize));
    if (hdr.compSize > 0) readExact(in, comp.data(), hdr.compSize);
    return comp;
}

} // namespace keeply

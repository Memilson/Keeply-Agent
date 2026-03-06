#include "storage_backend.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <memory>
#include <stdexcept>

namespace keeply {
namespace {

constexpr char kChunkPackFileMagic[8] = {'K','P','C','H','U','N','K','1'};
constexpr char kChunkPackRecMagic[8]  = {'K','P','R','E','C','0','0','1'};
constexpr std::size_t   kMaxChunkRawSize  = static_cast<std::size_t>(CHUNK_SIZE);
constexpr std::size_t   kMaxChunkCompSize = (static_cast<std::size_t>(CHUNK_SIZE)*2u)+4096u;
constexpr std::uint32_t kMaxCompAlgoLen   = 64;
constexpr char kPackIdxMagic[8] = {'K','P','I','D','X','0','0','1'};

#pragma pack(push,1)
struct ChunkPackRecHeader {
    char          magic[8];
    std::uint64_t rawSize;
    std::uint64_t compSize;
    unsigned char hash[32];
    std::uint32_t algoLen;
    std::uint32_t version;
};

struct PackIndexEntry {
    unsigned char hash[32];
    std::uint64_t packOffset;
    std::uint64_t rawSize;
    std::uint64_t compSize;
    char          algo[16];
};
#pragma pack(pop)

void writeExact(std::ostream& os, const void* data, std::size_t len) {
    if (len == 0) return;
    os.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len));
    if (!os) throw std::runtime_error("Falha escrevendo pack.");
}

void readExact(std::istream& is, void* data, std::size_t len) {
    if (len == 0) return;
    is.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(len));
    if (!is || static_cast<std::size_t>(is.gcount()) != len) {
        throw std::runtime_error("Leitura curta/corrompida no pack.");
    }
}

class LocalPackBackend final : public StorageBackend {
    fs::path      packPath_;
    fs::path      idxPath_;
    std::fstream  packStream_;
    std::ofstream idxStream_;

    void verifyOrInitPack() {
        std::error_code ec;
        if (!packPath_.parent_path().empty()) {
            fs::create_directories(packPath_.parent_path(), ec);
        }
        if (!fs::exists(packPath_, ec) || fs::file_size(packPath_, ec) == 0) {
            std::ofstream out(packPath_, std::ios::binary | std::ios::trunc);
            writeExact(out, kChunkPackFileMagic, sizeof(kChunkPackFileMagic));
            out.flush();
            if (!out) throw std::runtime_error("Falha ao inicializar pack.");
            return;
        }
        std::ifstream in(packPath_, std::ios::binary);
        if (!in) throw std::runtime_error("Falha ao validar pack.");
        char magic[8]{};
        readExact(in, magic, sizeof(magic));
        if (std::memcmp(magic, kChunkPackFileMagic, sizeof(magic)) != 0) {
            throw std::runtime_error("Pack existente com magic invalido.");
        }
    }

    void verifyOrInitIndex() {
        std::error_code ec;
        if (!fs::exists(idxPath_, ec) || fs::file_size(idxPath_, ec) == 0) {
            std::ofstream out(idxPath_, std::ios::binary | std::ios::trunc);
            writeExact(out, kPackIdxMagic, sizeof(kPackIdxMagic));
            out.flush();
        }
        idxStream_.open(idxPath_, std::ios::binary | std::ios::app);
        if (!idxStream_) throw std::runtime_error("Falha ao abrir pack index.");
    }

public:
    explicit LocalPackBackend(const fs::path& dbPath)
        : packPath_(chunkPackPathFromArchive(dbPath))
        , idxPath_(chunkIndexPathFromArchive(dbPath)) {}

    void beginSession() override {
        verifyOrInitPack();
        verifyOrInitIndex();
        packStream_.open(packPath_, std::ios::binary | std::ios::in | std::ios::out);
        if (!packStream_) throw std::runtime_error("Falha ao abrir LocalPackBackend.");
        packStream_.seekp(0, std::ios::end);
        if (!packStream_) throw std::runtime_error("Falha seek no pack.");
    }

    sqlite3_int64 appendBlob(const ProcessedBlob& blob) override {
        packStream_.seekp(0, std::ios::end);
        const std::streamoff off = packStream_.tellp();
        if (off < 0) throw std::runtime_error("Falha tellp no pack.");

        ChunkPackRecHeader hdr{};
        std::memcpy(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic));
        hdr.rawSize = static_cast<std::uint64_t>(blob.rawSize);
        hdr.compSize = static_cast<std::uint64_t>(blob.data.size());
        std::memcpy(hdr.hash, blob.hash.data(), blob.hash.size());
        hdr.algoLen = static_cast<std::uint32_t>(blob.compAlgo.size());
        hdr.version = 1;
        writeExact(packStream_, &hdr, sizeof(hdr));
        if (!blob.compAlgo.empty()) writeExact(packStream_, blob.compAlgo.data(), blob.compAlgo.size());
        if (!blob.data.empty()) writeExact(packStream_, blob.data.data(), blob.data.size());

        PackIndexEntry ie{};
        std::memcpy(ie.hash, blob.hash.data(), blob.hash.size());
        ie.packOffset = static_cast<std::uint64_t>(off);
        ie.rawSize = blob.rawSize;
        ie.compSize = blob.data.size();
        const std::size_t copyLen = std::min(blob.compAlgo.size(), sizeof(ie.algo) - 1);
        std::memcpy(ie.algo, blob.compAlgo.data(), copyLen);
        ie.algo[copyLen] = '\0';
        writeExact(idxStream_, &ie, sizeof(ie));
        return static_cast<sqlite3_int64>(off);
    }

    void commitSession() override {
        if (packStream_.is_open()) {
            packStream_.flush();
            packStream_.close();
        }
        if (idxStream_.is_open()) {
            idxStream_.flush();
            idxStream_.close();
        }
    }

    void rollbackSession() override {
        if (packStream_.is_open()) packStream_.close();
        if (idxStream_.is_open()) idxStream_.close();
    }
};

} // namespace

fs::path chunkPackPathFromArchive(const fs::path& dbPath) {
    fs::path out = dbPath;
    out.replace_extension(".klyp");
    return out;
}

fs::path chunkIndexPathFromArchive(const fs::path& dbPath) {
    return fs::path(chunkPackPathFromArchive(dbPath).string() + ".idx");
}

std::shared_ptr<StorageBackend> asBackend(const std::shared_ptr<void>& opaque) {
    return std::static_pointer_cast<StorageBackend>(opaque);
}

std::shared_ptr<void> makeLocalStorageBackendOpaque(const fs::path& dbPath) {
    return std::static_pointer_cast<void>(std::make_shared<LocalPackBackend>(dbPath));
}

void StorageArchive::begin() {
    db_.exec("BEGIN IMMEDIATE;");
    auto backend = asBackend(makeLocalStorageBackendOpaque(path_));
    backend->beginSession();
    backendOpaque_ = backend;
}

void StorageArchive::commit() {
    auto backend = asBackend(backendOpaque_);
    if (backend) {
        backend->commitSession();
        backendOpaque_.reset();
    }
    db_.exec("COMMIT;");
}

void StorageArchive::rollback() {
    auto backend = asBackend(backendOpaque_);
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
    const fs::path packPath = chunkPackPathFromArchive(path_);
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

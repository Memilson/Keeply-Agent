#include "fabrica_objetos_armazenamento.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>

namespace keeply {
namespace {

constexpr char kChunkPackFileMagic[8] = {'K','P','C','H','U','N','K','1'};
constexpr char kChunkPackRecMagic[8]  = {'K','P','R','E','C','0','0','1'};
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

class LocalPackBackend final : public StorageBackend {
    fs::path packPath_;
    fs::path idxPath_;
    std::fstream packStream_;
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
        in.read(magic, static_cast<std::streamsize>(sizeof(magic)));
        if (!in || std::memcmp(magic, kChunkPackFileMagic, sizeof(magic)) != 0) {
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
    explicit LocalPackBackend(const fs::path& archivePath) {
        const auto files = StorageObjectFactory::describeArchive(archivePath);
        packPath_ = files.packPath;
        idxPath_ = files.indexPath;
    }

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

StorageObjectFactory::ArchiveFiles StorageObjectFactory::describeArchive(const fs::path& archivePath) {
    ArchiveFiles files;
    files.archivePath = archivePath;
    files.packPath = archivePath;
    files.packPath.replace_extension(".klyp");
    files.indexPath = fs::path(files.packPath.string() + ".idx");
    return files;
}

void StorageObjectFactory::ensureArchiveDirectory(const fs::path& archivePath) {
    if (archivePath.parent_path().empty()) return;
    std::error_code ec;
    fs::create_directories(archivePath.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando pasta do arquivo: " + ec.message());
}

std::shared_ptr<StorageBackend> StorageObjectFactory::asBackend(const std::shared_ptr<void>& opaque) {
    return std::static_pointer_cast<StorageBackend>(opaque);
}

std::shared_ptr<void> StorageObjectFactory::makeLocalBackendOpaque(const fs::path& archivePath) {
    return std::static_pointer_cast<void>(std::make_shared<LocalPackBackend>(archivePath));
}

StorageArchive::CloudBundleFile StorageObjectFactory::makeBundleFile(const fs::path& path,
                                                                     const std::string& bundleId,
                                                                     const std::string& uploadName,
                                                                     const std::string& contentType,
                                                                     bool manifest,
                                                                     bool blobPart) {
    StorageArchive::CloudBundleFile file;
    file.path = path;
    file.uploadName = uploadName;
    file.objectKey = bundleId + "/" + uploadName;
    file.contentType = contentType;
    file.manifest = manifest;
    file.blobPart = blobPart;
    return file;
}

StorageArchive::CloudBundleFile StorageObjectFactory::makeMetaFile(const fs::path& bundleDir,
                                                                   const std::string& bundleId) {
    const std::string uploadName = bundleId + "-meta.kipy";
    return makeBundleFile(bundleDir / uploadName, bundleId, uploadName, "application/octet-stream", false, false);
}

StorageArchive::CloudBundleFile StorageObjectFactory::makeIndexFile(const fs::path& bundleDir,
                                                                    const std::string& bundleId) {
    const std::string uploadName = bundleId + "-pack.klyp.idx";
    return makeBundleFile(bundleDir / uploadName, bundleId, uploadName, "application/octet-stream", false, false);
}

StorageArchive::CloudBundleFile StorageObjectFactory::makeManifestFile(const fs::path& bundleDir,
                                                                       const std::string& bundleId) {
    const std::string uploadName = bundleId + "-manifest.json";
    return makeBundleFile(bundleDir / uploadName, bundleId, uploadName, "application/json", true, false);
}

StorageArchive::CloudBundleFile StorageObjectFactory::makeBlobFile(const std::string& bundleId,
                                                                   std::size_t partIndex,
                                                                   std::uintmax_t partSize) {
    std::ostringstream name;
    name << bundleId << "-blob-" << std::setw(5) << std::setfill('0') << (partIndex + 1) << ".bin";
    auto file = makeBundleFile({}, bundleId, name.str(), "application/octet-stream", false, true);
    file.size = partSize;
    return file;
}

} // namespace keeply

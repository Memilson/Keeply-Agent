#include "../keeply.hpp"
#include "../storage_backend.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>

namespace keeply {
namespace {

constexpr std::uint64_t kDefaultCloudBlobSize = 16ull * 1024ull * 1024ull;

std::string jsonEscape(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) out += '?';
                else out += c;
        }
    }
    return out;
}

std::string makeCloudBundleId() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
    std::random_device rd;
    std::ostringstream oss;
    oss << "bundle-" << ms << "-" << std::hex << std::setw(8) << std::setfill('0')
        << static_cast<unsigned int>(rd());
    return oss.str();
}

std::uintmax_t copyFileToBundle(const fs::path& src, const fs::path& dst) {
    std::error_code ec;
    fs::create_directories(dst.parent_path(), ec);
    ec.clear();
    fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
    if (ec) throw std::runtime_error("Falha copiando arquivo para bundle cloud: " + ec.message());
    return fs::file_size(dst, ec);
}

std::size_t computeBlobPartCount(const fs::path& srcPack, std::uint64_t blobMaxBytes) {
    std::error_code ec;
    const std::uintmax_t packSize = fs::file_size(srcPack, ec);
    if (ec) throw std::runtime_error("Falha lendo tamanho do pack para exportacao cloud: " + ec.message());
    if (packSize == 0) return 0;
    return static_cast<std::size_t>((packSize + blobMaxBytes - 1) / blobMaxBytes);
}

std::vector<StorageArchive::CloudBundleFile> buildPlannedBlobFiles(
    const fs::path& srcPack,
    const std::string& bundleId,
    std::uint64_t blobMaxBytes) {
    std::error_code ec;
    const std::uintmax_t packSize = fs::file_size(srcPack, ec);
    if (ec) throw std::runtime_error("Falha lendo tamanho do pack para exportacao cloud: " + ec.message());
    std::vector<StorageArchive::CloudBundleFile> files;
    const std::size_t partCount = computeBlobPartCount(srcPack, blobMaxBytes);
    files.reserve(partCount);
    for (std::size_t partIndex = 0; partIndex < partCount; ++partIndex) {
        const std::uintmax_t offset = static_cast<std::uintmax_t>(partIndex) * blobMaxBytes;
        const std::uintmax_t remaining = packSize > offset ? (packSize - offset) : 0;
        const std::uintmax_t partSize = std::min<std::uintmax_t>(blobMaxBytes, remaining);
        std::ostringstream name;
        name << bundleId << "-blob-" << std::setw(5) << std::setfill('0') << (partIndex + 1) << ".bin";
        StorageArchive::CloudBundleFile file;
        file.uploadName = name.str();
        file.objectKey = bundleId + "/" + file.uploadName;
        file.contentType = "application/octet-stream";
        file.size = partSize;
        file.blobPart = true;
        files.push_back(std::move(file));
    }
    return files;
}

void writeBundleManifest(const fs::path& manifestPath,
                         const std::string& bundleId,
                         const fs::path& archivePath,
                         const std::vector<StorageArchive::CloudBundleFile>& files,
                         std::uint64_t blobMaxBytes) {
    std::ofstream out(manifestPath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Falha criando manifest do bundle cloud.");
    out << "{";
    out << "\"type\":\"keeply.cloud.bundle\",";
    out << "\"bundleId\":\"" << jsonEscape(bundleId) << "\",";
    out << "\"archive\":\"" << jsonEscape(archivePath.filename().string()) << "\",";
    out << "\"blobMaxBytes\":" << blobMaxBytes << ",";
    out << "\"createdAt\":\"" << jsonEscape(nowIsoLocal()) << "\",";
    out << "\"files\":[";
    for (std::size_t i = 0; i < files.size(); ++i) {
        const auto& file = files[i];
        if (i) out << ",";
        out << "{";
        out << "\"name\":\"" << jsonEscape(file.uploadName) << "\",";
        out << "\"objectKey\":\"" << jsonEscape(file.objectKey) << "\",";
        out << "\"contentType\":\"" << jsonEscape(file.contentType) << "\",";
        out << "\"size\":" << file.size << ",";
        out << "\"manifest\":" << (file.manifest ? "true" : "false") << ",";
        out << "\"blobPart\":" << (file.blobPart ? "true" : "false");
        out << "}";
    }
    out << "]}";
    out.flush();
    if (!out) throw std::runtime_error("Falha escrevendo manifest do bundle cloud.");
}

} // namespace

StorageArchive::CloudBundleExport StorageArchive::exportCloudBundle(const fs::path& tempRoot,
                                                                    std::uint64_t blobMaxBytes) const {
    const std::uint64_t effectiveBlobSize = blobMaxBytes == 0 ? kDefaultCloudBlobSize : blobMaxBytes;
    if (effectiveBlobSize < (1024ull * 1024ull)) {
        throw std::runtime_error("blobMaxBytes invalido para exportacao cloud.");
    }

    const fs::path archivePath = path_;
    const fs::path packPath = chunkPackPathFromArchive(path_);
    const fs::path idxPath = chunkIndexPathFromArchive(path_);
    if (!fs::exists(archivePath)) throw std::runtime_error("Arquivo .kipy nao encontrado para exportacao cloud.");
    if (!fs::exists(packPath)) throw std::runtime_error("Arquivo .klyp nao encontrado para exportacao cloud.");

    const std::string bundleId = makeCloudBundleId();
    const fs::path bundleDir = tempRoot / bundleId;
    std::error_code ec;
    fs::remove_all(bundleDir, ec);
    ec.clear();
    fs::create_directories(bundleDir, ec);
    if (ec) throw std::runtime_error("Falha criando pasta temporaria do bundle cloud: " + ec.message());

    CloudBundleExport out;
    out.bundleId = bundleId;
    out.rootDir = bundleDir;
    out.packPath = packPath;
    out.blobMaxBytes = effectiveBlobSize;

    CloudBundleFile dbFile;
    dbFile.path = bundleDir / (bundleId + "-meta.kipy");
    dbFile.uploadName = dbFile.path.filename().string();
    dbFile.objectKey = bundleId + "/" + dbFile.uploadName;
    dbFile.contentType = "application/octet-stream";
    dbFile.size = copyFileToBundle(archivePath, dbFile.path);
    out.files.push_back(dbFile);

    if (fs::exists(idxPath)) {
        CloudBundleFile idxFile;
        idxFile.path = bundleDir / (bundleId + "-pack.klyp.idx");
        idxFile.uploadName = idxFile.path.filename().string();
        idxFile.objectKey = bundleId + "/" + idxFile.uploadName;
        idxFile.contentType = "application/octet-stream";
        idxFile.size = copyFileToBundle(idxPath, idxFile.path);
        out.files.push_back(idxFile);
    }

    auto blobFiles = buildPlannedBlobFiles(packPath, bundleId, effectiveBlobSize);
    out.blobPartCount = blobFiles.size();
    out.files.insert(out.files.end(), blobFiles.begin(), blobFiles.end());

    CloudBundleFile manifestFile;
    manifestFile.path = bundleDir / (bundleId + "-manifest.json");
    manifestFile.uploadName = manifestFile.path.filename().string();
    manifestFile.objectKey = bundleId + "/" + manifestFile.uploadName;
    manifestFile.contentType = "application/json";
    manifestFile.manifest = true;
    writeBundleManifest(manifestFile.path, bundleId, archivePath, out.files, effectiveBlobSize);
    manifestFile.size = fs::file_size(manifestFile.path, ec);
    if (ec) manifestFile.size = 0;
    out.files.push_back(manifestFile);

    return out;
}

StorageArchive::CloudBundleFile StorageArchive::materializeCloudBundleBlob(const CloudBundleExport& bundle,
                                                                           std::size_t partIndex) const {
    if (partIndex >= bundle.blobPartCount) {
        throw std::runtime_error("Indice de blob cloud fora do intervalo.");
    }
    const std::uint64_t blobSize = bundle.blobMaxBytes == 0 ? kDefaultCloudBlobSize : bundle.blobMaxBytes;
    std::ifstream in(bundle.packPath, std::ios::binary);
    if (!in) throw std::runtime_error("Falha abrindo pack para gerar blob cloud.");
    const std::uint64_t offset = static_cast<std::uint64_t>(partIndex) * blobSize;
    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!in) throw std::runtime_error("Falha posicionando pack para gerar blob cloud.");

    const auto plannedFiles = buildPlannedBlobFiles(bundle.packPath, bundle.bundleId, blobSize);
    const auto& planned = plannedFiles.at(partIndex);
    const fs::path outPath = bundle.rootDir / planned.uploadName;
    std::vector<char> buffer(static_cast<std::size_t>(planned.size));
    if (!buffer.empty()) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        if (static_cast<std::size_t>(in.gcount()) != buffer.size()) {
            throw std::runtime_error("Leitura curta ao materializar blob cloud.");
        }
    }
    std::ofstream out(outPath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Falha criando blob cloud temporario.");
    if (!buffer.empty()) out.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    out.flush();
    if (!out) throw std::runtime_error("Falha finalizando blob cloud temporario.");

    CloudBundleFile file = planned;
    file.path = outPath;
    return file;
}

} // namespace keeply

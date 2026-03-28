#pragma once
#include "arquivo_armazenamento.hpp"
#include <memory>

namespace keeply {

struct ArchiveStoragePaths {
    fs::path archivePath;
    fs::path packPath;
    fs::path indexPath;
};

struct ProcessedBlob {
    ChunkHash   hash;
    std::size_t rawSize;
    const std::vector<unsigned char>& data;
    std::string compAlgo;
};

class StorageBackend {
public:
    virtual ~StorageBackend() = default;
    virtual void beginSession() = 0;
    virtual sqlite3_int64 appendBlob(const ProcessedBlob& blob) = 0;
    virtual void commitSession() = 0;
    virtual void rollbackSession() = 0;
};

class StorageCloudExporter {
public:
    virtual ~StorageCloudExporter() = default;
    virtual StorageArchive::CloudBundleExport exportBundle(const fs::path& tempRoot,
                                                           std::uint64_t blobMaxBytes) const = 0;
    virtual StorageArchive::CloudBundleFile materializeBlob(const StorageArchive::CloudBundleExport& bundle,
                                                            std::size_t partIndex) const = 0;
};

ArchiveStoragePaths describeArchiveStorage(const fs::path& archivePath);
void ensureArchiveStorageParent(const fs::path& archivePath);
std::shared_ptr<StorageBackend> makeLocalStorageBackend(const fs::path& archivePath);
std::unique_ptr<StorageCloudExporter> makeLocalCloudExporter(const fs::path& archivePath);

} 

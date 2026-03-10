#pragma once

#include "storage_backend.hpp"

namespace keeply {

class StorageObjectFactory {
public:
    struct ArchiveFiles {
        fs::path archivePath;
        fs::path packPath;
        fs::path indexPath;
    };

    static ArchiveFiles describeArchive(const fs::path& archivePath);
    static void ensureArchiveDirectory(const fs::path& archivePath);

    static std::shared_ptr<StorageBackend> asBackend(const std::shared_ptr<void>& opaque);
    static std::shared_ptr<void> makeLocalBackendOpaque(const fs::path& archivePath);

    static StorageArchive::CloudBundleFile makeBundleFile(const fs::path& path,
                                                          const std::string& bundleId,
                                                          const std::string& uploadName,
                                                          const std::string& contentType,
                                                          bool manifest,
                                                          bool blobPart);

    static StorageArchive::CloudBundleFile makeMetaFile(const fs::path& bundleDir,
                                                        const std::string& bundleId);
    static StorageArchive::CloudBundleFile makeIndexFile(const fs::path& bundleDir,
                                                         const std::string& bundleId);
    static StorageArchive::CloudBundleFile makeManifestFile(const fs::path& bundleDir,
                                                            const std::string& bundleId);
    static StorageArchive::CloudBundleFile makeBlobFile(const std::string& bundleId,
                                                        std::size_t partIndex,
                                                        std::uintmax_t partSize);
};

} // namespace keeply

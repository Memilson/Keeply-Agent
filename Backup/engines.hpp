#pragma once






#include "../Core/tipos.hpp"

namespace keeply {

class ScanEngine {
public:
    static BackupStats backupFolderToKply(const fs::path& sourceRoot,
                                          const fs::path& archivePath,
                                          const std::string& label,
                                          const std::function<void(const BackupProgress&)>& progressCallback = {});
    static std::vector<std::string> listAvailableSourceRoots();
};

class RestoreEngine {
public:
    static void restoreFile(const fs::path& archivePath,
                            sqlite3_int64 snapshotId,
                            const std::string& relPath,
                            const fs::path& outRoot);

    static void restoreSnapshot(const fs::path& archivePath,
                                sqlite3_int64 snapshotId,
                                const fs::path& outRoot);
};

struct VerifyResult {
    bool ok = true;
    std::vector<std::string> errors;
    std::size_t chunksChecked = 0;
    std::size_t chunksOk = 0;
};

class VerifyEngine {
public:
    static VerifyResult verifyArchive(const fs::path& archivePath, bool verbose = false);
};

} 

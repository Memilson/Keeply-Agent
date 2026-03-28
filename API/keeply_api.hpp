#pragma once

#include "../Core/tipos.hpp"

namespace keeply {

struct ScanScopeState {
    std::string id = "home";
    std::string label = "Home";
    std::string requestedPath;
    std::string resolvedPath = pathToUtf8(defaultSourceRootPath());
};

struct AppState {
    std::string source = pathToUtf8(defaultSourceRootPath());
    std::string archive = pathToUtf8(defaultArchivePath());
    std::string restoreRoot = pathToUtf8(defaultRestoreRootPath());
    ScanScopeState scanScope{};
    bool archiveSplitEnabled = false;
    std::uint64_t archiveSplitMaxBytes = 0;
};

class KeeplyApi {
public:
    KeeplyApi();

    const AppState& state() const;
    void setSource(const std::string& source);
    void setScanScope(const std::string& scopeId);
    void setArchive(const std::string& archive);
    void setRestoreRoot(const std::string& restoreRoot);
    void setArchiveSplitMaxBytes(std::uint64_t maxBytes);
    void disableArchiveSplit();
    bool archiveExists() const;

    BackupStats runBackup(const std::string& label,
                          const std::function<void(const BackupProgress&)>& progressCallback = {});
    std::vector<SnapshotRow> listSnapshots();
    std::vector<ChangeEntry> diffSnapshots(const std::string& olderSnapshotInput,
                                           const std::string& newerSnapshotInput);
    std::vector<ChangeEntry> diffLatestVsPrevious();
    std::vector<std::string> listSnapshotPaths(const std::string& snapshotInput);

    void restoreFile(const std::string& snapshotInput,
                     const std::string& relPath,
                     const std::optional<fs::path>& outRootOpt);

    void restoreSnapshot(const std::string& snapshotInput,
                         const std::optional<fs::path>& outRootOpt);

private:
    AppState state_;
};

}

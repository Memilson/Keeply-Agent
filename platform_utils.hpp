#pragma once

#include <cstdio>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

namespace keeply {

namespace fs = std::filesystem;

enum class KnownDirectory {
    Home,
    Desktop,
    Documents,
    Downloads,
    Pictures,
    Music,
    Videos,
    LocalData,
    StateData,
    CacheData,
    Temp
};

fs::path homeDirectoryPath();
std::optional<fs::path> knownDirectoryPath(KnownDirectory dir);

fs::path defaultKeeplyDataDir();
fs::path defaultKeeplyStateDir();
fs::path defaultKeeplyTempDir();
fs::path defaultCloudBundleExportRoot();
fs::path defaultSourceRootPath();
fs::path defaultArchivePath();
fs::path defaultRestoreRootPath();

std::vector<fs::path> defaultSystemExcludedRoots();
bool isFilesystemRootPath(const fs::path& path);

std::string pathToUtf8(const fs::path& path);
fs::path pathFromUtf8(const std::string& utf8Path);

FILE* fopenPath(const fs::path& path, const char* mode);
int sqlite3_open_path(const fs::path& path, sqlite3** db);
int sqlite3_open_v2_path(const fs::path& path, sqlite3** db, int flags, const char* vfs);

} // namespace keeply

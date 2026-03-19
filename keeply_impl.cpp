#include "keeply.hpp"

#include <cstdlib>
#include <fstream>
#include <system_error>

#ifdef _WIN32
#include <shlobj.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace keeply {

namespace detail {

// trimAscii removida — usar keeply::trim() de utilitarios_backup.hpp
inline std::string trimAscii(const std::string& value) { return keeply::trim(value); }

inline std::optional<std::string> envValue(const char* key) {
    const char* value = std::getenv(key);
    if (!value || !*value) return std::nullopt;
    return std::string(value);
}

inline fs::path normalizedOrEmpty(const fs::path& path) {
    if (path.empty()) return {};
    std::error_code ec;
    const fs::path absolute = path.is_absolute() ? path : fs::absolute(path, ec);
    if (ec) return path.lexically_normal();
    return absolute.lexically_normal();
}

inline void appendUnique(std::vector<fs::path>& out, const fs::path& path) {
    if (path.empty()) return;
    const fs::path normalized = normalizedOrEmpty(path);
    for (const auto& current : out) {
        if (normalized == current) return;
    }
    out.push_back(normalized);
}

#ifdef _WIN32
inline std::optional<fs::path> knownFolderPath(REFKNOWNFOLDERID folderId) {
    PWSTR raw = nullptr;
    const HRESULT hr = SHGetKnownFolderPath(folderId, KF_FLAG_DEFAULT, nullptr, &raw);
    if (FAILED(hr) || !raw) return std::nullopt;
    const fs::path path(raw);
    CoTaskMemFree(raw);
    return normalizedOrEmpty(path);
}

inline std::wstring widenAscii(const char* text) {
    std::wstring out;
    if (!text) return out;
    while (*text) out.push_back(static_cast<wchar_t>(*text++));
    return out;
}
#endif

#if !defined(_WIN32)
inline std::optional<std::string> readXdgUserDir(const fs::path& homeDir, const std::string& key) {
    const fs::path configPath = homeDir / ".config" / "user-dirs.dirs";
    std::ifstream input(configPath);
    if (!input) return std::nullopt;

    const std::string prefix = key + "=";
    std::string line;
    while (std::getline(input, line)) {
        line = trimAscii(line);
        if (line.empty() || line.front() == '#') continue;
        if (line.rfind(prefix, 0) != 0) continue;

        std::string value = trimAscii(line.substr(prefix.size()));
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }

        const std::string homeToken = "$HOME";
        const auto homePos = value.find(homeToken);
        if (homePos != std::string::npos) {
            value.replace(homePos, homeToken.size(), homeDir.string());
        }
        return value;
    }

    return std::nullopt;
}
#endif

inline std::optional<fs::path> homeFromEnvironment() {
#ifdef _WIN32
    if (const auto fromProfile = envValue("USERPROFILE")) return normalizedOrEmpty(pathFromUtf8(*fromProfile));
    const auto drive = envValue("HOMEDRIVE");
    const auto homePath = envValue("HOMEPATH");
    if (drive && homePath) return normalizedOrEmpty(pathFromUtf8(*drive + *homePath));
#else
    if (const auto fromHome = envValue("HOME")) return normalizedOrEmpty(pathFromUtf8(*fromHome));
#endif
    return std::nullopt;
}

inline fs::path fallbackCurrentPath() {
    std::error_code ec;
    const fs::path cwd = fs::current_path(ec);
    return ec ? fs::path(".") : cwd.lexically_normal();
}

} // namespace detail

std::string pathToUtf8(const fs::path& path) {
    return path.u8string();
}

fs::path pathFromUtf8(const std::string& utf8Path) {
    return fs::u8path(utf8Path);
}

std::optional<fs::path> knownDirectoryPath(KnownDirectory dir) {
#ifdef _WIN32
    switch (dir) {
        case KnownDirectory::Home:
            return detail::knownFolderPath(FOLDERID_Profile);
        case KnownDirectory::Desktop:
            return detail::knownFolderPath(FOLDERID_Desktop);
        case KnownDirectory::Documents:
            return detail::knownFolderPath(FOLDERID_Documents);
        case KnownDirectory::Downloads:
            return detail::knownFolderPath(FOLDERID_Downloads);
        case KnownDirectory::Pictures:
            return detail::knownFolderPath(FOLDERID_Pictures);
        case KnownDirectory::Music:
            return detail::knownFolderPath(FOLDERID_Music);
        case KnownDirectory::Videos:
            return detail::knownFolderPath(FOLDERID_Videos);
        case KnownDirectory::LocalData:
        case KnownDirectory::StateData:
        case KnownDirectory::CacheData:
            return detail::knownFolderPath(FOLDERID_LocalAppData);
        case KnownDirectory::Temp:
            if (const auto temp = detail::envValue("TEMP")) return detail::normalizedOrEmpty(pathFromUtf8(*temp));
            if (const auto tmp = detail::envValue("TMP")) return detail::normalizedOrEmpty(pathFromUtf8(*tmp));
            return std::nullopt;
    }
    return std::nullopt;
#else
    const fs::path home = homeDirectoryPath();
    switch (dir) {
        case KnownDirectory::Home:
            if (const auto value = detail::homeFromEnvironment()) return *value;
            return detail::fallbackCurrentPath();
        case KnownDirectory::Desktop:
            if (const auto value = detail::readXdgUserDir(home, "XDG_DESKTOP_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Desktop");
        case KnownDirectory::Documents:
            if (const auto value = detail::readXdgUserDir(home, "XDG_DOCUMENTS_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Documents");
        case KnownDirectory::Downloads:
            if (const auto value = detail::readXdgUserDir(home, "XDG_DOWNLOAD_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Downloads");
        case KnownDirectory::Pictures:
            if (const auto value = detail::readXdgUserDir(home, "XDG_PICTURES_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Pictures");
        case KnownDirectory::Music:
            if (const auto value = detail::readXdgUserDir(home, "XDG_MUSIC_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Music");
        case KnownDirectory::Videos:
            if (const auto value = detail::readXdgUserDir(home, "XDG_VIDEOS_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / "Videos");
        case KnownDirectory::LocalData:
#if defined(__APPLE__)
            return detail::normalizedOrEmpty(home / "Library" / "Application Support");
#else
            if (const auto value = detail::envValue("XDG_DATA_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / ".local" / "share");
#endif
        case KnownDirectory::StateData:
#if defined(__APPLE__)
            return detail::normalizedOrEmpty(home / "Library" / "Application Support");
#else
            if (const auto value = detail::envValue("XDG_STATE_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / ".local" / "state");
#endif
        case KnownDirectory::CacheData:
#if defined(__APPLE__)
            return detail::normalizedOrEmpty(home / "Library" / "Caches");
#else
            if (const auto value = detail::envValue("XDG_CACHE_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(home / ".cache");
#endif
        case KnownDirectory::Temp:
            if (const auto value = detail::envValue("TMPDIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return std::nullopt;
    }
    return std::nullopt;
#endif
}

fs::path homeDirectoryPath() {
#ifdef _WIN32
    if (const auto home = knownDirectoryPath(KnownDirectory::Home)) return *home;
#endif
    if (const auto home = detail::homeFromEnvironment()) return *home;
    return detail::fallbackCurrentPath();
}

fs::path defaultKeeplyDataDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::LocalData)) {
#if defined(__APPLE__)
        return *base / "Keeply";
#else
        return *base / "keeply";
#endif
    }
    return detail::fallbackCurrentPath() / ".keeply";
}

fs::path defaultKeeplyStateDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::StateData)) {
#if defined(__APPLE__)
        return *base / "Keeply" / "state";
#else
        return *base / "keeply";
#endif
    }
    return defaultKeeplyDataDir();
}

fs::path defaultKeeplyTempDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::Temp)) {
        return *base / "keeply";
    }
    std::error_code ec;
    const fs::path temp = fs::temp_directory_path(ec);
    if (!ec) return temp / "keeply";
    return defaultKeeplyDataDir() / "tmp";
}

fs::path defaultCloudBundleExportRoot() {
    return defaultKeeplyTempDir() / "cloud_bundle_export";
}

fs::path defaultSourceRootPath() {
    return homeDirectoryPath();
}

fs::path defaultArchivePath() {
    return defaultKeeplyDataDir() / "keeply.kipy";
}

fs::path defaultRestoreRootPath() {
    return defaultKeeplyDataDir() / "restore";
}

std::vector<fs::path> defaultSystemExcludedRoots() {
    std::vector<fs::path> out;
#ifdef _WIN32
    if (const auto systemRoot = detail::envValue("SystemRoot")) detail::appendUnique(out, pathFromUtf8(*systemRoot));
    if (const auto windir = detail::envValue("WINDIR")) detail::appendUnique(out, pathFromUtf8(*windir));
    if (const auto programFiles = detail::envValue("ProgramFiles")) detail::appendUnique(out, pathFromUtf8(*programFiles));
    if (const auto programFilesX86 = detail::envValue("ProgramFiles(x86)")) detail::appendUnique(out, pathFromUtf8(*programFilesX86));
    if (const auto programData = detail::envValue("ProgramData")) detail::appendUnique(out, pathFromUtf8(*programData));
    detail::appendUnique(out, defaultKeeplyTempDir());
    if (const auto home = homeDirectoryPath(); !home.root_path().empty()) {
        detail::appendUnique(out, home.root_path() / "$Recycle.Bin");
        detail::appendUnique(out, home.root_path() / "System Volume Information");
    }
#else
    detail::appendUnique(out, "/proc");
    detail::appendUnique(out, "/sys");
    detail::appendUnique(out, "/dev");
    detail::appendUnique(out, "/run");
    detail::appendUnique(out, "/tmp");
    detail::appendUnique(out, "/mnt");
    detail::appendUnique(out, "/media");
    detail::appendUnique(out, "/lost+found");
    detail::appendUnique(out, "/var/run");
    detail::appendUnique(out, "/var/tmp");
#endif
    return out;
}

bool isFilesystemRootPath(const fs::path& path) {
    if (path.empty()) return false;
    const fs::path normalized = detail::normalizedOrEmpty(path);
    const fs::path root = normalized.root_path();
    return !root.empty() && normalized == root;
}

FILE* fopenPath(const fs::path& path, const char* mode) {
#ifdef _WIN32
    const std::wstring wideMode = detail::widenAscii(mode);
    return _wfopen(path.wstring().c_str(), wideMode.c_str());
#else
    return std::fopen(path.c_str(), mode);
#endif
}

int sqlite3_open_path(const fs::path& path, sqlite3** db) {
    return sqlite3_open(pathToUtf8(path).c_str(), db);
}

int sqlite3_open_v2_path(const fs::path& path, sqlite3** db, int flags, const char* vfs) {
    return sqlite3_open_v2(pathToUtf8(path).c_str(), db, flags, vfs);
}

} // namespace keeply

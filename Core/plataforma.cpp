#include "../keeply.hpp"
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <pwd.h>
#include <system_error>
#include <unistd.h>
namespace keeply {
namespace detail {
inline std::string trimAscii(const std::string& value) { return keeply::trim(value); }
inline std::optional<std::string> envValue(const char* key) {
    const char* value = std::getenv(key);
    if (!value || !*value) return std::nullopt;
    return std::string(value);}
inline fs::path normalizedOrEmpty(const fs::path& path) {
    if (path.empty()) return {};
    std::error_code ec;
    const fs::path absolute = path.is_absolute() ? path : fs::absolute(path, ec);
    if (ec) return path.lexically_normal();
    return absolute.lexically_normal();}
inline void appendUnique(std::vector<fs::path>& out, const fs::path& path) {
    if (path.empty()) return;
    const fs::path normalized = normalizedOrEmpty(path);
    for (const auto& current : out) {
        if (normalized == current) return;}
    out.push_back(normalized);}
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
            value = value.substr(1, value.size() - 2);}
        const std::string homeToken = "$HOME";
        const auto homePos = value.find(homeToken);
        if (homePos != std::string::npos) {
            value.replace(homePos, homeToken.size(), homeDir.string());}
        return value;}
    return std::nullopt;}
inline std::optional<fs::path> homeFromEnvironment() {
    if (const auto fromHome = envValue("HOME")) return normalizedOrEmpty(pathFromUtf8(*fromHome));
    return std::nullopt;}
inline fs::path fallbackCurrentPath();
inline std::optional<fs::path> homeFromPasswdUid(uid_t uid) {
    passwd* entry = ::getpwuid(uid);
    if (!entry || !entry->pw_dir || !*entry->pw_dir) return std::nullopt;
    return normalizedOrEmpty(pathFromUtf8(entry->pw_dir));}
inline std::optional<fs::path> homeFromPasswdUser(const std::string& user) {
    if (user.empty() || user == "root") return std::nullopt;
    passwd* entry = ::getpwnam(user.c_str());
    if (!entry || !entry->pw_dir || !*entry->pw_dir) return std::nullopt;
    return normalizedOrEmpty(pathFromUtf8(entry->pw_dir));}
inline std::optional<fs::path> homeFromUidEnv(const char* key) {
    const auto raw = envValue(key);
    if (!raw || raw->empty()) return std::nullopt;
    char* end = nullptr;
    errno = 0;
    const unsigned long parsed = std::strtoul(raw->c_str(), &end, 10);
    if (errno != 0 || !end || *end != '\0') return std::nullopt;
    return homeFromPasswdUid(static_cast<uid_t>(parsed));}
inline std::optional<fs::path> homeFromCurrentPath() {
    std::error_code ec;
    const fs::path cwd = fs::current_path(ec);
    if (ec || cwd.empty()) return std::nullopt;
    std::vector<std::string> parts;
    for (const auto& part : normalizedOrEmpty(cwd)) parts.push_back(part.string());
    if (parts.size() >= 3 && parts[1] == "home" && !parts[2].empty() && parts[2] != "root") {
        return normalizedOrEmpty(fs::path("/home") / parts[2]);}
    return std::nullopt;}
inline fs::path sourceHomeDirectoryPath() {
    if (::geteuid() == 0) {
        if (const auto fromUid = homeFromUidEnv("SUDO_UID")) return *fromUid;
        if (const auto fromUser = envValue("SUDO_USER")) {
            if (const auto resolved = homeFromPasswdUser(*fromUser)) return *resolved;}
        if (const auto fromUid = homeFromUidEnv("PKEXEC_UID")) return *fromUid;
        if (const auto fromPath = homeFromCurrentPath()) return *fromPath;
        if (const auto fromUser = envValue("LOGNAME")) {
            if (const auto resolved = homeFromPasswdUser(*fromUser)) return *resolved;}
        if (const auto fromUser = envValue("USER")) {
            if (const auto resolved = homeFromPasswdUser(*fromUser)) return *resolved;}}
    if (const auto fromHome = homeFromEnvironment()) return *fromHome;
    if (const auto fromPasswd = homeFromPasswdUid(::geteuid())) return *fromPasswd;
    return fallbackCurrentPath();}
inline fs::path fallbackCurrentPath() {
    std::error_code ec;
    const fs::path cwd = fs::current_path(ec);
    return ec ? fs::path(".") : cwd.lexically_normal();}}
std::string pathToUtf8(const fs::path& path) {
    return path.u8string();}
fs::path pathFromUtf8(const std::string& utf8Path) {
    return fs::u8path(utf8Path);}
std::optional<fs::path> knownDirectoryPath(KnownDirectory dir) {
    const fs::path sourceHome = detail::sourceHomeDirectoryPath();
    const fs::path runtimeHome = homeDirectoryPath();
    switch (dir) {
        case KnownDirectory::Home:
            return sourceHome;
        case KnownDirectory::Desktop:
            if (const auto value = detail::readXdgUserDir(sourceHome, "XDG_DESKTOP_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(sourceHome / "Desktop");
        case KnownDirectory::Documents:
            if (const auto value = detail::readXdgUserDir(sourceHome, "XDG_DOCUMENTS_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(sourceHome / "Documents");
        case KnownDirectory::Downloads:
            if (const auto value = detail::readXdgUserDir(sourceHome, "XDG_DOWNLOAD_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(sourceHome / "Downloads");
        case KnownDirectory::Pictures:
            if (const auto value = detail::readXdgUserDir(sourceHome, "XDG_PICTURES_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(sourceHome / "Pictures");
        case KnownDirectory::Music:
            if (const auto value = detail::readXdgUserDir(sourceHome, "XDG_MUSIC_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(sourceHome / "Music");
        case KnownDirectory::Videos:
            if (const auto value = detail::readXdgUserDir(sourceHome, "XDG_VIDEOS_DIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(sourceHome / "Videos");
        case KnownDirectory::LocalData:
            if (const auto value = detail::envValue("XDG_DATA_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(runtimeHome / ".local" / "share");
        case KnownDirectory::StateData:
            if (const auto value = detail::envValue("XDG_STATE_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(runtimeHome / ".local" / "state");
        case KnownDirectory::CacheData:
            if (const auto value = detail::envValue("XDG_CACHE_HOME")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return detail::normalizedOrEmpty(runtimeHome / ".cache");
        case KnownDirectory::Temp:
            if (const auto value = detail::envValue("TMPDIR")) return detail::normalizedOrEmpty(pathFromUtf8(*value));
            return std::nullopt;}
    return std::nullopt;}
fs::path homeDirectoryPath() {
    if (const auto home = detail::homeFromEnvironment()) return *home;
    if (const auto home = detail::homeFromPasswdUid(::geteuid())) return *home;
    return detail::fallbackCurrentPath();}
fs::path defaultKeeplyDataDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::LocalData)) {
        return *base / "keeply";
    }
    return detail::fallbackCurrentPath() / ".keeply";}
fs::path defaultKeeplyStateDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::StateData)) {
        return *base / "keeply";
    }
    return defaultKeeplyDataDir();}
fs::path defaultKeeplyTempDir() {
    if (const auto base = knownDirectoryPath(KnownDirectory::Temp)) {
        return *base / "keeply";}
    std::error_code ec;
    const fs::path temp = fs::temp_directory_path(ec);
    if (!ec) return temp / "keeply";
    return defaultKeeplyDataDir() / "tmp";}
fs::path defaultCloudBundleExportRoot() {
    return defaultKeeplyTempDir() / "cloud_bundle_export";}
fs::path defaultSourceRootPath() {
    if (const auto home = knownDirectoryPath(KnownDirectory::Home)) return *home;
    return detail::sourceHomeDirectoryPath();}
fs::path defaultArchivePath() {
    return defaultKeeplyDataDir() / "keeply.kipy";}
fs::path defaultRestoreRootPath() {
    return defaultKeeplyDataDir() / "restore";}
std::vector<fs::path> defaultSystemExcludedRoots() {
    std::vector<fs::path> out;
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
    return out;}
bool isFilesystemRootPath(const fs::path& path) {
    if (path.empty()) return false;
    const fs::path normalized = detail::normalizedOrEmpty(path);
    const fs::path root = normalized.root_path();
    return !root.empty() && normalized == root;}
FILE* fopenPath(const fs::path& path, const char* mode) {
    return std::fopen(path.c_str(), mode);}
int sqlite3_open_path(const fs::path& path, sqlite3** db) {
    return sqlite3_open(pathToUtf8(path).c_str(), db);}
int sqlite3_open_v2_path(const fs::path& path, sqlite3** db, int flags, const char* vfs) {
    return sqlite3_open_v2(pathToUtf8(path).c_str(), db, flags, vfs);}
fs::path normalizeAbsolutePath(const fs::path& p) {
    if (p.empty()) return {};
    std::error_code ec;
    fs::path abs = p.is_absolute() ? p : fs::absolute(p, ec);
    if (ec) return p.lexically_normal();
    return abs.lexically_normal();}
bool sourceRootUsesSystemExclusionPolicy(const fs::path& sourceRoot) {
    const fs::path normalized = normalizeAbsolutePath(sourceRoot);
    if (isFilesystemRootPath(normalized)) return true;
    const fs::path home = defaultSourceRootPath();
    return normalized == normalizeAbsolutePath(home);}
bool isExcludedBySystemPolicy(const fs::path& sourceRoot, const fs::path& candidatePath) {
    if (!sourceRootUsesSystemExclusionPolicy(sourceRoot)) return false;
    const auto excluded = defaultSystemExcludedRoots();
    const fs::path normalized = normalizeAbsolutePath(candidatePath);
    for (const auto& excl : excluded) {
        if (normalized == excl) return true;
        auto eit = excl.begin();
        auto nit = normalized.begin();
        bool match = true;
        for (; eit != excl.end(); ++eit, ++nit) {
            if (nit == normalized.end() || *nit != *eit) { match = false; break; }}
        if (match) return true;}
    return false;}}

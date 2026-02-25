#include "keeply.hpp"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <system_error>
namespace keeply {
namespace {
fs::path normalizeAbsPathForConfig(const fs::path& p) {
    std::error_code ec;
    fs::path abs = p.is_absolute() ? p : fs::absolute(p, ec);
    if (ec) {
        throw std::runtime_error("Caminho invalido: " + p.string() + " | " + ec.message());
    }
    return abs.lexically_normal();
}
void validateSourceRootIsHome(const fs::path& sourcePath) {
    static const fs::path kAllowedSource = fs::path("/home");
    const fs::path normalized = normalizeAbsPathForConfig(sourcePath);
    if (normalized != kAllowedSource) {
        throw std::runtime_error(
            "Origem permitida neste build: /home (recebido: " + normalized.string() + ")"
        );
    }
}
}
std::string trim(const std::string& s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    const auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);}
std::string nowIsoLocal() {
    std::time_t t = std::time(nullptr);
    std::tm tmv{};
#if defined(_WIN32)
    localtime_s(&tmv, &t);
#else
    localtime_r(&t, &tmv);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tmv, "%Y-%m-%d %H:%M:%S");
    return oss.str();}
long long fileTimeToUnixSeconds(const fs::file_time_type& ftp) {
    using namespace std::chrono;
    auto sctp = time_point_cast<system_clock::duration>(
        ftp - fs::file_time_type::clock::now() + system_clock::now()
    );
    return duration_cast<seconds>(sctp.time_since_epoch()).count();}
std::string hexOfBytes(const unsigned char* bytes, std::size_t n) {
    static const char* h = "0123456789abcdef";
    std::string out;
    out.resize(n * 2);
    for (std::size_t i = 0; i < n; ++i) {
        out[i * 2]     = h[(bytes[i] >> 4) & 0x0F];
        out[i * 2 + 1] = h[bytes[i] & 0x0F];
    }
    return out;}
std::string normalizeRelPath(std::string s) {
    std::replace(s.begin(), s.end(), '\\', '/');
    while (!s.empty() && s.front() == '/') s.erase(s.begin());
    return s;}
bool isSafeRelativePath(const std::string& p) {
    if (p.empty()) return false;
    if (p.find(':') != std::string::npos) return false;
    fs::path pp(p);
    if (pp.is_absolute()) return false;
    for (const auto& part : pp) {
        if (part == "..") return false;
    }
    return true;}
void ensureDefaults() {
    std::error_code ec;
    fs::create_directories(DEFAULT_ARCHIVE_PATH.parent_path(), ec);
    ec.clear();
    fs::create_directories(DEFAULT_RESTORE_ROOT, ec);}
KeeplyApi::KeeplyApi() {
    ensureDefaults();}
const AppState& KeeplyApi::state() const {
    return state_;}
void KeeplyApi::setSource(const std::string& source) {
    const std::string s = trim(source);
    if (s.empty()) throw std::runtime_error("Origem nao pode ser vazia.");
    validateSourceRootIsHome(fs::path(s));
    state_.source = normalizeAbsPathForConfig(fs::path(s)).string();}
void KeeplyApi::setArchive(const std::string& archive) {
    const std::string s = trim(archive);
    if (s.empty()) throw std::runtime_error("Arquivo KPLY nao pode ser vazio.");
    state_.archive = s;
    std::error_code ec;
    fs::path p(s);
    if (!p.parent_path().empty()) fs::create_directories(p.parent_path(), ec);}
void KeeplyApi::setRestoreRoot(const std::string& restoreRoot) {
    const std::string s = trim(restoreRoot);
    if (s.empty()) throw std::runtime_error("Destino de restore nao pode ser vazio.");
    state_.restoreRoot = s;
    std::error_code ec;
    fs::create_directories(fs::path(s), ec);}
bool KeeplyApi::archiveExists() const {
    return fs::exists(state_.archive);}
BackupStats KeeplyApi::runBackup(const std::string& label) {
    return ScanEngine::backupFolderToKply(state_.source, state_.archive, label);}
std::vector<SnapshotRow> KeeplyApi::listSnapshots() {
    StorageArchive arc(state_.archive);
    return arc.listSnapshots();}
std::vector<ChangeEntry> KeeplyApi::diffSnapshots(const std::string& olderSnapshotInput,
                                                  const std::string& newerSnapshotInput) {
    StorageArchive arc(state_.archive);
    sqlite3_int64 older = arc.resolveSnapshotId(olderSnapshotInput);
    sqlite3_int64 newer = arc.resolveSnapshotId(newerSnapshotInput);
    return arc.diffSnapshots(older, newer);}
std::vector<ChangeEntry> KeeplyApi::diffLatestVsPrevious() {
    StorageArchive arc(state_.archive);
    return arc.diffLatestVsPrevious();}
std::vector<std::string> KeeplyApi::listSnapshotPaths(const std::string& snapshotInput) {
    StorageArchive arc(state_.archive);
    sqlite3_int64 sid = arc.resolveSnapshotId(snapshotInput);
    return arc.listSnapshotPaths(sid);}
void KeeplyApi::restoreFile(const std::string& snapshotInput,
                            const std::string& relPath,
                            const std::optional<fs::path>& outRootOpt) {
    fs::path outRoot = outRootOpt.value_or(fs::path(state_.restoreRoot));
    std::error_code ec;
    fs::create_directories(outRoot, ec);
    StorageArchive arc(state_.archive);
    sqlite3_int64 sid = arc.resolveSnapshotId(snapshotInput);
    RestoreEngine::restoreFile(state_.archive, sid, relPath, outRoot);}
void KeeplyApi::restoreSnapshot(const std::string& snapshotInput,
                                const std::optional<fs::path>& outRootOpt) {
    fs::path outRoot = outRootOpt.value_or(fs::path(state_.restoreRoot));
    std::error_code ec;
    fs::create_directories(outRoot, ec);
    StorageArchive arc(state_.archive);
    sqlite3_int64 sid = arc.resolveSnapshotId(snapshotInput);
    RestoreEngine::restoreSnapshot(state_.archive, sid, outRoot);}}

#include "../keeply.hpp"
#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <system_error>
#include <vector>

#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#endif
namespace keeply {
namespace {
fs::path detectHomeDir() {
    return normalizeAbsolutePath(homeDirectoryPath());
}

bool isPathWithin(const fs::path& baseInput, const fs::path& candidateInput) {
    const fs::path base = normalizeAbsolutePath(baseInput);
    const fs::path candidate = normalizeAbsolutePath(candidateInput);
    auto bit = base.begin();
    auto cit = candidate.begin();
    for (; bit != base.end(); ++bit, ++cit) {
        if (cit == candidate.end() || *bit != *cit) return false;
    }
    return true;
}

void validateSourceRootAllowed(const fs::path& sourcePath) {
    const fs::path normalized = normalizeAbsolutePath(sourcePath);
    std::error_code ec;
    if (!fs::exists(normalized, ec) || ec) {
        throw std::runtime_error("Origem nao encontrada: " + normalized.string());
    }
    ec.clear();
    if (!fs::is_directory(normalized, ec) || ec) {
        throw std::runtime_error("Origem precisa ser um diretorio: " + normalized.string());
    }
#if defined(__unix__) || defined(__APPLE__)
    if (::access(normalized.c_str(), R_OK | X_OK) != 0) {
        throw std::runtime_error(
            "Sem permissao para acessar a origem configurada: " + normalized.string()
        );
    }
#endif
}

std::optional<std::string> readXdgDirValue(const fs::path& homeDir, const std::string& key) {
#if defined(_WIN32)
    (void)homeDir;
    (void)key;
    return std::nullopt;
#else
    const fs::path configPath = homeDir / ".config" / "user-dirs.dirs";
    std::ifstream input(configPath);
    if (!input) return std::nullopt;
    std::string line;
    const std::string prefix = key + "=";
    while (std::getline(input, line)) {
        line = trim(line);
        if (line.empty() || line.front() == '#') continue;
        if (line.rfind(prefix, 0) != 0) continue;
        std::string value = trim(line.substr(prefix.size()));
        if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
            value = value.substr(1, value.size() - 2);
        }
        const std::string homeToken = "$HOME";
        const std::size_t homePos = value.find(homeToken);
        if (homePos != std::string::npos) {
            value.replace(homePos, homeToken.size(), homeDir.string());
        }
        return value;
    }
    return std::nullopt;
#endif
}

fs::path pickExistingPath(const std::vector<fs::path>& candidates) {
    std::error_code ec;
    for (const auto& candidate : candidates) {
        ec.clear();
        if (candidate.empty()) continue;
        if (fs::exists(candidate, ec) && fs::is_directory(candidate, ec) && !ec) {
            return normalizeAbsolutePath(candidate);
        }
    }
    return fs::path();
}

fs::path resolveKnownScopePath(const std::string& scopeId) {
    struct ScopeCandidate {
        const char* id;
        KnownDirectory dir;
        std::vector<const char*> fallbackNames;
    };
    static const std::vector<ScopeCandidate> kCandidates = {
        {"home", KnownDirectory::Home, {}},
        {"documents", KnownDirectory::Documents, {"Documents", "Documentos"}},
        {"desktop", KnownDirectory::Desktop, {"Desktop", "Area de Trabalho"}},
        {"downloads", KnownDirectory::Downloads, {"Downloads"}},
        {"pictures", KnownDirectory::Pictures, {"Pictures", "Imagens"}},
        {"music", KnownDirectory::Music, {"Music", "Musicas", "Música"}},
        {"videos", KnownDirectory::Videos, {"Videos", "Vídeos"}}
    };

    for (const auto& scope : kCandidates) {
        if (scopeId != scope.id) continue;
        if (auto knownDir = knownDirectoryPath(scope.dir); knownDir && !knownDir->empty()) {
            std::error_code ec;
            if (fs::exists(*knownDir, ec) && fs::is_directory(*knownDir, ec) && !ec) {
                return normalizeAbsolutePath(*knownDir);
            }
        }
        std::vector<fs::path> fallbacks;
        const fs::path homeDir = detectHomeDir();
        for (const char* name : scope.fallbackNames) {
            fallbacks.emplace_back(homeDir / name);
        }
        const fs::path resolved = pickExistingPath(fallbacks);
        if (!resolved.empty()) return resolved;
        throw std::runtime_error(
            "Nao foi possivel localizar a pasta do escopo '" + scopeId + "' dentro de " +
            detectHomeDir().string()
        );
    }

    throw std::runtime_error("Escopo de scan nao suportado: " + scopeId);
}

ScanScopeState resolveScanScope(const std::string& rawScopeId) {
    std::string scopeId = trim(rawScopeId);
    std::transform(scopeId.begin(), scopeId.end(), scopeId.begin(), [](unsigned char c) {
        return (char)std::tolower(c);
    });
    if (scopeId.empty()) scopeId = "home";

    ScanScopeState scope;
    scope.id = scopeId;
    scope.requestedPath.clear();
    if (scopeId == "home") scope.label = "Home";
    else if (scopeId == "documents") scope.label = "Documents";
    else if (scopeId == "desktop") scope.label = "Desktop";
    else if (scopeId == "downloads") scope.label = "Downloads";
    else if (scopeId == "pictures") scope.label = "Pictures";
    else if (scopeId == "music") scope.label = "Music";
    else if (scopeId == "videos") scope.label = "Videos";
    else throw std::runtime_error("Escopo de scan nao suportado: " + scopeId);
    scope.resolvedPath = pathToUtf8(resolveKnownScopePath(scopeId));
    return scope;
}

ScanScopeState resolveCustomScope(const fs::path& sourcePath) {
    const fs::path normalized = normalizeAbsolutePath(sourcePath);
    validateSourceRootAllowed(normalized);
    ScanScopeState scope;
    scope.id = "custom";
    scope.label = "Custom";
    scope.requestedPath = pathToUtf8(normalized);
    scope.resolvedPath = pathToUtf8(normalized);
    return scope;
}
}
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
    fs::create_directories(defaultArchivePath().parent_path(), ec);
    ec.clear();
    fs::create_directories(defaultRestoreRootPath(), ec);
}
KeeplyApi::KeeplyApi() {
    state_.scanScope = resolveScanScope("home");
    state_.source = state_.scanScope.resolvedPath;
    ensureDefaults();}
const AppState& KeeplyApi::state() const {
    return state_;}
void KeeplyApi::setSource(const std::string& source) {
    const std::string s = trim(source);
    if (s.empty()) throw std::runtime_error("Origem nao pode ser vazia.");
    state_.scanScope = resolveCustomScope(pathFromUtf8(s));
    state_.source = state_.scanScope.resolvedPath;}
void KeeplyApi::setScanScope(const std::string& scopeId) {
    state_.scanScope = resolveScanScope(scopeId);
    state_.source = state_.scanScope.resolvedPath;}
void KeeplyApi::setArchive(const std::string& archive) {
    const std::string s = trim(archive);
    if (s.empty()) throw std::runtime_error("Arquivo KPLY nao pode ser vazio.");
    const fs::path archivePath = normalizeAbsolutePath(pathFromUtf8(s));
    state_.archive = pathToUtf8(archivePath);
    std::error_code ec;
    if (!archivePath.parent_path().empty()) {
        fs::create_directories(archivePath.parent_path(), ec);
        if (ec) {
            throw std::runtime_error("Falha criando diretorio do arquivo KPLY: " + ec.message());
        }
    }}
void KeeplyApi::setRestoreRoot(const std::string& restoreRoot) {
    const std::string s = trim(restoreRoot);
    if (s.empty()) throw std::runtime_error("Destino de restore nao pode ser vazio.");
    const fs::path restorePath = normalizeAbsolutePath(pathFromUtf8(s));
    state_.restoreRoot = pathToUtf8(restorePath);
    std::error_code ec;
    fs::create_directories(restorePath, ec);
    if (ec) {
        throw std::runtime_error("Falha criando diretorio de restore: " + ec.message());
    }}
void KeeplyApi::setArchiveSplitMaxBytes(std::uint64_t maxBytes) {
    if (maxBytes == 0) {
        disableArchiveSplit();
        return;
    }

    constexpr std::uint64_t kMinSplitBytes = 64ull * 1024ull * 1024ull;
    if (maxBytes < kMinSplitBytes) {
        throw std::runtime_error("Tamanho de divisao muito pequeno (minimo: 64 MiB).");
    }
    state_.archiveSplitEnabled = true;
    state_.archiveSplitMaxBytes = maxBytes;
}
void KeeplyApi::disableArchiveSplit() {
    state_.archiveSplitEnabled = false;
    state_.archiveSplitMaxBytes = 0;
}
bool KeeplyApi::archiveExists() const {
    return fs::exists(pathFromUtf8(state_.archive));}
BackupStats KeeplyApi::runBackup(const std::string& label,
                                 const std::function<void(const BackupProgress&)>& progressCallback) {

    return ScanEngine::backupFolderToKply(pathFromUtf8(state_.source), pathFromUtf8(state_.archive), label, progressCallback);}
std::vector<SnapshotRow> KeeplyApi::listSnapshots() {
    StorageArchive arc(pathFromUtf8(state_.archive));
    return arc.listSnapshots();}
std::vector<ChangeEntry> KeeplyApi::diffSnapshots(const std::string& olderSnapshotInput,
                                                  const std::string& newerSnapshotInput) {
    StorageArchive arc(pathFromUtf8(state_.archive));
    sqlite3_int64 older = arc.resolveSnapshotId(olderSnapshotInput);
    sqlite3_int64 newer = arc.resolveSnapshotId(newerSnapshotInput);
    return arc.diffSnapshots(older, newer);}
std::vector<ChangeEntry> KeeplyApi::diffLatestVsPrevious() {
    StorageArchive arc(pathFromUtf8(state_.archive));
    return arc.diffLatestVsPrevious();}
std::vector<std::string> KeeplyApi::listSnapshotPaths(const std::string& snapshotInput) {
    StorageArchive arc(pathFromUtf8(state_.archive));
    sqlite3_int64 sid = arc.resolveSnapshotId(snapshotInput);
    return arc.listSnapshotPaths(sid);}
void KeeplyApi::restoreFile(const std::string& snapshotInput,
                            const std::string& relPath,
                            const std::optional<fs::path>& outRootOpt) {
    fs::path outRoot = outRootOpt.value_or(pathFromUtf8(state_.restoreRoot));
    std::error_code ec;
    fs::create_directories(outRoot, ec);
    StorageArchive arc(pathFromUtf8(state_.archive));
    sqlite3_int64 sid = arc.resolveSnapshotId(snapshotInput);
    RestoreEngine::restoreFile(pathFromUtf8(state_.archive), sid, relPath, outRoot);}
void KeeplyApi::restoreSnapshot(const std::string& snapshotInput,
                                const std::optional<fs::path>& outRootOpt) {
    fs::path outRoot = outRootOpt.value_or(pathFromUtf8(state_.restoreRoot));
    std::error_code ec;
    fs::create_directories(outRoot, ec);
    StorageArchive arc(pathFromUtf8(state_.archive));
    sqlite3_int64 sid = arc.resolveSnapshotId(snapshotInput);
    RestoreEngine::restoreSnapshot(pathFromUtf8(state_.archive), sid, outRoot);}}

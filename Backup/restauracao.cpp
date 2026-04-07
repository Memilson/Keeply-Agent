#include "../keeply.hpp"
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <system_error>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
namespace keeply {
namespace {
bool restoreFsyncEnabled() {
    const char* raw = std::getenv("KEEPLY_RESTORE_FSYNC");
    if (!raw) return false;
    std::string value = trim(raw);
    for (char& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return value == "1" || value == "true" || value == "yes" || value == "on";}
void fsyncRestoredFile(const fs::path& target) {
    const int fd = ::open(target.c_str(), O_RDWR);
    if (fd < 0) throw std::runtime_error("Falha abrindo arquivo para fsync: " + target.string());
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) throw std::runtime_error("Falha no fsync do arquivo restaurado: " + target.string());}
void applyRestoredMtime(const fs::path& target, sqlite3_int64 unixSecs) {
    std::error_code ec;
    const auto nowFileClock = fs::file_time_type::clock::now();
    const auto nowUnix = fileTimeToUnixSeconds(nowFileClock);
    const auto desired = nowFileClock + std::chrono::seconds(unixSecs - nowUnix);
    fs::last_write_time(target, desired, ec);
    if (ec) {
        throw std::runtime_error("Falha aplicando mtime no arquivo restaurado: " + ec.message());}}
class ArchiveReadSession {
    StorageArchive& arc_;
    bool active_ = false;
public:
    explicit ArchiveReadSession(StorageArchive& arc) : arc_(arc) {
        arc_.beginRead();
        active_ = true;}
    ArchiveReadSession(const ArchiveReadSession&) = delete;
    ArchiveReadSession& operator=(const ArchiveReadSession&) = delete;
    ~ArchiveReadSession() {
        if (active_) {
            try {
                arc_.endRead();
            } catch (...) {}}}};
fs::path buildSafeRestoreTarget(const fs::path& outRoot, const std::string& relPath) {
    const std::string rp = normalizeRelPath(relPath);
    if (!isSafeRelativePath(rp))
        throw std::runtime_error("Path relativo inseguro: " + rp);
    fs::path rel(rp);
    if (rel.empty())
        throw std::runtime_error("Path relativo vazio.");
    if (rel.is_absolute() || rel.has_root_name() || rel.has_root_directory())
        throw std::runtime_error("Path relativo invalido: " + rp);
    const fs::path base = fs::absolute(outRoot).lexically_normal();
    const fs::path target = (base / rel).lexically_normal();
    auto baseIt = base.begin();
    auto targetIt = target.begin();
    for (; baseIt != base.end() && targetIt != target.end(); ++baseIt, ++targetIt) {
        if (*baseIt != *targetIt)
            throw std::runtime_error("Path escapou do diretorio de restore: " + rp);}
    if (baseIt != base.end())
        throw std::runtime_error("Diretorio base invalido para restore.");
    return target;}
fs::path makeTempRestorePath(const fs::path& target) {
    return target.string() + ".keeply.tmp";}
struct TempFileCleanup {
    fs::path path;
    bool keep = false;
    explicit TempFileCleanup(fs::path p) : path(std::move(p)) {}
    ~TempFileCleanup() {
        if (!keep) {
            std::error_code ec;
            fs::remove(path, ec);}}};}
static void restoreFileFromArc(StorageArchive& arc, sqlite3_int64 snapshotId, const std::string& relPath, const fs::path& outRoot) {
    const std::string rp = normalizeRelPath(relPath);
    const auto found = arc.findFileBySnapshotAndPath(snapshotId, rp);
    if (!found)
        throw std::runtime_error("Arquivo nao encontrado no snapshot: " + rp);
    const sqlite3_int64 fileId = found->fileId;
    const fs::path target = buildSafeRestoreTarget(outRoot, rp);
    const fs::path tempTarget = makeTempRestorePath(target);
    std::error_code ec;
    fs::create_directories(target.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretorio de destino: " + ec.message());
    TempFileCleanup cleanup(tempTarget);
    std::ofstream out(tempTarget, std::ios::binary | std::ios::trunc);
    if (!out)
        throw std::runtime_error("Falha ao abrir destino temporario de restore: " + tempTarget.string());
    auto chunks = arc.loadFileChunks(fileId);
    Blob chunkHashSeq;
    chunkHashSeq.reserve(chunks.size() * 32);
    for (const auto& c : chunks) {
        const std::string chunkHex = Compactador::blake3Hex(c.blob.data(), c.blob.size());
        const Blob chunkHashBytes = hexDecode(chunkHex);
        chunkHashSeq.insert(chunkHashSeq.end(), chunkHashBytes.begin(), chunkHashBytes.end());
        out.write(reinterpret_cast<const char*>(c.blob.data()), static_cast<std::streamsize>(c.blob.size()));
        if (!out)
            throw std::runtime_error("Erro escrevendo arquivo restaurado: " + tempTarget.string());}
    out.close();
    if (!out)
        throw std::runtime_error("Falha ao fechar arquivo restaurado: " + tempTarget.string());
    if (!found->fileHash.empty() && !chunkHashSeq.empty()) {
        const std::string computedHex = Compactador::blake3Hex(chunkHashSeq.data(), chunkHashSeq.size());
        const std::string expectedHex = hexEncode(found->fileHash.data(), found->fileHash.size());
        if (computedHex != expectedHex)
            throw std::runtime_error("Verificacao de integridade BLAKE3 falhou: hash divergente apos restaurar '" + rp + "'");}
    if (restoreFsyncEnabled())
        fsyncRestoredFile(tempTarget);
    fs::rename(tempTarget, target, ec);
    if (ec) {
        std::error_code removeEc;
        fs::remove(target, removeEc);
        ec.clear();
        fs::rename(tempTarget, target, ec);
        if (ec)
            throw std::runtime_error("Falha movendo arquivo restaurado para destino final: " + ec.message());}
    cleanup.keep = true;
    applyRestoredMtime(target, found->mtime);
    if (restoreFsyncEnabled())
        fsyncRestoredFile(target);}
void RestoreEngine::restoreFile(const fs::path& archivePath, sqlite3_int64 snapshotId, const std::string& relPath, const fs::path& outRoot) {
    StorageArchive arc(archivePath);
    ArchiveReadSession session(arc);
    restoreFileFromArc(arc, snapshotId, relPath, outRoot);}
void RestoreEngine::restoreSnapshot(const fs::path& archivePath, sqlite3_int64 snapshotId, const fs::path& outRoot) {
    StorageArchive arc(archivePath);
    ArchiveReadSession session(arc);
    const auto paths = arc.listSnapshotPaths(snapshotId);
    for (const auto& p : paths) {
        restoreFileFromArc(arc, snapshotId, p, outRoot);}}}

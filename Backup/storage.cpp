#include "keeply.hpp"
#include "change_tracker.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <cstdint>
#include <iostream>

#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>
#elif defined(__linux__)
#include <fcntl.h>
#include <sys/inotify.h>
#include <unistd.h>
#endif
namespace keeply {
namespace {
static void finalizeStmt(sqlite3_stmt*& st) { if (st) { sqlite3_finalize(st); st=nullptr; } }
static void resetStmt(sqlite3_stmt* st) { sqlite3_reset(st); sqlite3_clear_bindings(st); }
static void stepDoneOrThrow(sqlite3* db, sqlite3_stmt* st) { if (sqlite3_step(st) != SQLITE_DONE) throw SqliteError(sqlite3_errmsg(db)); }
static bool stepRowOrDoneOrThrow(sqlite3* db, sqlite3_stmt* st) { int rc=sqlite3_step(st); if (rc==SQLITE_ROW) return true; if (rc==SQLITE_DONE) return false; throw SqliteError(sqlite3_errmsg(db)); }
static void bindIntOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, int v) { if (sqlite3_bind_int(st, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db)); }
static void bindInt64OrThrow(sqlite3* db, sqlite3_stmt* st, int idx, sqlite3_int64 v) { if (sqlite3_bind_int64(st, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db)); }
static void bindTextOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, const std::string& v) { if (sqlite3_bind_text(st, idx, v.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db)); }
static void bindBlobOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, const void* data, int len) { if (sqlite3_bind_blob(st, idx, data, len, SQLITE_TRANSIENT) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db)); }
static void bindNullOrThrow(sqlite3* db, sqlite3_stmt* st, int idx) { if (sqlite3_bind_null(st, idx) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db)); }
static std::string colText(sqlite3_stmt* st, int idx) { const unsigned char* p=sqlite3_column_text(st, idx); return p ? reinterpret_cast<const char*>(p) : ""; }
static constexpr char kChunkPackFileMagic[8]={'K','P','C','H','U','N','K','1'};
static constexpr char kChunkPackRecMagic[8]={'K','P','R','E','C','0','0','1'};
static constexpr std::size_t kMaxChunkRawSize=static_cast<std::size_t>(CHUNK_SIZE);
static constexpr std::size_t kMaxChunkCompSize=(static_cast<std::size_t>(CHUNK_SIZE)*2u)+4096u;
static constexpr std::uint32_t kMaxCompAlgoLen=64;
#pragma pack(push,1)
struct ChunkPackRecHeader { char magic[8]; std::uint64_t rawSize; std::uint64_t compSize; unsigned char sha256[32]; std::uint32_t algoLen; std::uint32_t version; };
#pragma pack(pop)
static fs::path chunkPackPathFromArchive(const fs::path& archiveDbPath) { return fs::path(archiveDbPath.string()+".keeply"); }
static void writeExact(std::ostream& os, const void* data, std::size_t len) { if (len==0) return; os.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len)); if (!os) throw std::runtime_error("Falha escrevendo pack."); }
static void readExact(std::istream& is, void* data, std::size_t len) { if (len==0) return; is.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(len)); if (!is || static_cast<std::size_t>(is.gcount())!=len) throw std::runtime_error("Leitura curta/corrompida no pack."); }
struct ProcessedBlob { ChunkHash hash; std::size_t rawSize; const std::vector<unsigned char>& data; std::string compAlgo; };
class StorageBackend {
public:
    virtual ~StorageBackend()=default;
    virtual void beginSession()=0;
    virtual sqlite3_int64 appendBlob(const ProcessedBlob& blob)=0;
    virtual void commitSession()=0;
    virtual void rollbackSession()=0;
};
class LocalPackBackend : public StorageBackend {
    fs::path packPath;
    std::fstream packStream;
    void verifyOrInitPack() {
        std::error_code ec;
        if (!packPath.parent_path().empty()) fs::create_directories(packPath.parent_path(), ec);
        if (!fs::exists(packPath, ec) || fs::file_size(packPath, ec)==0) {
            std::ofstream out(packPath, std::ios::binary|std::ios::trunc);
            writeExact(out, kChunkPackFileMagic, sizeof(kChunkPackFileMagic));
            out.flush();
            if (!out) throw std::runtime_error("Falha ao inicializar pack.");
            return;
        }
        std::ifstream in(packPath, std::ios::binary);
        if (!in) throw std::runtime_error("Falha ao validar pack.");
        char magic[8]{};
        readExact(in, magic, sizeof(magic));
        if (std::memcmp(magic, kChunkPackFileMagic, sizeof(magic))!=0) throw std::runtime_error("Pack existente com magic invalido.");
    }
public:
    LocalPackBackend(const fs::path& dbPath) : packPath(chunkPackPathFromArchive(dbPath)) {}
    void beginSession() override {
        verifyOrInitPack();
        packStream.open(packPath, std::ios::binary|std::ios::in|std::ios::out);
        if (!packStream) throw std::runtime_error("Falha ao abrir LocalPackBackend.");
        packStream.seekp(0, std::ios::end);
        if (!packStream) throw std::runtime_error("Falha seek no pack.");
    }
    sqlite3_int64 appendBlob(const ProcessedBlob& blob) override {
        packStream.seekp(0, std::ios::end);
        const std::streamoff off=packStream.tellp();
        if (off<0) throw std::runtime_error("Falha tellp no pack.");
        ChunkPackRecHeader hdr{};
        std::memcpy(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic));
        hdr.rawSize=static_cast<std::uint64_t>(blob.rawSize);
        hdr.compSize=static_cast<std::uint64_t>(blob.data.size());
        std::memcpy(hdr.sha256, blob.hash.data(), blob.hash.size());
        hdr.algoLen=static_cast<std::uint32_t>(blob.compAlgo.size());
        hdr.version=1;
        writeExact(packStream, &hdr, sizeof(hdr));
        if (!blob.compAlgo.empty()) writeExact(packStream, blob.compAlgo.data(), blob.compAlgo.size());
        if (!blob.data.empty()) writeExact(packStream, blob.data.data(), blob.data.size());
        return static_cast<sqlite3_int64>(off);
    }
    void commitSession() override { if (packStream.is_open()) { packStream.flush(); if (!packStream) throw std::runtime_error("Falha flush no pack."); packStream.close(); } }
    void rollbackSession() override { if (packStream.is_open()) packStream.close(); }
};
class S3CloudBackend : public StorageBackend {
    std::vector<unsigned char> uploadBuffer;
    const std::size_t S3_PART_SIZE=50*1024*1024;
    sqlite3_int64 virtualOffset=0;
    void flushToS3() {
        if (uploadBuffer.empty()) return;
        std::cout << "[S3_MOCK] Disparando PUT na AWS S3 de " << (uploadBuffer.size()/1024.0/1024.0) << " MB...\n";
        uploadBuffer.clear();
    }
public:
    S3CloudBackend() {}
    void beginSession() override { uploadBuffer.reserve(S3_PART_SIZE); virtualOffset=0; }
    sqlite3_int64 appendBlob(const ProcessedBlob& blob) override {
        std::size_t totalPayloadSize=sizeof(ChunkPackRecHeader)+blob.compAlgo.size()+blob.data.size();
        if (uploadBuffer.size()+totalPayloadSize>S3_PART_SIZE) flushToS3();
        const sqlite3_int64 currentOffset=virtualOffset;
        ChunkPackRecHeader hdr{};
        std::memcpy(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic));
        hdr.rawSize=static_cast<std::uint64_t>(blob.rawSize);
        hdr.compSize=static_cast<std::uint64_t>(blob.data.size());
        std::memcpy(hdr.sha256, blob.hash.data(), blob.hash.size());
        hdr.algoLen=static_cast<std::uint32_t>(blob.compAlgo.size());
        hdr.version=1;
        unsigned char* hdrPtr=reinterpret_cast<unsigned char*>(&hdr);
        uploadBuffer.insert(uploadBuffer.end(), hdrPtr, hdrPtr+sizeof(hdr));
        uploadBuffer.insert(uploadBuffer.end(), blob.compAlgo.begin(), blob.compAlgo.end());
        uploadBuffer.insert(uploadBuffer.end(), blob.data.begin(), blob.data.end());
        virtualOffset+=static_cast<sqlite3_int64>(totalPayloadSize);
        return currentOffset;
    }
    void commitSession() override { flushToS3(); }
    void rollbackSession() override { uploadBuffer.clear(); virtualOffset=0; }
};
static bool tableHasColumn(sqlite3* db, const char* table, const char* colName) {
    std::string sql="PRAGMA table_info("+std::string(table)+");";
    sqlite3_stmt* st=nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr)!=SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
    bool found=false;
    while (sqlite3_step(st)==SQLITE_ROW) {
        const unsigned char* name=sqlite3_column_text(st, 1);
        if (name && std::string(reinterpret_cast<const char*>(name))==colName) { found=true; break; }
    }
    sqlite3_finalize(st);
    return found;
}
static void assertChunksExternalSchemaOrThrow(sqlite3* db) { if (!tableHasColumn(db, "chunks", "pack_offset")) throw std::runtime_error("Tabela chunks sem coluna pack_offset. Schema incompativel."); }
static std::shared_ptr<StorageBackend> asBackend(const std::shared_ptr<void>& p) { return std::static_pointer_cast<StorageBackend>(p); }
}
SqliteError::SqliteError(const std::string& msg) : std::runtime_error(msg) {}
Stmt::Stmt(sqlite3* db, const std::string& sql) : db_(db) { if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr)!=SQLITE_OK) throw SqliteError("sqlite prepare falhou: "+sql); }
Stmt::~Stmt() { if (stmt_) sqlite3_finalize(stmt_); }
sqlite3_stmt* Stmt::get() { return stmt_; }
void Stmt::bindInt(int idx, int v) { bindIntOrThrow(db_, stmt_, idx, v); }
void Stmt::bindInt64(int idx, sqlite3_int64 v) { bindInt64OrThrow(db_, stmt_, idx, v); }
void Stmt::bindText(int idx, const std::string& v) { bindTextOrThrow(db_, stmt_, idx, v); }
void Stmt::bindBlob(int idx, const void* data, int len) { bindBlobOrThrow(db_, stmt_, idx, data, len); }
void Stmt::bindNull(int idx) { bindNullOrThrow(db_, stmt_, idx); }
bool Stmt::stepRow() { return stepRowOrDoneOrThrow(db_, stmt_); }
void Stmt::stepDone() { stepDoneOrThrow(db_, stmt_); }
DB::DB(const fs::path& p) {
    if (sqlite3_open(p.string().c_str(), &db_)!=SQLITE_OK) { std::string err=sqlite3_errmsg(db_); sqlite3_close(db_); throw SqliteError("sqlite open: "+err); }
    exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY;");
    initSchema();
}
DB::~DB() { if (db_) sqlite3_close(db_); }
sqlite3* DB::raw() { return db_; }
void DB::exec(const std::string& sql) {
    char* err=nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err)!=SQLITE_OK) { std::string msg=err ? err : "erro sqlite"; sqlite3_free(err); throw SqliteError(msg+" | SQL: "+sql); }
}
sqlite3_int64 DB::lastInsertId() const { return sqlite3_last_insert_rowid(db_); }
int DB::changes() const { return sqlite3_changes(db_); }
void DB::initSchema() {
    exec("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);");
    exec("CREATE TABLE IF NOT EXISTS snapshots (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL, source_root TEXT NOT NULL, label TEXT, cbt_token INTEGER NOT NULL DEFAULT 0);");
    if (!tableHasColumn(db_, "snapshots", "cbt_token")) {
        exec("ALTER TABLE snapshots ADD COLUMN cbt_token INTEGER NOT NULL DEFAULT 0;");
    }
    exec("CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL, mtime INTEGER NOT NULL, file_hash TEXT, FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE, UNIQUE(snapshot_id, path));");
    exec("CREATE TABLE IF NOT EXISTS chunks (hash_sha256 BLOB PRIMARY KEY, raw_size INTEGER NOT NULL, comp_size INTEGER NOT NULL, comp_algo TEXT NOT NULL, pack_offset INTEGER NOT NULL);");
    exec("CREATE TABLE IF NOT EXISTS file_chunks (file_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL, chunk_hash_sha256 BLOB NOT NULL, raw_size INTEGER NOT NULL, PRIMARY KEY(file_id, chunk_index), FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE, FOREIGN KEY(chunk_hash_sha256) REFERENCES chunks(hash_sha256));");
}
StorageArchive::StorageArchive(const fs::path& path) : path_(path), db_(path) { assertChunksExternalSchemaOrThrow(db_.raw()); prepareHotStatements(); }
StorageArchive::~StorageArchive() { try { if (backendOpaque_) rollback(); } catch (...) {} finalizeHotStatements(); }
void StorageArchive::prepareHotStatements() {
    auto prepare=[&](sqlite3_stmt*& out, const char* sql) { if (sqlite3_prepare_v2(db_.raw(), sql, -1, &out, nullptr)!=SQLITE_OK) throw SqliteError(std::string("sqlite prepare(hot): ")+sqlite3_errmsg(db_.raw())); };
    prepare(hot_.insertSnapshot, "INSERT INTO snapshots(created_at, source_root, label) VALUES(?,?,?);");
    prepare(hot_.updateSnapshotCbtToken, "UPDATE snapshots SET cbt_token = ? WHERE id = ?;");
    prepare(hot_.insertFilePlaceholder, "INSERT INTO files(snapshot_id, path, size, mtime, file_hash) VALUES(?,?,?,?,NULL);");
    prepare(hot_.cloneFileInsert, "INSERT INTO files(snapshot_id, path, size, mtime, file_hash) VALUES(?,?,?,?,?);");
    prepare(hot_.cloneFileChunks, "INSERT INTO file_chunks(file_id, chunk_index, chunk_hash_sha256, raw_size) SELECT ?, chunk_index, chunk_hash_sha256, raw_size FROM file_chunks WHERE file_id = ? ORDER BY chunk_index;");
    prepare(hot_.insertChunkIfMissing, "INSERT OR IGNORE INTO chunks(hash_sha256, raw_size, comp_size, comp_algo, pack_offset) VALUES(?,?,?,?,?);");
    prepare(hot_.updateChunkOffset, "UPDATE chunks SET pack_offset = ? WHERE hash_sha256 = ?;");
    prepare(hot_.addFileChunk, "INSERT INTO file_chunks(file_id, chunk_index, chunk_hash_sha256, raw_size) VALUES(?,?,?,?);");
}
void StorageArchive::finalizeHotStatements() {
    finalizeStmt(hot_.insertSnapshot); finalizeStmt(hot_.updateSnapshotCbtToken); finalizeStmt(hot_.insertFilePlaceholder); finalizeStmt(hot_.cloneFileInsert);
    finalizeStmt(hot_.cloneFileChunks); finalizeStmt(hot_.insertChunkIfMissing); finalizeStmt(hot_.updateChunkOffset); finalizeStmt(hot_.addFileChunk);
}
void StorageArchive::begin() {
    db_.exec("BEGIN IMMEDIATE;");
    auto backend=std::make_shared<LocalPackBackend>(path_);
    backend->beginSession();
    backendOpaque_=backend;
}
void StorageArchive::commit() {
    auto backend=asBackend(backendOpaque_);
    if (backend) { backend->commitSession(); backendOpaque_.reset(); }
    db_.exec("COMMIT;");
}
void StorageArchive::rollback() {
    auto backend=asBackend(backendOpaque_);
    if (backend) { backend->rollbackSession(); backendOpaque_.reset(); }
    packIn_.reset();
    try { db_.exec("ROLLBACK;"); } catch (...) {}
}
sqlite3_int64 StorageArchive::createSnapshot(const std::string& sourceRoot, const std::string& label) {
    sqlite3_stmt* st=hot_.insertSnapshot;
    resetStmt(st);
    bindTextOrThrow(db_.raw(), st, 1, nowIsoLocal());
    bindTextOrThrow(db_.raw(), st, 2, sourceRoot);
    if (label.empty()) bindNullOrThrow(db_.raw(), st, 3); else bindTextOrThrow(db_.raw(), st, 3, label);
    stepDoneOrThrow(db_.raw(), st);
    return db_.lastInsertId();
}
std::optional<sqlite3_int64> StorageArchive::latestSnapshotId() {
    Stmt st(db_.raw(), "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1;");
    if (st.stepRow()) return sqlite3_column_int64(st.get(), 0);
    return std::nullopt;
}
std::optional<std::uint64_t> StorageArchive::latestSnapshotCbtToken() {
    Stmt st(db_.raw(), "SELECT cbt_token FROM snapshots ORDER BY id DESC LIMIT 1;");
    if (!st.stepRow()) return std::nullopt;
    return static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 0));
}
std::map<std::string, FileInfo> StorageArchive::loadSnapshotFileMap(sqlite3_int64 snapshotId) {
    std::map<std::string, FileInfo> out;
    Stmt st(db_.raw(), "SELECT id, path, size, mtime, COALESCE(file_hash,'') FROM files WHERE snapshot_id = ?;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) { FileInfo f; f.fileId=sqlite3_column_int64(st.get(), 0); f.size=sqlite3_column_int64(st.get(), 2); f.mtime=sqlite3_column_int64(st.get(), 3); f.fileHash=colText(st.get(), 4); out[colText(st.get(), 1)]=std::move(f); }
    return out;
}
sqlite3_int64 StorageArchive::insertFilePlaceholder(sqlite3_int64 snapshotId, const std::string& relPath, sqlite3_int64 size, sqlite3_int64 mtime) {
    sqlite3_stmt* st=hot_.insertFilePlaceholder;
    resetStmt(st);
    bindInt64OrThrow(db_.raw(), st, 1, snapshotId);
    bindTextOrThrow(db_.raw(), st, 2, relPath);
    bindInt64OrThrow(db_.raw(), st, 3, size);
    bindInt64OrThrow(db_.raw(), st, 4, mtime);
    stepDoneOrThrow(db_.raw(), st);
    return db_.lastInsertId();
}
sqlite3_int64 StorageArchive::cloneFileFromPrevious(sqlite3_int64 snapshotId, const std::string& relPath, const FileInfo& prev) {
    sqlite3_stmt* ins=hot_.cloneFileInsert;
    resetStmt(ins);
    bindInt64OrThrow(db_.raw(), ins, 1, snapshotId);
    bindTextOrThrow(db_.raw(), ins, 2, relPath);
    bindInt64OrThrow(db_.raw(), ins, 3, prev.size);
    bindInt64OrThrow(db_.raw(), ins, 4, prev.mtime);
    if (prev.fileHash.empty()) bindNullOrThrow(db_.raw(), ins, 5); else bindTextOrThrow(db_.raw(), ins, 5, prev.fileHash);
    stepDoneOrThrow(db_.raw(), ins);
    const sqlite3_int64 newFileId=db_.lastInsertId();
    sqlite3_stmt* clone=hot_.cloneFileChunks;
    resetStmt(clone);
    bindInt64OrThrow(db_.raw(), clone, 1, newFileId);
    bindInt64OrThrow(db_.raw(), clone, 2, prev.fileId);
    stepDoneOrThrow(db_.raw(), clone);
    return newFileId;
}
bool StorageArchive::insertChunkIfMissing(const ChunkHash& sha, std::size_t rawSize, const std::vector<unsigned char>& comp, const std::string& compAlgo) {
    { Stmt check(db_.raw(), "SELECT 1 FROM chunks WHERE hash_sha256 = ? LIMIT 1;"); check.bindBlob(1, sha.data(), static_cast<int>(sha.size())); if (check.stepRow()) return false; }
    sqlite3_stmt* ins=hot_.insertChunkIfMissing;
    resetStmt(ins);
    bindBlobOrThrow(db_.raw(), ins, 1, sha.data(), static_cast<int>(sha.size()));
    bindInt64OrThrow(db_.raw(), ins, 2, static_cast<sqlite3_int64>(rawSize));
    bindInt64OrThrow(db_.raw(), ins, 3, static_cast<sqlite3_int64>(comp.size()));
    bindTextOrThrow(db_.raw(), ins, 4, compAlgo);
    bindInt64OrThrow(db_.raw(), ins, 5, static_cast<sqlite3_int64>(-1));
    stepDoneOrThrow(db_.raw(), ins);
    if (sqlite3_changes(db_.raw())<=0) return false;
    auto backend=asBackend(backendOpaque_);
    if (!backend) throw std::runtime_error("Backend nao iniciado. Chame begin().");
    ProcessedBlob pBlob{ sha, rawSize, comp, compAlgo };
    const sqlite3_int64 packOffset=backend->appendBlob(pBlob);
    sqlite3_stmt* upd=hot_.updateChunkOffset;
    resetStmt(upd);
    bindInt64OrThrow(db_.raw(), upd, 1, packOffset);
    bindBlobOrThrow(db_.raw(), upd, 2, sha.data(), static_cast<int>(sha.size()));
    stepDoneOrThrow(db_.raw(), upd);
    if (sqlite3_changes(db_.raw())!=1) throw std::runtime_error("Falha atualizando pack_offset.");
    return true;
}
void StorageArchive::addFileChunk(sqlite3_int64 fileId, int chunkIdx, const ChunkHash& chunkSha, std::size_t rawSize) {
    sqlite3_stmt* st=hot_.addFileChunk;
    resetStmt(st);
    bindInt64OrThrow(db_.raw(), st, 1, fileId);
    bindIntOrThrow(db_.raw(), st, 2, chunkIdx);
    bindBlobOrThrow(db_.raw(), st, 3, chunkSha.data(), static_cast<int>(chunkSha.size()));
    bindInt64OrThrow(db_.raw(), st, 4, static_cast<sqlite3_int64>(rawSize));
    stepDoneOrThrow(db_.raw(), st);
}
void StorageArchive::addFileChunksBulk(sqlite3_int64 fileId, const std::vector<PendingFileChunk>& rows) { if (rows.empty()) return; for (const auto& r : rows) addFileChunk(fileId, r.chunkIdx, r.chunkHash, r.rawSize); }
std::optional<std::pair<sqlite3_int64, sqlite3_int64>> StorageArchive::findFileBySnapshotAndPath(sqlite3_int64 snapshotId, const std::string& relPath) {
    Stmt st(db_.raw(), "SELECT id, size FROM files WHERE snapshot_id = ? AND path = ?;");
    st.bindInt64(1, snapshotId);
    st.bindText(2, relPath);
    if (!st.stepRow()) return std::nullopt;
    return std::make_pair(sqlite3_column_int64(st.get(), 0), sqlite3_column_int64(st.get(), 1));
}
std::vector<unsigned char> StorageArchive::readPackAt(sqlite3_int64 recordOffset,const ChunkHash& expectedSha,std::size_t expectedRawSize,std::size_t expectedCompSize,const std::string& expectedAlgo) const {
    const fs::path packPath=chunkPackPathFromArchive(path_);
    if (!packIn_) { packIn_=std::make_unique<std::ifstream>(packPath, std::ios::binary); if (!*packIn_) throw std::runtime_error("Falha abrindo pack para leitura: "+packPath.string()); char magic[8]{}; readExact(*packIn_, magic, sizeof(magic)); if (std::memcmp(magic, kChunkPackFileMagic, sizeof(magic))!=0) throw std::runtime_error("Magic invalido no arquivo pack."); }
    std::ifstream& in=*packIn_;
    in.clear();
    in.seekg(static_cast<std::streamoff>(recordOffset), std::ios::beg);
    if (!in) throw std::runtime_error("Offset invalido no pack de chunks.");
    ChunkPackRecHeader hdr{};
    readExact(in, &hdr, sizeof(hdr));
    if (std::memcmp(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic))!=0) throw std::runtime_error("Magic invalido no registro do pack.");
    if (hdr.algoLen>kMaxCompAlgoLen) throw std::runtime_error("algoLen invalido no pack.");
    if (hdr.rawSize>kMaxChunkRawSize) throw std::runtime_error("rawSize invalido no pack.");
    if (hdr.compSize>kMaxChunkCompSize) throw std::runtime_error("compSize invalido no pack.");
    if (hdr.rawSize!=static_cast<std::uint64_t>(expectedRawSize)) throw std::runtime_error("rawSize divergente entre SQLite e pack.");
    if (hdr.compSize!=static_cast<std::uint64_t>(expectedCompSize)) throw std::runtime_error("compSize divergente entre SQLite e pack.");
    if (std::memcmp(hdr.sha256, expectedSha.data(), expectedSha.size())!=0) throw std::runtime_error("hash divergente entre SQLite e pack.");
    std::string algo(static_cast<std::size_t>(hdr.algoLen), '\0');
    if (hdr.algoLen>0) readExact(in, algo.data(), static_cast<std::size_t>(hdr.algoLen));
    if (algo!=expectedAlgo) throw std::runtime_error("comp_algo divergente entre SQLite e pack.");
    std::vector<unsigned char> comp(static_cast<std::size_t>(hdr.compSize));
    if (hdr.compSize>0) readExact(in, comp.data(), static_cast<std::size_t>(hdr.compSize));
    return comp;
}
std::vector<StoredChunkRow> StorageArchive::loadFileChunks(sqlite3_int64 fileId) {
    std::vector<StoredChunkRow> out;
    Stmt st(db_.raw(), "SELECT fc.chunk_index, fc.raw_size, c.comp_size, c.comp_algo, c.pack_offset, fc.chunk_hash_sha256 FROM file_chunks fc JOIN chunks c ON c.hash_sha256 = fc.chunk_hash_sha256 WHERE fc.file_id = ? ORDER BY fc.chunk_index ASC;");
    st.bindInt64(1, fileId);
    while (st.stepRow()) {
        StoredChunkRow row;
        row.chunkIndex=sqlite3_column_int(st.get(), 0);
        row.rawSize=static_cast<std::size_t>(sqlite3_column_int64(st.get(), 1));
        row.compSize=static_cast<std::size_t>(sqlite3_column_int64(st.get(), 2));
        row.algo=colText(st.get(), 3);
        const sqlite3_int64 packOffset=sqlite3_column_int64(st.get(), 4);
        const void* hashPtr=sqlite3_column_blob(st.get(), 5);
        const int hashBytes=sqlite3_column_bytes(st.get(), 5);
        if (!hashPtr || hashBytes!=(int)ChunkHash{}.size()) throw std::runtime_error("hash_sha256 invalido vindo do SQLite.");
        ChunkHash chunkSha{};
        std::memcpy(chunkSha.data(), hashPtr, chunkSha.size());
        row.blob=readPackAt(packOffset, chunkSha, row.rawSize, row.compSize, row.algo);
        out.push_back(std::move(row));
    }
    return out;
}
std::vector<std::string> StorageArchive::listSnapshotPaths(sqlite3_int64 snapshotId) {
    std::vector<std::string> paths;
    Stmt st(db_.raw(), "SELECT path FROM files WHERE snapshot_id = ? ORDER BY path;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) paths.emplace_back(colText(st.get(), 0));
    return paths;
}
void StorageArchive::updateSnapshotCbtToken(sqlite3_int64 snapshotId, std::uint64_t token) {
    sqlite3_stmt* st=hot_.updateSnapshotCbtToken;
    resetStmt(st);
    bindInt64OrThrow(db_.raw(), st, 1, static_cast<sqlite3_int64>(token));
    bindInt64OrThrow(db_.raw(), st, 2, snapshotId);
    stepDoneOrThrow(db_.raw(), st);
    if (sqlite3_changes(db_.raw()) != 1) throw std::runtime_error("Falha atualizando cbt_token do snapshot.");
}

#ifdef _WIN32
class WindowsUSNTracker : public ChangeTracker {
    HANDLE hVol = INVALID_HANDLE_VALUE;
    std::filesystem::path driveLetter;
    std::filesystem::path root;

    std::string resolvePathFromFileId(DWORDLONG fileId) {
        FILE_ID_DESCRIPTOR fileDesc{};
        fileDesc.dwSize = sizeof(FILE_ID_DESCRIPTOR);
        fileDesc.Type = FileIdType;
        fileDesc.FileId.QuadPart = fileId;

        HANDLE hFile = OpenFileById(
            hVol,
            &fileDesc,
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            nullptr,
            FILE_FLAG_BACKUP_SEMANTICS
        );
        if (hFile == INVALID_HANDLE_VALUE) return {};

        char pathBuf[MAX_PATH]{};
        const DWORD len = GetFinalPathNameByHandleA(hFile, pathBuf, MAX_PATH, FILE_NAME_NORMALIZED);
        CloseHandle(hFile);

        if (len == 0 || len >= MAX_PATH) return {};

        std::string fullPath(pathBuf);
        if (fullPath.rfind("\\\\?\\", 0) == 0) fullPath = fullPath.substr(4);
        return fullPath;
    }

public:
    ~WindowsUSNTracker() override {
        if (hVol != INVALID_HANDLE_VALUE) CloseHandle(hVol);
    }

    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "usn_journal"; }

    void startTracking(const std::filesystem::path& rootPath) override {
        root = std::filesystem::absolute(rootPath);
        driveLetter = root.root_name();

        const std::string volPath = "\\\\.\\" + driveLetter.string();
        hVol = CreateFileA(
            volPath.c_str(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr,
            OPEN_EXISTING,
            0,
            nullptr
        );

        if (hVol == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("Falha ao abrir o volume NTFS. Keeply precisa rodar como Administrador.");
        }
    }

    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        std::vector<ChangedFile> changes;
        std::unordered_set<std::string> seen;

        USN_JOURNAL_DATA_V0 usnJournalData{};
        DWORD bytesReturned = 0;
        if (!DeviceIoControl(
                hVol,
                FSCTL_QUERY_USN_JOURNAL,
                nullptr,
                0,
                &usnJournalData,
                sizeof(usnJournalData),
                &bytesReturned,
                nullptr)) {
            throw std::runtime_error("Falha ao consultar o USN Journal. O volume e NTFS?");
        }

        READ_USN_JOURNAL_DATA_V0 readData{};
        readData.StartUsn = lastToken == 0 ? usnJournalData.NextUsn : static_cast<USN>(lastToken);
        readData.ReasonMask =
            USN_REASON_DATA_OVERWRITE |
            USN_REASON_DATA_EXTEND |
            USN_REASON_FILE_CREATE |
            USN_REASON_FILE_DELETE;
        readData.ReturnOnlyOnClose = FALSE;
        readData.Timeout = 0;
        readData.BytesToWaitFor = 0;
        readData.UsnJournalID = usnJournalData.UsnJournalID;

        char buffer[4096];
        DWORD bytesRead = 0;
        newToken = static_cast<std::uint64_t>(usnJournalData.NextUsn);

        while (true) {
            if (!DeviceIoControl(
                    hVol,
                    FSCTL_READ_USN_JOURNAL,
                    &readData,
                    sizeof(readData),
                    buffer,
                    sizeof(buffer),
                    &bytesRead,
                    nullptr)) {
                break;
            }

            if (bytesRead <= sizeof(USN)) break;

            USN* nextUsn = reinterpret_cast<USN*>(buffer);
            readData.StartUsn = *nextUsn;
            newToken = static_cast<std::uint64_t>(*nextUsn);

            USN_RECORD* record = reinterpret_cast<USN_RECORD*>(reinterpret_cast<PUCHAR>(buffer) + sizeof(USN));
            while (reinterpret_cast<PUCHAR>(record) < reinterpret_cast<PUCHAR>(buffer) + bytesRead) {
                const bool isDeleted = (record->Reason & USN_REASON_FILE_DELETE) != 0;
                const std::string fullPath = resolvePathFromFileId(record->FileReferenceNumber);

                if (!fullPath.empty() && fullPath.find(root.string()) == 0) {
                    std::string relPath = fullPath.substr(root.string().length());
                    if (!relPath.empty() && relPath.front() == '\\') relPath.erase(relPath.begin());
                    if (seen.insert(relPath).second) changes.push_back({relPath, isDeleted});
                }

                record = reinterpret_cast<USN_RECORD*>(reinterpret_cast<PUCHAR>(record) + record->RecordLength);
            }
        }

        return changes;
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<WindowsUSNTracker>();
}
#elif defined(__linux__)
class LinuxInotifyTracker : public ChangeTracker {
    int inotifyFd = -1;
    std::filesystem::path root;
    std::unordered_map<int, std::string> wdToPath;

    void addWatchRecursive(const std::filesystem::path& path) {
        const int wd = inotify_add_watch(
            inotifyFd,
            path.c_str(),
            IN_MODIFY | IN_CREATE | IN_DELETE | IN_MOVED_TO
        );
        if (wd != -1) wdToPath[wd] = path.string();

        std::error_code ec;
        for (auto& p : std::filesystem::directory_iterator(path, ec)) {
            if (p.is_directory()) addWatchRecursive(p.path());
        }
    }

public:
    ~LinuxInotifyTracker() override {
        if (inotifyFd != -1) close(inotifyFd);
    }

    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "inotify"; }

    void startTracking(const std::filesystem::path& rootPath) override {
        root = std::filesystem::absolute(rootPath);
        inotifyFd = inotify_init1(IN_NONBLOCK);
        if (inotifyFd == -1) throw std::runtime_error("Falha ao inicializar inotify.");

        std::cout << "[Keeply] Indexando watches do inotify recursivamente...\n";
        addWatchRecursive(root);
    }

    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        newToken = lastToken + 1;

        std::vector<ChangedFile> changes;
        std::unordered_set<std::string> seen;

        alignas(struct inotify_event) char buffer[4096];
        ssize_t len = 0;

        while ((len = read(inotifyFd, buffer, sizeof(buffer))) > 0) {
            char* ptr = buffer;
            while (ptr < buffer + len) {
                const auto* event = reinterpret_cast<const struct inotify_event*>(ptr);

                if (event->len > 0) {
                    auto it = wdToPath.find(event->wd);
                    if (it != wdToPath.end()) {
                        const std::string fullPath = it->second + "/" + event->name;
                        const std::string relPath = fullPath.substr(root.string().length() + 1);
                        const bool isDeleted = (event->mask & IN_DELETE) != 0;

                        if (seen.insert(relPath).second) changes.push_back({relPath, isDeleted});

                        if (event->mask & IN_CREATE) {
                            std::error_code ec;
                            if (std::filesystem::is_directory(fullPath, ec)) addWatchRecursive(fullPath);
                        }
                    }
                }

                ptr += sizeof(struct inotify_event) + event->len;
            }
        }

        return changes;
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<LinuxInotifyTracker>();
}
#else
class FallbackScanner : public ChangeTracker {
public:
    bool isAvailable() const override { return false; }
    const char* backendName() const override { return "unsupported"; }

    void startTracking(const std::filesystem::path&) override {
        std::cout << "[Keeply] Aviso: SO nao suporta CBT nativo. Rastreamento incremental puro indisponivel.\n";
    }

    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        newToken = lastToken;
        return {};
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<FallbackScanner>();
}
#endif
}

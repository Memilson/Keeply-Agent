#include "backend_armazenamento.hpp"
#include "../Core/utilitarios.hpp"
#include "../Core/sqlite_util.hpp"
#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <cstdint>
namespace keeply {
namespace {
constexpr const char* kDefaultPackId = "main";
constexpr std::uint64_t kPackMagic = 0x31504C594B4C424Bull;
static void logStorageWarning(const std::string& msg) {
    std::cerr << "[keeply][storage][warn] " << msg << '\n';}
static void ensureParentDir(const fs::path& p) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretorio: " + ec.message());}
static std::string blobToHex(const Blob& b) {
    return keeply::hexEncode(b.data(), b.size());}
static std::string safeFileComponent(std::string value) {
    for (char& c : value) {
        const bool ok =
            (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.';
        if (!ok) c = '_';}
    while (!value.empty() && value.back() == '.') value.pop_back();
    return value.empty() ? std::string("bundle") : value;}
static void stepDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    if (sqlite3_step(st) != SQLITE_DONE) throw SqliteError(sqlite3_errmsg(db));}
static bool stepRowOrDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) return true;
    if (rc == SQLITE_DONE) return false;
    throw SqliteError(sqlite3_errmsg(db));}
static std::string colText(sqlite3_stmt* st, int idx) {
    const unsigned char* p = sqlite3_column_text(st, idx);
    return p ? reinterpret_cast<const char*>(p) : "";}
static Blob colBlob(sqlite3_stmt* st, int idx) {
    const void* p = sqlite3_column_blob(st, idx);
    const int n = sqlite3_column_bytes(st, idx);
    if (!p || n <= 0) return {};
    const auto* b = static_cast<const unsigned char*>(p);
    return Blob(b, b + n);}
using SqlTxn = keeply::SharedSqlTransaction;
static int readSchemaVersion(sqlite3* db) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT version FROM schema_version LIMIT 1;", -1, &st, nullptr) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
    int ver = 0;
    if (sqlite3_step(st) == SQLITE_ROW) ver = sqlite3_column_int(st, 0);
    else {
        sqlite3_finalize(st);
        throw SqliteError("schema_version vazio ou invalido.");}
    sqlite3_finalize(st);
    return ver;}
static void writeSchemaVersion(sqlite3* db, int ver) {
    char* err = nullptr;
    const std::string sql = "INSERT OR REPLACE INTO schema_version(rowid,version) VALUES(1," + std::to_string(ver) + ");";
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string m = err ? err : "erro sqlite";
        if (err) sqlite3_free(err);
        throw SqliteError(m);}}
class LocalPackBackend final : public StorageBackend {
public:
    explicit LocalPackBackend(const fs::path& archivePath)
        : paths_(describeArchiveStorage(archivePath)) {}
    void beginSession() override {
        if (inTxn_) return;
        ensureParentDir(paths_.packPath);
        ensureParentDir(paths_.indexPath);
        if (!fs::exists(paths_.packPath)) {
            std::ofstream create(paths_.packPath, std::ios::binary);
            if (!create) throw std::runtime_error("Falha criando pack.");}
        if (!fs::exists(paths_.indexPath)) {
            std::ofstream create(paths_.indexPath, std::ios::binary);
            if (!create) throw std::runtime_error("Falha criando index do pack.");}
        packTxnStart_ = fileSizeNoThrow(paths_.packPath);
        indexTxnStart_ = fileSizeNoThrow(paths_.indexPath);
        pack_.open(paths_.packPath, std::ios::binary | std::ios::in | std::ios::out);
        if (!pack_) throw std::runtime_error("Falha abrindo pack.");
        idx_.open(paths_.indexPath, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);
        if (!idx_) throw std::runtime_error("Falha abrindo index do pack.");
        inTxn_ = true;}
    void commitSession() override {
        if (!inTxn_) return;
        pack_.flush();
        idx_.flush();
        pack_.close();
        idx_.close();
        inTxn_ = false;}
    void rollbackSession() override {
        if (!inTxn_) return;
        pack_.flush();
        idx_.flush();
        pack_.close();
        idx_.close();
        std::error_code ec1, ec2;
        fs::resize_file(paths_.packPath, packTxnStart_, ec1);
        fs::resize_file(paths_.indexPath, indexTxnStart_, ec2);
        inTxn_ = false;
        if (ec1) throw std::runtime_error("Falha no rollback do pack: " + ec1.message());
        if (ec2) throw std::runtime_error("Falha no rollback do index do pack: " + ec2.message());}
    sqlite3_int64 appendBlob(const ProcessedBlob& blob) override {
        if (!inTxn_) throw std::runtime_error("Backend nao esta em transacao.");
        pack_.seekp(0, std::ios::end);
        const sqlite3_int64 off = static_cast<sqlite3_int64>(pack_.tellp());
        const std::uint64_t raw64 = static_cast<std::uint64_t>(blob.rawSize);
        const std::uint64_t comp64 = static_cast<std::uint64_t>(blob.data.size());
        const std::uint32_t algoLen = static_cast<std::uint32_t>(blob.compAlgo.size());
        pack_.write(reinterpret_cast<const char*>(&kPackMagic), sizeof(kPackMagic));
        pack_.write(reinterpret_cast<const char*>(&raw64), sizeof(raw64));
        pack_.write(reinterpret_cast<const char*>(&comp64), sizeof(comp64));
        pack_.write(reinterpret_cast<const char*>(&algoLen), sizeof(algoLen));
        pack_.write(reinterpret_cast<const char*>(blob.hash.data()), static_cast<std::streamsize>(blob.hash.size()));
        if (algoLen > 0) pack_.write(blob.compAlgo.data(), static_cast<std::streamsize>(algoLen));
        if (!blob.data.empty()) pack_.write(reinterpret_cast<const char*>(blob.data.data()), static_cast<std::streamsize>(blob.data.size()));
        if (!pack_) throw std::runtime_error("Falha gravando blob no pack.");
        idx_ << off << "|" << hexOfBytes(blob.hash.data(), blob.hash.size()) << "|" << raw64 << "|" << comp64 << "|" << blob.compAlgo << "\n";
        if (!idx_) throw std::runtime_error("Falha gravando index do pack.");
        return off;}
private:
    static std::uint64_t fileSizeNoThrow(const fs::path& p) {
        std::error_code ec;
        const auto sz = fs::file_size(p, ec);
        return ec ? 0ull : static_cast<std::uint64_t>(sz);}
    ArchiveStoragePaths paths_;
    std::fstream pack_;
    std::fstream idx_;
    std::uint64_t packTxnStart_ = 0;
    std::uint64_t indexTxnStart_ = 0;
    bool inTxn_ = false;
};}
ArchiveStoragePaths describeArchiveStorage(const fs::path& archivePath) {
    ArchiveStoragePaths paths;
    paths.archivePath = archivePath;
    paths.packPath = archivePath.parent_path() / (archivePath.stem().string() + ".klyp.pack");
    paths.indexPath = archivePath.parent_path() / (archivePath.stem().string() + ".klyp.idx");
    return paths;}
void ensureArchiveStorageParent(const fs::path& archivePath) {
    if (archivePath.parent_path().empty()) return;
    std::error_code ec;
    fs::create_directories(archivePath.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando pasta do arquivo: " + ec.message());}
std::shared_ptr<StorageBackend> makeLocalStorageBackend(const fs::path& archivePath) {
    return std::make_shared<LocalPackBackend>(archivePath);}
SqliteError::SqliteError(const std::string& msg) : std::runtime_error(msg) {}
Stmt::Stmt(sqlite3* db, const std::string& sql) : db_(db) {
    if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK)
        throw SqliteError("sqlite prepare falhou: " + sql + " — " + sqlite3_errmsg(db_));}
Stmt::~Stmt() {
    if (stmt_) sqlite3_finalize(stmt_);}
sqlite3_stmt* Stmt::get() { return stmt_; }
void Stmt::bindInt(int idx, int v) {
    if (sqlite3_bind_int(stmt_, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));}
void Stmt::bindInt64(int idx, sqlite3_int64 v) {
    if (sqlite3_bind_int64(stmt_, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));}
void Stmt::bindText(int idx, const std::string& v) {
    if (sqlite3_bind_text(stmt_, idx, v.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));}
void Stmt::bindBlob(int idx, const void* d, int len) {
    if (sqlite3_bind_blob(stmt_, idx, d, len, SQLITE_TRANSIENT) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));}
void Stmt::bindNull(int idx) {
    if (sqlite3_bind_null(stmt_, idx) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));}
void Stmt::reset() {
    sqlite3_reset(stmt_);
    sqlite3_clear_bindings(stmt_);}
bool Stmt::stepRow() { return stepRowOrDoneOrThrow(db_, stmt_); }
void Stmt::stepDone() { stepDoneOrThrow(db_, stmt_); }
DB::DB(const fs::path& p) {
    ensureArchiveStorageParent(p);
    try {
        db_ = openKeeplyDb(p, "sqlite open");
    } catch (const std::exception& ex) {
        throw SqliteError(ex.what());}
    initSchema();}
DB::~DB() {
    if (db_) sqlite3_close(db_);}
sqlite3* DB::raw() { return db_; }
void DB::exec(const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "erro sqlite";
        if (err) sqlite3_free(err);
        throw SqliteError(msg + " | SQL: " + sql);}}
sqlite3_int64 DB::lastInsertId() const { return sqlite3_last_insert_rowid(db_); }
int DB::changes() const { return sqlite3_changes(db_); }
void DB::initSchema() {
    constexpr int kTargetSchemaVersion = 11;
    exec("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);");
    exec("INSERT OR IGNORE INTO schema_version(rowid,version) SELECT 1,0 WHERE NOT EXISTS (SELECT 1 FROM schema_version);");
    const int ver = readSchemaVersion(db_);
    if (ver != kTargetSchemaVersion) {
        exec("DROP TABLE IF EXISTS file_signatures;");
        exec("DROP TABLE IF EXISTS upload_parts;");
        exec("DROP TABLE IF EXISTS file_chunks;");
        exec("DROP TABLE IF EXISTS files;");
        exec("DROP TABLE IF EXISTS chunks;");
        exec("DROP TABLE IF EXISTS snapshots;");
        exec("DROP TABLE IF EXISTS meta;");}
    exec("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);");
    exec("CREATE TABLE IF NOT EXISTS snapshots("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "created_at TEXT NOT NULL,"
         "source_root TEXT NOT NULL,"
         "label TEXT,"
         "backup_type TEXT NOT NULL DEFAULT 'full',"
         "cbt_token INTEGER NOT NULL DEFAULT 0);");
    exec("CREATE TABLE IF NOT EXISTS chunks("
         "chunk_hash BLOB PRIMARY KEY,"
         "raw_size INTEGER NOT NULL,"
         "comp_size INTEGER NOT NULL,"
         "comp_algo TEXT NOT NULL,"
         "pack_id TEXT NOT NULL DEFAULT 'main',"
         "pack_offset INTEGER NOT NULL,"
         "storage_state TEXT NOT NULL DEFAULT 'ready',"
         "encrypt_iv BLOB);");
    exec("CREATE TABLE IF NOT EXISTS files("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "snapshot_id INTEGER NOT NULL,"
         "path TEXT NOT NULL,"
         "size INTEGER NOT NULL,"
         "mtime INTEGER NOT NULL,"
         "file_hash BLOB,"
         "FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,"
         "UNIQUE(snapshot_id, path));");
    exec("CREATE TABLE IF NOT EXISTS file_chunks("
         "file_id INTEGER NOT NULL,"
         "chunk_index INTEGER NOT NULL,"
         "chunk_hash BLOB NOT NULL,"
         "raw_size INTEGER NOT NULL,"
         "offset_in_chunk INTEGER NOT NULL DEFAULT 0,"
         "PRIMARY KEY(file_id,chunk_index),"
         "FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,"
         "FOREIGN KEY(chunk_hash) REFERENCES chunks(chunk_hash));");
    exec("CREATE TABLE IF NOT EXISTS file_signatures("
         "id INTEGER PRIMARY KEY AUTOINCREMENT,"
         "snapshot_id INTEGER NOT NULL,"
         "path TEXT NOT NULL,"
         "sig_blob BLOB NOT NULL,"
         "created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),"
         "UNIQUE(snapshot_id, path),"
         "FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE);");
    exec("CREATE TABLE IF NOT EXISTS upload_parts("
         "bundle_id TEXT NOT NULL,"
         "part_index INTEGER NOT NULL,"
         "upload_id TEXT NOT NULL,"
         "etag TEXT NOT NULL,"
         "uploaded_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),"
         "PRIMARY KEY(bundle_id, part_index));");
    exec("CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id);");
    exec("CREATE INDEX IF NOT EXISTS idx_fc_file ON file_chunks(file_id);");
    exec("CREATE INDEX IF NOT EXISTS idx_filesig_snapshot_path ON file_signatures(snapshot_id, path);");
    exec("CREATE INDEX IF NOT EXISTS idx_chunks_state_hash ON chunks(storage_state, chunk_hash);");
    writeSchemaVersion(db_, kTargetSchemaVersion);}
StorageArchive::StorageArchive(const fs::path& path) : path_(path), db_(path) {
    prepareHotStatements();}
StorageArchive::~StorageArchive() {
    try {
        if (writeTxnActive_) rollback();
        else if (readTxnActive_) endRead();
    } catch (const std::exception& ex) {
        logStorageWarning(std::string("rollback falhou no destrutor: ") + ex.what());
    } catch (...) {
        logStorageWarning("rollback falhou no destrutor com excecao desconhecida.");}
    finalizeHotStatements();}
void StorageArchive::begin() {
    if (writeTxnActive_ || readTxnActive_) throw std::runtime_error("StorageArchive ja possui transacao ativa.");
    db_.exec("BEGIN IMMEDIATE;");
    if (!backendOpaque_) backendOpaque_ = makeLocalStorageBackend(path_);
    std::static_pointer_cast<StorageBackend>(backendOpaque_)->beginSession();
    writeTxnActive_ = true;}
void StorageArchive::beginRead() {
    if (writeTxnActive_ || readTxnActive_) throw std::runtime_error("StorageArchive ja possui transacao ativa.");
    db_.exec("BEGIN DEFERRED;");
    readTxnActive_ = true;}
void StorageArchive::commit() {
    if (!writeTxnActive_) throw std::runtime_error("Nenhuma transacao de escrita ativa.");
    if (backendOpaque_) std::static_pointer_cast<StorageBackend>(backendOpaque_)->commitSession();
    db_.exec("COMMIT;");
    packIn_.reset();
    writeTxnActive_ = false;}
void StorageArchive::endRead() {
    if (!readTxnActive_) return;
    db_.exec("ROLLBACK;");
    packIn_.reset();
    readTxnActive_ = false;}
void StorageArchive::rollback() {
    if (!writeTxnActive_) return;
    try {
        if (backendOpaque_) std::static_pointer_cast<StorageBackend>(backendOpaque_)->rollbackSession();
    } catch (...) {
        try { db_.exec("ROLLBACK;"); } catch (...) {}
        packIn_.reset();
        writeTxnActive_ = false;
        throw;}
    db_.exec("ROLLBACK;");
    packIn_.reset();
    writeTxnActive_ = false;}
void StorageArchive::prepareHotStatements() {
    auto prepare = [&](std::unique_ptr<Stmt>& out, const char* sql) {
        out = std::make_unique<Stmt>(db_.raw(), sql);
    };
    prepare(hot_.insertSnapshot, "INSERT INTO snapshots(created_at,source_root,label,backup_type) VALUES(?,?,?,?);");
    prepare(hot_.updateSnapshotCbtToken, "UPDATE snapshots SET cbt_token=? WHERE id=?;");
    prepare(hot_.insertFilePlaceholder, "INSERT INTO files(snapshot_id,path,size,mtime,file_hash) VALUES(?,?,?,?,NULL);");
    prepare(hot_.cloneFileInsert, "INSERT INTO files(snapshot_id,path,size,mtime,file_hash) VALUES(?,?,?,?,?);");
    prepare(hot_.cloneFileChunks, "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size,offset_in_chunk) SELECT ?,chunk_index,chunk_hash,raw_size,offset_in_chunk FROM file_chunks WHERE file_id=? ORDER BY chunk_index;");
    prepare(hot_.insertChunkIfMissing, "INSERT OR IGNORE INTO chunks(chunk_hash,raw_size,comp_size,comp_algo,pack_id,pack_offset,storage_state) VALUES(?,?,?,?,?,-1,'pending');");
    prepare(hot_.updateChunkOffset, "UPDATE chunks SET pack_id=?, pack_offset=?, storage_state='ready' WHERE chunk_hash=? AND storage_state='pending';");
    prepare(hot_.addFileChunk, "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size,offset_in_chunk) VALUES(?,?,?,?,?);");}
void StorageArchive::finalizeHotStatements() {
    hot_.insertSnapshot.reset();
    hot_.updateSnapshotCbtToken.reset();
    hot_.insertFilePlaceholder.reset();
    hot_.cloneFileInsert.reset();
    hot_.cloneFileChunks.reset();
    hot_.insertChunkIfMissing.reset();
    hot_.updateChunkOffset.reset();
    hot_.addFileChunk.reset();}
sqlite3_int64 StorageArchive::createSnapshot(const std::string& sourceRoot, const std::string& label, const std::string& backupType) {
    Stmt& st = *hot_.insertSnapshot;
    st.reset();
    st.bindText(1, nowIsoLocal());
    st.bindText(2, sourceRoot);
    if (label.empty()) st.bindNull(3);
    else st.bindText(3, label);
    st.bindText(4, backupType == "incremental" ? "incremental" : "full");
    st.stepDone();
    return db_.lastInsertId();}
std::optional<sqlite3_int64> StorageArchive::latestSnapshotId() {
    Stmt st(db_.raw(), "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1;");
    if (st.stepRow()) return sqlite3_column_int64(st.get(), 0);
    return std::nullopt;}
std::optional<sqlite3_int64> StorageArchive::previousSnapshotId() {
    Stmt st(db_.raw(), "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1 OFFSET 1;");
    if (st.stepRow()) return sqlite3_column_int64(st.get(), 0);
    return std::nullopt;}
std::optional<std::uint64_t> StorageArchive::latestSnapshotCbtToken() {
    Stmt st(db_.raw(), "SELECT cbt_token FROM snapshots ORDER BY id DESC LIMIT 1;");
    if (!st.stepRow()) return std::nullopt;
    return static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 0));}
sqlite3_int64 StorageArchive::resolveSnapshotId(const std::string& userInput) {
    const std::string input = lowerAscii(trim(userInput));
    if (input.empty() || input == "latest" || input == "last") {
        const auto l = latestSnapshotId();
        if (!l) throw std::runtime_error("Nenhum snapshot encontrado.");
        return *l;}
    if (input == "previous" || input == "prev") {
        const auto p = previousSnapshotId();
        if (!p) throw std::runtime_error("Nao existe snapshot anterior.");
        return *p;}
    try {
        const sqlite3_int64 id = static_cast<sqlite3_int64>(std::stoll(input));
        Stmt st(db_.raw(), "SELECT 1 FROM snapshots WHERE id=? LIMIT 1;");
        st.bindInt64(1, id);
        if (!st.stepRow()) throw std::runtime_error("Snapshot nao encontrado: " + userInput);
        return id;
    } catch (const std::invalid_argument&) {
        throw std::runtime_error("Identificador de snapshot invalido: " + userInput);
    } catch (const std::out_of_range&) {
        throw std::runtime_error("Identificador de snapshot fora do intervalo: " + userInput);}}
std::map<std::string, FileInfo> StorageArchive::loadSnapshotFileMap(sqlite3_int64 snapshotId) {
    std::map<std::string, FileInfo> out;
    Stmt st(db_.raw(), "SELECT id,path,size,mtime,file_hash FROM files WHERE snapshot_id=?;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) {
        FileInfo f;
        f.fileId = sqlite3_column_int64(st.get(), 0);
        f.size = sqlite3_column_int64(st.get(), 2);
        f.mtime = sqlite3_column_int64(st.get(), 3);
        f.fileHash = colBlob(st.get(), 4);
        out[colText(st.get(), 1)] = std::move(f);}
    return out;}
std::map<std::string, FileInfo> StorageArchive::loadLatestSnapshotFileMap() {
    const auto l = latestSnapshotId();
    if (!l) return {};
    return loadSnapshotFileMap(*l);}
sqlite3_int64 StorageArchive::insertFilePlaceholder(sqlite3_int64 snapshotId, const std::string& relPath, sqlite3_int64 size, sqlite3_int64 mtime) {
    Stmt& st = *hot_.insertFilePlaceholder;
    st.reset();
    st.bindInt64(1, snapshotId);
    st.bindText(2, relPath);
    st.bindInt64(3, size);
    st.bindInt64(4, mtime);
    st.stepDone();
    return db_.lastInsertId();}
void StorageArchive::updateFileHash(sqlite3_int64 fileId, const Blob& fileHash) {
    Stmt st(db_.raw(), "UPDATE files SET file_hash=? WHERE id=?;");
    if (fileHash.empty()) st.bindNull(1);
    else st.bindBlob(1, fileHash.data(), static_cast<int>(fileHash.size()));
    st.bindInt64(2, fileId);
    st.stepDone();
    if (db_.changes() != 1) throw std::runtime_error("Falha atualizando hash do arquivo.");}
void StorageArchive::deleteFileRecord(sqlite3_int64 fileId) {
    Stmt st(db_.raw(), "DELETE FROM files WHERE id=?;");
    st.bindInt64(1, fileId);
    st.stepDone();
    if (db_.changes() != 1) throw std::runtime_error("Falha removendo registro do arquivo.");}
sqlite3_int64 StorageArchive::cloneFileFromPrevious(sqlite3_int64 snapshotId, const std::string& relPath, const FileInfo& prev) {
    Stmt& ins = *hot_.cloneFileInsert;
    ins.reset();
    ins.bindInt64(1, snapshotId);
    ins.bindText(2, relPath);
    ins.bindInt64(3, prev.size);
    ins.bindInt64(4, prev.mtime);
    if (prev.fileHash.empty()) ins.bindNull(5);
    else ins.bindBlob(5, prev.fileHash.data(), static_cast<int>(prev.fileHash.size()));
    ins.stepDone();
    const sqlite3_int64 newId = db_.lastInsertId();
    Stmt& cl = *hot_.cloneFileChunks;
    cl.reset();
    cl.bindInt64(1, newId);
    cl.bindInt64(2, prev.fileId);
    cl.stepDone();
    return newId;}
bool StorageArchive::insertChunkIfMissing(const ChunkHash& hash, std::size_t rawSize, const std::vector<unsigned char>& comp, const std::string& compAlgo) {
    auto queryState = [&]() -> std::pair<std::string, sqlite3_int64> {
        Stmt st(db_.raw(), "SELECT storage_state, pack_offset FROM chunks WHERE chunk_hash=? LIMIT 1;");
        st.bindBlob(1, hash.data(), static_cast<int>(hash.size()));
        if (!st.stepRow()) return {"", -1};
        return {colText(st.get(), 0), sqlite3_column_int64(st.get(), 1)};
    };
    Stmt& ins = *hot_.insertChunkIfMissing;
    ins.reset();
    ins.bindBlob(1, hash.data(), static_cast<int>(hash.size()));
    ins.bindInt64(2, static_cast<sqlite3_int64>(rawSize));
    ins.bindInt64(3, static_cast<sqlite3_int64>(comp.size()));
    ins.bindText(4, compAlgo);
    ins.bindText(5, kDefaultPackId);
    ins.stepDone();
    const auto stateInfo = queryState();
    if (stateInfo.first == "ready" && stateInfo.second >= 0) return false;
    if (!backendOpaque_) throw std::runtime_error("Backend nao iniciado. Chame begin() antes de gravar chunks.");
    auto backend = std::static_pointer_cast<StorageBackend>(backendOpaque_);
    if (!backend) throw std::runtime_error("Backend invalido ou nao inicializado.");
    const sqlite3_int64 packOffset = backend->appendBlob(ProcessedBlob{hash, rawSize, comp, compAlgo});
    if (packOffset < 0) throw std::runtime_error("Backend retornou pack_offset invalido.");
    Stmt& upd = *hot_.updateChunkOffset;
    upd.reset();
    upd.bindText(1, kDefaultPackId);
    upd.bindInt64(2, packOffset);
    upd.bindBlob(3, hash.data(), static_cast<int>(hash.size()));
    upd.stepDone();
    if (sqlite3_changes(db_.raw()) != 1) {
        Stmt force(db_.raw(), "UPDATE chunks SET pack_id=?, pack_offset=?, storage_state='ready' WHERE chunk_hash=?;");
        force.bindText(1, kDefaultPackId);
        force.bindInt64(2, packOffset);
        force.bindBlob(3, hash.data(), static_cast<int>(hash.size()));
        force.stepDone();
        if (db_.changes() != 1) throw std::runtime_error("Falha atualizando metadata do chunk.");}
    return true;}
bool StorageArchive::hasChunk(const ChunkHash& hash) {
    Stmt chk(db_.raw(), "SELECT 1 FROM chunks WHERE chunk_hash=? AND storage_state='ready' AND pack_offset>=0 LIMIT 1;");
    chk.bindBlob(1, hash.data(), static_cast<int>(hash.size()));
    return chk.stepRow();}
void StorageArchive::addFileChunk(sqlite3_int64 fileId, int chunkIdx, const ChunkHash& chunkHash, std::size_t rawSize, std::size_t offsetInChunk) {
    Stmt& st = *hot_.addFileChunk;
    st.reset();
    st.bindInt64(1, fileId);
    st.bindInt(2, chunkIdx);
    st.bindBlob(3, chunkHash.data(), static_cast<int>(chunkHash.size()));
    st.bindInt64(4, static_cast<sqlite3_int64>(rawSize));
    st.bindInt64(5, static_cast<sqlite3_int64>(offsetInChunk));
    st.stepDone();}
void StorageArchive::addFileChunksBulk(sqlite3_int64 fileId, const std::vector<PendingFileChunk>& rows) {
    if (rows.empty()) return;
    Stmt st(db_.raw(), "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size,offset_in_chunk) VALUES(?,?,?,?,?);");
    for (const auto& r : rows) {
        st.reset();
        st.bindInt64(1, fileId);
        st.bindInt(2, r.chunkIdx);
        st.bindBlob(3, r.chunkHash.data(), static_cast<int>(r.chunkHash.size()));
        st.bindInt64(4, static_cast<sqlite3_int64>(r.rawSize));
        st.bindInt64(5, static_cast<sqlite3_int64>(r.offsetInChunk));
        st.stepDone();}}
std::optional<RestorableFileRef> StorageArchive::findFileBySnapshotAndPath(sqlite3_int64 snapshotId, const std::string& relPath) {
    Stmt st(db_.raw(), "SELECT id,size,mtime,file_hash FROM files WHERE snapshot_id=? AND path=?;");
    st.bindInt64(1, snapshotId);
    st.bindText(2, relPath);
    if (!st.stepRow()) return std::nullopt;
    RestorableFileRef out;
    out.fileId = sqlite3_column_int64(st.get(), 0);
    out.size = sqlite3_column_int64(st.get(), 1);
    out.mtime = sqlite3_column_int64(st.get(), 2);
    out.fileHash = colBlob(st.get(), 3);
    return out;}
std::vector<unsigned char> StorageArchive::readPackAt(sqlite3_int64 recordOffset, const ChunkHash& expectedSha, std::size_t expectedRawSize, std::size_t expectedCompSize, const std::string& expectedAlgo, const Blob& encryptIv) const {
    const fs::path p = describeArchiveStorage(path_).packPath;
    if (!packIn_ || !packIn_->is_open()) {
        packIn_ = std::make_unique<std::ifstream>(p, std::ios::binary);
        if (!packIn_ || !*packIn_) throw std::runtime_error("Falha abrindo pack para leitura.");}
    auto& in = *packIn_;
    in.clear();
    in.seekg(recordOffset, std::ios::beg);
    if (!in) throw std::runtime_error("Falha posicionando no pack.");
    std::uint64_t magic = 0;
    std::uint64_t raw64 = 0;
    std::uint64_t comp64 = 0;
    std::uint32_t algoLen = 0;
    ChunkHash ch{};
    in.read(reinterpret_cast<char*>(&magic), sizeof(magic));
    in.read(reinterpret_cast<char*>(&raw64), sizeof(raw64));
    in.read(reinterpret_cast<char*>(&comp64), sizeof(comp64));
    in.read(reinterpret_cast<char*>(&algoLen), sizeof(algoLen));
    in.read(reinterpret_cast<char*>(ch.data()), static_cast<std::streamsize>(ch.size()));
    if (!in) throw std::runtime_error("Falha lendo cabecalho do pack.");
    if (magic != kPackMagic) throw std::runtime_error("Magic invalida no pack.");
    if (raw64 != expectedRawSize) throw std::runtime_error("raw_size divergente no pack.");
    if (comp64 != expectedCompSize) throw std::runtime_error("comp_size divergente no pack.");
    if (ch != expectedSha) throw std::runtime_error("chunk_hash divergente no pack.");
    std::string algo(algoLen, '\0');
    if (algoLen > 0) in.read(algo.data(), static_cast<std::streamsize>(algoLen));
    if (!in) throw std::runtime_error("Falha lendo algoritmo no pack.");
    if (algo != expectedAlgo) throw std::runtime_error("Algoritmo divergente no pack.");
    Blob comp(expectedCompSize);
    if (expectedCompSize > 0) in.read(reinterpret_cast<char*>(comp.data()), static_cast<std::streamsize>(comp.size()));
    if (!in) throw std::runtime_error("Falha lendo blob comprimido do pack.");
    if (!encryptIv.empty()) {
        if (encryptIv.size() != 12) throw std::runtime_error("IV invalido: esperado 12 bytes para AES-GCM.");
        std::array<unsigned char, 32> key{};
        if (!Compactador::deriveKeyFromEnv(key))
            throw std::runtime_error("Chunk criptografado mas KEEPLY_BACKUP_KEY nao esta definida.");
        std::array<unsigned char, 12> iv{};
        std::memcpy(iv.data(), encryptIv.data(), 12);
        Blob plain;
        if (!Compactador::aesGcmDecrypt(key, iv, comp.data(), comp.size(), plain))
            throw std::runtime_error("Falha na autenticacao AES-256-GCM: chunk corrompido ou adulterado.");
        comp = std::move(plain);}
    Blob raw;
    if (algo == "zstd") Compactador::zstdDecompress(comp.data(), comp.size(), expectedRawSize, raw);
    else if (algo == "zlib") Compactador::zlibDecompress(comp.data(), comp.size(), expectedRawSize, raw);
    else throw std::runtime_error("Algoritmo de compressao nao suportado: " + algo);
    if (raw.size() != expectedRawSize) throw std::runtime_error("Tamanho bruto divergente apos descompressao.");
    return raw;}
std::vector<StoredChunkRow> StorageArchive::loadFileChunks(sqlite3_int64 fileId) {
    std::vector<StoredChunkRow> out;
    Stmt st(db_.raw(),
        "SELECT fc.chunk_index, fc.raw_size, fc.offset_in_chunk, "
               "c.raw_size, c.comp_size, c.comp_algo, "
               "c.pack_id, c.pack_offset, c.storage_state, fc.chunk_hash, c.encrypt_iv "
        "FROM file_chunks fc JOIN chunks c ON c.chunk_hash = fc.chunk_hash "
        "WHERE fc.file_id=? ORDER BY fc.chunk_index ASC;");
    st.bindInt64(1, fileId);
    while (st.stepRow()) {
        StoredChunkRow row;
        row.chunkIndex = sqlite3_column_int(st.get(), 0);
        const std::size_t sliceSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 1));
        const std::size_t sliceOffset = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 2));
        const std::size_t chunkRawSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 3));
        row.rawSize = sliceSize;
        row.compSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 4));
        row.algo = colText(st.get(), 5);
        const std::string packId = colText(st.get(), 6);
        const sqlite3_int64 packOffset = sqlite3_column_int64(st.get(), 7);
        const std::string storageState = colText(st.get(), 8);
        const void* hp = sqlite3_column_blob(st.get(), 9);
        const int hb = sqlite3_column_bytes(st.get(), 9);
        row.encryptIv = colBlob(st.get(), 10);
        if (storageState != "ready") throw std::runtime_error("Chunk ainda nao esta pronto para leitura.");
        if (packOffset < 0) throw std::runtime_error("Chunk com pack_offset invalido.");
        if (packId.empty()) throw std::runtime_error("Chunk sem pack_id.");
        if (!hp || hb != static_cast<int>(ChunkHash{}.size())) throw std::runtime_error("chunk_hash invalido vindo do SQLite.");
        if (sliceOffset + sliceSize > chunkRawSize) throw std::runtime_error("Slice de file_chunks fora dos limites do chunk.");
        ChunkHash ch{};
        std::memcpy(ch.data(), hp, ch.size());
        Blob full = readPackAt(packOffset, ch, chunkRawSize, row.compSize, row.algo, row.encryptIv);
        if (sliceOffset == 0 && sliceSize == chunkRawSize) row.blob = std::move(full);
        else row.blob.assign(full.begin() + sliceOffset, full.begin() + sliceOffset + sliceSize);
        out.push_back(std::move(row));}
    return out;}
void StorageArchive::streamFileChunks(sqlite3_int64 fileId,
                                      const std::function<void(const ChunkHash&, const Blob&)>& cb) {
    Stmt st(db_.raw(),
        "SELECT fc.raw_size, fc.offset_in_chunk, c.raw_size, c.comp_size, c.comp_algo, "
               "c.pack_id, c.pack_offset, c.storage_state, fc.chunk_hash, c.encrypt_iv "
        "FROM file_chunks fc JOIN chunks c ON c.chunk_hash = fc.chunk_hash "
        "WHERE fc.file_id=? ORDER BY fc.chunk_index ASC;");
    st.bindInt64(1, fileId);
    while (st.stepRow()) {
        const std::size_t sliceSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 0));
        const std::size_t sliceOffset = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 1));
        const std::size_t chunkRawSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 2));
        const std::size_t compSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 3));
        const std::string algo = colText(st.get(), 4);
        const std::string packId = colText(st.get(), 5);
        const sqlite3_int64 packOffset = sqlite3_column_int64(st.get(), 6);
        const std::string storageState = colText(st.get(), 7);
        const void* hp = sqlite3_column_blob(st.get(), 8);
        const int hb = sqlite3_column_bytes(st.get(), 8);
        const Blob encryptIv = colBlob(st.get(), 9);
        if (storageState != "ready") throw std::runtime_error("Chunk ainda nao esta pronto para leitura.");
        if (packOffset < 0) throw std::runtime_error("Chunk com pack_offset invalido.");
        if (packId.empty()) throw std::runtime_error("Chunk sem pack_id.");
        if (!hp || hb != static_cast<int>(ChunkHash{}.size())) throw std::runtime_error("chunk_hash invalido vindo do SQLite.");
        if (sliceOffset + sliceSize > chunkRawSize) throw std::runtime_error("Slice de file_chunks fora dos limites do chunk.");
        ChunkHash ch{};
        std::memcpy(ch.data(), hp, ch.size());
        Blob full = readPackAt(packOffset, ch, chunkRawSize, compSize, algo, encryptIv);
        if (sliceOffset == 0 && sliceSize == chunkRawSize) {
            cb(ch, full);
        } else {
            Blob slice(full.begin() + sliceOffset, full.begin() + sliceOffset + sliceSize);
            cb(ch, slice);}}}
std::vector<std::string> StorageArchive::listSnapshotPaths(sqlite3_int64 snapshotId) {
    std::vector<std::string> paths;
    Stmt st(db_.raw(), "SELECT path FROM files WHERE snapshot_id=? ORDER BY path;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) paths.emplace_back(colText(st.get(), 0));
    return paths;}
void StorageArchive::updateSnapshotCbtToken(sqlite3_int64 snapshotId, std::uint64_t token) {
    Stmt& st = *hot_.updateSnapshotCbtToken;
    st.reset();
    st.bindInt64(1, static_cast<sqlite3_int64>(token));
    st.bindInt64(2, snapshotId);
    st.stepDone();
    if (sqlite3_changes(db_.raw()) != 1) throw std::runtime_error("Falha atualizando cbt_token do snapshot.");}
std::vector<SnapshotRow> StorageArchive::listSnapshots() {
    std::vector<SnapshotRow> out;
    Stmt st(db_.raw(), "SELECT s.id,s.created_at,s.source_root,COALESCE(s.label,''),COALESCE(NULLIF(TRIM(s.backup_type),''),CASE WHEN s.id=(SELECT MIN(id) FROM snapshots) THEN 'full' ELSE 'incremental' END),COUNT(f.id) FROM snapshots s LEFT JOIN files f ON f.snapshot_id=s.id GROUP BY s.id,s.created_at,s.source_root,s.label,s.backup_type ORDER BY s.id ASC;");
    while (st.stepRow()) {
        SnapshotRow row;
        row.id = sqlite3_column_int64(st.get(), 0);
        row.createdAt = colText(st.get(), 1);
        row.sourceRoot = colText(st.get(), 2);
        row.label = colText(st.get(), 3);
        row.backupType = colText(st.get(), 4);
        row.fileCount = sqlite3_column_int64(st.get(), 5);
        out.push_back(std::move(row));}
    return out;}
std::vector<ChangeEntry> StorageArchive::diffSnapshots(sqlite3_int64 olderSnapshotId, sqlite3_int64 newerSnapshotId) {
    const auto olderMap = loadSnapshotFileMap(olderSnapshotId);
    const auto newerMap = loadSnapshotFileMap(newerSnapshotId);
    std::set<std::string> allPaths;
    for (const auto& [p, _] : olderMap) allPaths.insert(p);
    for (const auto& [p, _] : newerMap) allPaths.insert(p);
    std::vector<ChangeEntry> out;
    for (const auto& path : allPaths) {
        const auto oi = olderMap.find(path);
        const auto ni = newerMap.find(path);
        if (oi == olderMap.end()) {
            ChangeEntry e{path, "added", 0, ni->second.size, 0, ni->second.mtime, {}, ni->second.fileHash};
            out.push_back(std::move(e));
            continue;}
        if (ni == newerMap.end()) {
            ChangeEntry e{path, "removed", oi->second.size, 0, oi->second.mtime, 0, oi->second.fileHash, {}};
            out.push_back(std::move(e));
            continue;}
        if (oi->second.size != ni->second.size || oi->second.mtime != ni->second.mtime || oi->second.fileHash != ni->second.fileHash) {
            ChangeEntry e{path, "modified", oi->second.size, ni->second.size, oi->second.mtime, ni->second.mtime, oi->second.fileHash, ni->second.fileHash};
            out.push_back(std::move(e));}}
    return out;}
std::vector<ChangeEntry> StorageArchive::diffLatestVsPrevious() {
    const auto newer = latestSnapshotId();
    const auto older = previousSnapshotId();
    if (!newer || !older) return {};
    return diffSnapshots(*older, *newer);}
void StorageArchive::setChunkEncryptIv(const ChunkHash& hash, const Blob& iv) {
    Stmt st(db_.raw(), "UPDATE chunks SET encrypt_iv=? WHERE chunk_hash=?;");
    if (iv.empty()) st.bindNull(1);
    else st.bindBlob(1, iv.data(), static_cast<int>(iv.size()));
    st.bindBlob(2, hash.data(), static_cast<int>(hash.size()));
    st.stepDone();}
void StorageArchive::saveFileSignature(sqlite3_int64 snapshotId, const std::string& path, const Blob& sigBlob) {
    Stmt st(db_.raw(), "INSERT OR REPLACE INTO file_signatures(snapshot_id, path, sig_blob) VALUES(?,?,?);");
    st.bindInt64(1, snapshotId);
    st.bindText(2, path);
    st.bindBlob(3, sigBlob.data(), static_cast<int>(sigBlob.size()));
    st.stepDone();}
std::optional<Blob> StorageArchive::loadFileSignature(const std::string& path) {
    Stmt st(db_.raw(), "SELECT sig_blob FROM file_signatures WHERE path=? ORDER BY id DESC LIMIT 1;");
    st.bindText(1, path);
    if (!st.stepRow()) return std::nullopt;
    return colBlob(st.get(), 0);}
StorageArchive::CloudUploadPlan StorageArchive::prepareCloudUpload(const std::string& deviceExternalId) {
    CloudUploadPlan plan;
    const ArchiveStoragePaths paths = describeArchiveStorage(path_);
    plan.archiveDbPath = paths.archivePath;
    plan.packPath = paths.packPath;
    {
        const std::string suffix = safeFileComponent(deviceExternalId);
        plan.bundleId = suffix.empty() ? std::string("bundle") : ("bundle-" + suffix);
    }
    {
        Stmt st(db_.raw(), "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1;");
        plan.latestSnapshotId = st.stepRow() ? sqlite3_column_int64(st.get(), 0) : 0;
    }
    Stmt st(db_.raw(),
        "SELECT chunk_hash, comp_size, comp_algo, pack_offset FROM chunks "
        "WHERE storage_state='ready' AND pack_offset >= 0 AND pack_id=? ORDER BY pack_offset ASC;");
    st.bindText(1, kDefaultPackId);
    while (st.stepRow()) {
        CloudChunkRef ref{};
        const void* hp = sqlite3_column_blob(st.get(), 0);
        const int hb = sqlite3_column_bytes(st.get(), 0);
        if (!hp || hb != static_cast<int>(ref.hash.size()))
            throw std::runtime_error("chunk_hash invalido ao preparar upload cloud.");
        std::memcpy(ref.hash.data(), hp, ref.hash.size());
        const std::uint64_t compSize = static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 1));
        const std::string algo = colText(st.get(), 2);
        ref.packOffset = static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 3));
        ref.recordSize = static_cast<std::uint64_t>(8 + 8 + 8 + 4) + ref.hash.size() + algo.size() + compSize;
        plan.chunks.push_back(ref);}
    return plan;}
std::vector<unsigned char> StorageArchive::readPackRecord(const CloudChunkRef& ref) const {
    const fs::path packPath = describeArchiveStorage(path_).packPath;
    std::ifstream in(packPath, std::ios::binary);
    if (!in) throw std::runtime_error("Falha abrindo pack para leitura cloud.");
    in.seekg(static_cast<std::streamoff>(ref.packOffset), std::ios::beg);
    if (!in) throw std::runtime_error("Falha posicionando no pack para leitura de record.");
    std::vector<unsigned char> out(static_cast<std::size_t>(ref.recordSize));
    if (ref.recordSize > 0) {
        in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(ref.recordSize));
        if (!in) throw std::runtime_error("Falha lendo record do pack.");}
    return out;}
std::vector<ChunkHash> StorageArchive::snapshotChunkHashes(sqlite3_int64 snapshotId) {
    std::vector<ChunkHash> out;
    Stmt st(db_.raw(),
        "SELECT DISTINCT fc.chunk_hash FROM file_chunks fc "
        "JOIN files f ON f.id = fc.file_id WHERE f.snapshot_id = ?;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) {
        const void* hp = sqlite3_column_blob(st.get(), 0);
        const int hb = sqlite3_column_bytes(st.get(), 0);
        if (!hp || hb != static_cast<int>(ChunkHash{}.size())) continue;
        ChunkHash ch{};
        std::memcpy(ch.data(), hp, ch.size());
        out.push_back(ch);}
    return out;}
void StorageArchive::setChunkPackOffsetForRestore(const ChunkHash& hash, std::uint64_t newOffset) {
    Stmt st(db_.raw(), "UPDATE chunks SET pack_id=?, pack_offset=?, storage_state='ready' WHERE chunk_hash=?;");
    st.bindText(1, kDefaultPackId);
    st.bindInt64(2, static_cast<sqlite3_int64>(newOffset));
    st.bindBlob(3, hash.data(), static_cast<int>(hash.size()));
    st.stepDone();}
}

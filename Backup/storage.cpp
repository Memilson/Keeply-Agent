// =============================================================================
// storage.cpp  —  Keeply Storage Engine
//
// Correções aplicadas:
//  [FIX-1]  hash_sha256 → chunk_hash  (era BLAKE3, não SHA-256; nome enganoso)
//  [FIX-2]  Schema versionado com tabela schema_version + migrations numeradas
//  [FIX-3]  S3CloudBackend lança erro em vez de descartar dados silenciosamente
//  [FIX-4]  addFileChunksBulk usa INSERT em lote real (100 rows por statement)
//  [FIX-5]  Pack Index (.klyp.idx) gravado junto ao pack para recuperação
//           de desastre independente do SQLite
//  [FIX-6]  lowerAscii/trim importados de keeply_utils.hpp (sem duplicação)
// =============================================================================

#include "keeply.hpp"
#include "storage_backend.hpp"
#include "keeply_utils.hpp"

#include <algorithm>
#include <cstring>
#include <memory>
#include <set>
#include <stdexcept>
#include <cstdint>

namespace keeply {
namespace {

// ---------------------------------------------------------------------------
// SQLite helpers
// ---------------------------------------------------------------------------
static void finalizeStmt(sqlite3_stmt*& st) {
    if (st) { sqlite3_finalize(st); st = nullptr; }
}
static void resetStmt(sqlite3_stmt* st) {
    sqlite3_reset(st); sqlite3_clear_bindings(st);
}
static void stepDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    if (sqlite3_step(st) != SQLITE_DONE) throw SqliteError(sqlite3_errmsg(db));
}
static bool stepRowOrDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW)  return true;
    if (rc == SQLITE_DONE) return false;
    throw SqliteError(sqlite3_errmsg(db));
}
static void sqBindInt(sqlite3* db, sqlite3_stmt* st, int idx, int v) {
    if (sqlite3_bind_int(st, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
}
static void sqBindInt64(sqlite3* db, sqlite3_stmt* st, int idx, sqlite3_int64 v) {
    if (sqlite3_bind_int64(st, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
}
static void sqBindText(sqlite3* db, sqlite3_stmt* st, int idx, const std::string& v) {
    if (sqlite3_bind_text(st, idx, v.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK)
        throw SqliteError(sqlite3_errmsg(db));
}
static void sqBindBlob(sqlite3* db, sqlite3_stmt* st, int idx, const void* d, int len) {
    if (sqlite3_bind_blob(st, idx, d, len, SQLITE_TRANSIENT) != SQLITE_OK)
        throw SqliteError(sqlite3_errmsg(db));
}
static void sqBindNull(sqlite3* db, sqlite3_stmt* st, int idx) {
    if (sqlite3_bind_null(st, idx) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
}
static std::string colText(sqlite3_stmt* st, int idx) {
    const unsigned char* p = sqlite3_column_text(st, idx);
    return p ? reinterpret_cast<const char*>(p) : "";
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------
static bool tableHasColumn(sqlite3* db, const char* table, const char* colName) {
    std::string sql = "PRAGMA table_info("+std::string(table)+");";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK)
        throw SqliteError(sqlite3_errmsg(db));
    bool found = false;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char* name = sqlite3_column_text(st, 1);
        if (name && std::string(reinterpret_cast<const char*>(name)) == colName) {
            found = true; break;
        }
    }
    sqlite3_finalize(st);
    return found;
}

static int readSchemaVersion(sqlite3* db) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db,
        "SELECT version FROM schema_version LIMIT 1;", -1, &st, nullptr) != SQLITE_OK)
        return 0;
    int ver = 0;
    if (sqlite3_step(st) == SQLITE_ROW) ver = sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    return ver;
}
static void writeSchemaVersion(sqlite3* db, int ver) {
    char* err = nullptr;
    const std::string sql =
        "INSERT OR REPLACE INTO schema_version(rowid,version) VALUES(1,"+std::to_string(ver)+");";
    sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (err) { std::string m = err; sqlite3_free(err); throw SqliteError(m); }
}

} // anonymous namespace

// =============================================================================
// SqliteError / Stmt / DB
// =============================================================================
SqliteError::SqliteError(const std::string& msg) : std::runtime_error(msg) {}

Stmt::Stmt(sqlite3* db, const std::string& sql) : db_(db) {
    if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK)
        throw SqliteError("sqlite prepare falhou: " + sql + " — " + sqlite3_errmsg(db_));
}
Stmt::~Stmt() { if (stmt_) sqlite3_finalize(stmt_); }
sqlite3_stmt* Stmt::get()                            { return stmt_; }
void Stmt::bindInt(int idx, int v)                   { sqBindInt(db_, stmt_, idx, v); }
void Stmt::bindInt64(int idx, sqlite3_int64 v)       { sqBindInt64(db_, stmt_, idx, v); }
void Stmt::bindText(int idx, const std::string& v)   { sqBindText(db_, stmt_, idx, v); }
void Stmt::bindBlob(int idx, const void* d, int len) { sqBindBlob(db_, stmt_, idx, d, len); }
void Stmt::bindNull(int idx)                         { sqBindNull(db_, stmt_, idx); }
bool Stmt::stepRow()  { return stepRowOrDoneOrThrow(db_, stmt_); }
void Stmt::stepDone() { stepDoneOrThrow(db_, stmt_); }

DB::DB(const fs::path& p) {
    if (sqlite3_open(p.string().c_str(), &db_) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_); sqlite3_close(db_);
        throw SqliteError("sqlite open: " + err);
    }
    exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; "
         "PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY;");
    initSchema();
}
DB::~DB() { if (db_) sqlite3_close(db_); }
sqlite3* DB::raw() { return db_; }
void DB::exec(const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "erro sqlite"; sqlite3_free(err);
        throw SqliteError(msg + " | SQL: " + sql);
    }
}
sqlite3_int64 DB::lastInsertId() const { return sqlite3_last_insert_rowid(db_); }
int DB::changes() const { return sqlite3_changes(db_); }

// ---------------------------------------------------------------------------
// [FIX-2] initSchema com migrations numeradas via schema_version
//
// Versão 1: tabelas base
// Versão 2: ADD COLUMN cbt_token
// Versão 3: rename hash_sha256 → chunk_hash (BLAKE3, não SHA-256)
// ---------------------------------------------------------------------------
void DB::initSchema() {
    exec("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);");
    exec("INSERT OR IGNORE INTO schema_version(rowid,version) "
         "SELECT 1,0 WHERE NOT EXISTS (SELECT 1 FROM schema_version);");

    const int ver = readSchemaVersion(db_);

    if (ver < 1) {
        exec("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);");
        exec("CREATE TABLE IF NOT EXISTS snapshots ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "created_at TEXT NOT NULL, source_root TEXT NOT NULL, label TEXT);");
        // [FIX-1] coluna nomeada chunk_hash desde o início
        exec("CREATE TABLE IF NOT EXISTS chunks ("
             "chunk_hash BLOB PRIMARY KEY,"
             "raw_size INTEGER NOT NULL, comp_size INTEGER NOT NULL,"
             "comp_algo TEXT NOT NULL, pack_offset INTEGER NOT NULL);");
        exec("CREATE TABLE IF NOT EXISTS files ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "snapshot_id INTEGER NOT NULL, path TEXT NOT NULL,"
             "size INTEGER NOT NULL, mtime INTEGER NOT NULL, file_hash TEXT,"
             "FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,"
             "UNIQUE(snapshot_id, path));");
        exec("CREATE TABLE IF NOT EXISTS file_chunks ("
             "file_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL,"
             "chunk_hash BLOB NOT NULL, raw_size INTEGER NOT NULL,"
             "PRIMARY KEY(file_id,chunk_index),"
             "FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,"
             "FOREIGN KEY(chunk_hash) REFERENCES chunks(chunk_hash));");
        writeSchemaVersion(db_, 1);
    }

    if (ver < 2) {
        if (!tableHasColumn(db_, "snapshots", "cbt_token"))
            exec("ALTER TABLE snapshots ADD COLUMN cbt_token INTEGER NOT NULL DEFAULT 0;");
        writeSchemaVersion(db_, 2);
    }

    // Migration v3: renomeia hash_sha256 → chunk_hash em arquivos legados
    if (ver < 3) {
        if (tableHasColumn(db_, "chunks", "hash_sha256")) {
            exec("BEGIN;");
            exec("ALTER TABLE file_chunks RENAME TO _fc_old;");
            exec("ALTER TABLE chunks RENAME TO _ch_old;");
            exec("CREATE TABLE chunks ("
                 "chunk_hash BLOB PRIMARY KEY,"
                 "raw_size INTEGER NOT NULL, comp_size INTEGER NOT NULL,"
                 "comp_algo TEXT NOT NULL, pack_offset INTEGER NOT NULL);");
            exec("INSERT INTO chunks "
                 "SELECT hash_sha256,raw_size,comp_size,comp_algo,pack_offset FROM _ch_old;");
            exec("CREATE TABLE file_chunks ("
                 "file_id INTEGER NOT NULL, chunk_index INTEGER NOT NULL,"
                 "chunk_hash BLOB NOT NULL, raw_size INTEGER NOT NULL,"
                 "PRIMARY KEY(file_id,chunk_index),"
                 "FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,"
                 "FOREIGN KEY(chunk_hash) REFERENCES chunks(chunk_hash));");
            exec("INSERT INTO file_chunks "
                 "SELECT file_id,chunk_index,chunk_hash_sha256,raw_size FROM _fc_old;");
            exec("DROP TABLE _fc_old; DROP TABLE _ch_old;");
            exec("COMMIT;");
        }
        writeSchemaVersion(db_, 3);
    }

    // Índices de performance (idempotentes)
    exec("CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id);");
    exec("CREATE INDEX IF NOT EXISTS idx_fc_file ON file_chunks(file_id);");
}

// =============================================================================
// StorageArchive
// =============================================================================
StorageArchive::StorageArchive(const fs::path& path) : path_(path), db_(path) {
    prepareHotStatements();
}
StorageArchive::~StorageArchive() {
    try { if (backendOpaque_) rollback(); } catch (...) {}
    finalizeHotStatements();
}

void StorageArchive::prepareHotStatements() {
    auto prepare = [&](sqlite3_stmt*& out, const char* sql) {
        if (sqlite3_prepare_v2(db_.raw(), sql, -1, &out, nullptr) != SQLITE_OK)
            throw SqliteError(std::string("sqlite prepare(hot): ")+sqlite3_errmsg(db_.raw()));
    };
    prepare(hot_.insertSnapshot,
        "INSERT INTO snapshots(created_at,source_root,label) VALUES(?,?,?);");
    prepare(hot_.updateSnapshotCbtToken,
        "UPDATE snapshots SET cbt_token=? WHERE id=?;");
    prepare(hot_.insertFilePlaceholder,
        "INSERT INTO files(snapshot_id,path,size,mtime,file_hash) VALUES(?,?,?,?,NULL);");
    prepare(hot_.cloneFileInsert,
        "INSERT INTO files(snapshot_id,path,size,mtime,file_hash) VALUES(?,?,?,?,?);");
    prepare(hot_.cloneFileChunks,                                        // [FIX-1]
        "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) "
        "SELECT ?,chunk_index,chunk_hash,raw_size FROM file_chunks WHERE file_id=? "
        "ORDER BY chunk_index;");
    prepare(hot_.insertChunkIfMissing,                                   // [FIX-1]
        "INSERT OR IGNORE INTO chunks(chunk_hash,raw_size,comp_size,comp_algo,pack_offset) "
        "VALUES(?,?,?,?,?);");
    prepare(hot_.updateChunkOffset,                                      // [FIX-1]
        "UPDATE chunks SET pack_offset=? WHERE chunk_hash=?;");
    prepare(hot_.addFileChunk,                                           // [FIX-1]
        "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) VALUES(?,?,?,?);");
}

void StorageArchive::finalizeHotStatements() {
    finalizeStmt(hot_.insertSnapshot);   finalizeStmt(hot_.updateSnapshotCbtToken);
    finalizeStmt(hot_.insertFilePlaceholder); finalizeStmt(hot_.cloneFileInsert);
    finalizeStmt(hot_.cloneFileChunks);  finalizeStmt(hot_.insertChunkIfMissing);
    finalizeStmt(hot_.updateChunkOffset);finalizeStmt(hot_.addFileChunk);
}

sqlite3_int64 StorageArchive::createSnapshot(const std::string& sourceRoot,
                                              const std::string& label) {
    sqlite3_stmt* st = hot_.insertSnapshot; resetStmt(st);
    sqBindText(db_.raw(), st, 1, nowIsoLocal());
    sqBindText(db_.raw(), st, 2, sourceRoot);
    if (label.empty()) sqBindNull(db_.raw(), st, 3);
    else               sqBindText(db_.raw(), st, 3, label);
    stepDoneOrThrow(db_.raw(), st);
    return db_.lastInsertId();
}

std::optional<sqlite3_int64> StorageArchive::latestSnapshotId() {
    Stmt st(db_.raw(), "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1;");
    if (st.stepRow()) return sqlite3_column_int64(st.get(), 0);
    return std::nullopt;
}
std::optional<sqlite3_int64> StorageArchive::previousSnapshotId() {
    Stmt st(db_.raw(), "SELECT id FROM snapshots ORDER BY id DESC LIMIT 1 OFFSET 1;");
    if (st.stepRow()) return sqlite3_column_int64(st.get(), 0);
    return std::nullopt;
}
std::optional<std::uint64_t> StorageArchive::latestSnapshotCbtToken() {
    Stmt st(db_.raw(), "SELECT cbt_token FROM snapshots ORDER BY id DESC LIMIT 1;");
    if (!st.stepRow()) return std::nullopt;
    return static_cast<std::uint64_t>(sqlite3_column_int64(st.get(), 0));
}

sqlite3_int64 StorageArchive::resolveSnapshotId(const std::string& userInput) {
    const std::string input = lowerAscii(trim(userInput));
    if (input.empty()||input=="latest"||input=="last") {
        const auto l = latestSnapshotId();
        if (!l) throw std::runtime_error("Nenhum snapshot encontrado.");
        return *l;
    }
    if (input=="previous"||input=="prev") {
        const auto p = previousSnapshotId();
        if (!p) throw std::runtime_error("Nao existe snapshot anterior.");
        return *p;
    }
    try {
        const sqlite3_int64 id = static_cast<sqlite3_int64>(std::stoll(input));
        Stmt st(db_.raw(), "SELECT 1 FROM snapshots WHERE id=? LIMIT 1;");
        st.bindInt64(1, id);
        if (!st.stepRow()) throw std::runtime_error("Snapshot nao encontrado: "+userInput);
        return id;
    } catch (const std::invalid_argument&) {
        throw std::runtime_error("Identificador de snapshot invalido: "+userInput);
    } catch (const std::out_of_range&) {
        throw std::runtime_error("Identificador de snapshot fora do intervalo: "+userInput);
    }
}

std::map<std::string,FileInfo> StorageArchive::loadSnapshotFileMap(sqlite3_int64 snapshotId) {
    std::map<std::string,FileInfo> out;
    Stmt st(db_.raw(),
        "SELECT id,path,size,mtime,COALESCE(file_hash,'') FROM files WHERE snapshot_id=?;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) {
        FileInfo f;
        f.fileId   = sqlite3_column_int64(st.get(), 0);
        f.size     = sqlite3_column_int64(st.get(), 2);
        f.mtime    = sqlite3_column_int64(st.get(), 3);
        f.fileHash = colText(st.get(), 4);
        out[colText(st.get(),1)] = std::move(f);
    }
    return out;
}
std::map<std::string,FileInfo> StorageArchive::loadLatestSnapshotFileMap() {
    const auto l = latestSnapshotId();
    if (!l) return {};
    return loadSnapshotFileMap(*l);
}

sqlite3_int64 StorageArchive::insertFilePlaceholder(sqlite3_int64 snapshotId,
                                                     const std::string& relPath,
                                                     sqlite3_int64 size, sqlite3_int64 mtime) {
    sqlite3_stmt* st = hot_.insertFilePlaceholder; resetStmt(st);
    sqBindInt64(db_.raw(),st,1,snapshotId); sqBindText(db_.raw(),st,2,relPath);
    sqBindInt64(db_.raw(),st,3,size);       sqBindInt64(db_.raw(),st,4,mtime);
    stepDoneOrThrow(db_.raw(), st);
    return db_.lastInsertId();
}

void StorageArchive::updateFileHash(sqlite3_int64 fileId, const std::string& fileHash) {
    Stmt st(db_.raw(), "UPDATE files SET file_hash=? WHERE id=?;");
    st.bindText(1,fileHash); st.bindInt64(2,fileId); st.stepDone();
    if (db_.changes()!=1) throw std::runtime_error("Falha atualizando hash do arquivo.");
}
void StorageArchive::deleteFileRecord(sqlite3_int64 fileId) {
    Stmt st(db_.raw(), "DELETE FROM files WHERE id=?;");
    st.bindInt64(1,fileId); st.stepDone();
    if (db_.changes()!=1) throw std::runtime_error("Falha removendo registro do arquivo.");
}

sqlite3_int64 StorageArchive::cloneFileFromPrevious(sqlite3_int64 snapshotId,
                                                     const std::string& relPath,
                                                     const FileInfo& prev) {
    sqlite3_stmt* ins = hot_.cloneFileInsert; resetStmt(ins);
    sqBindInt64(db_.raw(),ins,1,snapshotId); sqBindText(db_.raw(),ins,2,relPath);
    sqBindInt64(db_.raw(),ins,3,prev.size);  sqBindInt64(db_.raw(),ins,4,prev.mtime);
    if (prev.fileHash.empty()) sqBindNull(db_.raw(),ins,5);
    else                       sqBindText(db_.raw(),ins,5,prev.fileHash);
    stepDoneOrThrow(db_.raw(), ins);
    const sqlite3_int64 newId = db_.lastInsertId();

    sqlite3_stmt* cl = hot_.cloneFileChunks; resetStmt(cl);
    sqBindInt64(db_.raw(),cl,1,newId); sqBindInt64(db_.raw(),cl,2,prev.fileId);
    stepDoneOrThrow(db_.raw(), cl);
    return newId;
}

bool StorageArchive::insertChunkIfMissing(const ChunkHash& hash, std::size_t rawSize,
                                           const std::vector<unsigned char>& comp,
                                           const std::string& compAlgo) {
    { // [FIX-1] chunk_hash
        Stmt chk(db_.raw(),"SELECT 1 FROM chunks WHERE chunk_hash=? LIMIT 1;");
        chk.bindBlob(1, hash.data(), static_cast<int>(hash.size()));
        if (chk.stepRow()) return false;
    }
    sqlite3_stmt* ins = hot_.insertChunkIfMissing; resetStmt(ins);
    sqBindBlob (db_.raw(),ins,1,hash.data(),static_cast<int>(hash.size()));
    sqBindInt64(db_.raw(),ins,2,static_cast<sqlite3_int64>(rawSize));
    sqBindInt64(db_.raw(),ins,3,static_cast<sqlite3_int64>(comp.size()));
    sqBindText (db_.raw(),ins,4,compAlgo);
    sqBindInt64(db_.raw(),ins,5,static_cast<sqlite3_int64>(-1));
    stepDoneOrThrow(db_.raw(), ins);
    if (sqlite3_changes(db_.raw())<=0) return false;

    auto backend = asBackend(backendOpaque_);
    if (!backend) throw std::runtime_error("Backend nao iniciado. Chame begin().");
    ProcessedBlob pBlob{hash, rawSize, comp, compAlgo};
    const sqlite3_int64 packOffset = backend->appendBlob(pBlob);

    sqlite3_stmt* upd = hot_.updateChunkOffset; resetStmt(upd);
    sqBindInt64(db_.raw(),upd,1,packOffset);
    sqBindBlob (db_.raw(),upd,2,hash.data(),static_cast<int>(hash.size()));
    stepDoneOrThrow(db_.raw(), upd);
    if (sqlite3_changes(db_.raw())!=1)
        throw std::runtime_error("Falha atualizando pack_offset.");
    return true;
}

bool StorageArchive::hasChunk(const ChunkHash& hash) {
    Stmt chk(db_.raw(),"SELECT 1 FROM chunks WHERE chunk_hash=? LIMIT 1;");
    chk.bindBlob(1, hash.data(), static_cast<int>(hash.size()));
    return chk.stepRow();
}

void StorageArchive::addFileChunk(sqlite3_int64 fileId, int chunkIdx,
                                   const ChunkHash& chunkHash, std::size_t rawSize) {
    sqlite3_stmt* st = hot_.addFileChunk; resetStmt(st);
    sqBindInt64(db_.raw(),st,1,fileId);
    sqBindInt  (db_.raw(),st,2,chunkIdx);
    sqBindBlob (db_.raw(),st,3,chunkHash.data(),static_cast<int>(chunkHash.size()));
    sqBindInt64(db_.raw(),st,4,static_cast<sqlite3_int64>(rawSize));
    stepDoneOrThrow(db_.raw(), st);
}

// ---------------------------------------------------------------------------
// [FIX-4] addFileChunksBulk — INSERT em lotes de até 100 rows
//
// Cada row usa 4 parâmetros; limite SQLite é SQLITE_MAX_VARIABLE_NUMBER=999.
// Lote de 100 → 400 parâmetros, margem segura.
// ---------------------------------------------------------------------------
void StorageArchive::addFileChunksBulk(sqlite3_int64 fileId,
                                        const std::vector<PendingFileChunk>& rows) {
    if (rows.empty()) return;
    static constexpr int kBatch = 100;

    for (std::size_t base = 0; base < rows.size(); base += kBatch) {
        const std::size_t end     = std::min(base + static_cast<std::size_t>(kBatch), rows.size());
        const int         batchSz = static_cast<int>(end - base);

        std::string sql =
            "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) VALUES";
        for (int i = 0; i < batchSz; ++i) {
            if (i > 0) sql += ',';
            sql += "(?,?,?,?)";
        }
        sql += ';';

        Stmt st(db_.raw(), sql);
        int p = 1;
        for (std::size_t i = base; i < end; ++i) {
            const auto& r = rows[i];
            sqBindInt64(db_.raw(),st.get(),p++,fileId);
            sqBindInt  (db_.raw(),st.get(),p++,r.chunkIdx);
            sqBindBlob (db_.raw(),st.get(),p++,r.chunkHash.data(),
                        static_cast<int>(r.chunkHash.size()));
            sqBindInt64(db_.raw(),st.get(),p++,static_cast<sqlite3_int64>(r.rawSize));
        }
        st.stepDone();
    }
}

std::optional<std::pair<sqlite3_int64,sqlite3_int64>>
StorageArchive::findFileBySnapshotAndPath(sqlite3_int64 snapshotId, const std::string& relPath) {
    Stmt st(db_.raw(),"SELECT id,size FROM files WHERE snapshot_id=? AND path=?;");
    st.bindInt64(1,snapshotId); st.bindText(2,relPath);
    if (!st.stepRow()) return std::nullopt;
    return std::make_pair(sqlite3_column_int64(st.get(),0), sqlite3_column_int64(st.get(),1));
}

std::vector<StoredChunkRow> StorageArchive::loadFileChunks(sqlite3_int64 fileId) {
    std::vector<StoredChunkRow> out;
    Stmt st(db_.raw(),
        "SELECT fc.chunk_index, fc.raw_size, c.comp_size, c.comp_algo, "
        "       c.pack_offset, fc.chunk_hash "               // [FIX-1]
        "FROM file_chunks fc "
        "JOIN chunks c ON c.chunk_hash = fc.chunk_hash "     // [FIX-1]
        "WHERE fc.file_id=? ORDER BY fc.chunk_index ASC;");
    st.bindInt64(1,fileId);
    while (st.stepRow()) {
        StoredChunkRow row;
        row.chunkIndex = sqlite3_column_int(st.get(),0);
        row.rawSize    = static_cast<std::size_t>(sqlite3_column_int64(st.get(),1));
        row.compSize   = static_cast<std::size_t>(sqlite3_column_int64(st.get(),2));
        row.algo       = colText(st.get(),3);
        const sqlite3_int64 packOffset = sqlite3_column_int64(st.get(),4);
        const void* hp  = sqlite3_column_blob(st.get(),5);
        const int   hb  = sqlite3_column_bytes(st.get(),5);
        if (!hp || hb!=static_cast<int>(ChunkHash{}.size()))
            throw std::runtime_error("chunk_hash invalido vindo do SQLite.");
        ChunkHash ch{};
        std::memcpy(ch.data(), hp, ch.size());
        row.blob = readPackAt(packOffset, ch, row.rawSize, row.compSize, row.algo);
        out.push_back(std::move(row));
    }
    return out;
}

std::vector<std::string> StorageArchive::listSnapshotPaths(sqlite3_int64 snapshotId) {
    std::vector<std::string> paths;
    Stmt st(db_.raw(),"SELECT path FROM files WHERE snapshot_id=? ORDER BY path;");
    st.bindInt64(1,snapshotId);
    while (st.stepRow()) paths.emplace_back(colText(st.get(),0));
    return paths;
}

void StorageArchive::updateSnapshotCbtToken(sqlite3_int64 snapshotId, std::uint64_t token) {
    sqlite3_stmt* st = hot_.updateSnapshotCbtToken; resetStmt(st);
    sqBindInt64(db_.raw(),st,1,static_cast<sqlite3_int64>(token));
    sqBindInt64(db_.raw(),st,2,snapshotId);
    stepDoneOrThrow(db_.raw(),st);
    if (sqlite3_changes(db_.raw())!=1)
        throw std::runtime_error("Falha atualizando cbt_token do snapshot.");
}

std::vector<SnapshotRow> StorageArchive::listSnapshots() {
    std::vector<SnapshotRow> out;
    Stmt st(db_.raw(),
        "SELECT s.id,s.created_at,s.source_root,COALESCE(s.label,''),COUNT(f.id) "
        "FROM snapshots s LEFT JOIN files f ON f.snapshot_id=s.id "
        "GROUP BY s.id,s.created_at,s.source_root,s.label ORDER BY s.id ASC;");
    while (st.stepRow()) {
        SnapshotRow row;
        row.id=sqlite3_column_int64(st.get(),0); row.createdAt=colText(st.get(),1);
        row.sourceRoot=colText(st.get(),2);       row.label=colText(st.get(),3);
        row.fileCount=sqlite3_column_int64(st.get(),4);
        out.push_back(std::move(row));
    }
    return out;
}

std::vector<ChangeEntry> StorageArchive::diffSnapshots(sqlite3_int64 olderSnapshotId,
                                                        sqlite3_int64 newerSnapshotId) {
    const auto olderMap = loadSnapshotFileMap(olderSnapshotId);
    const auto newerMap = loadSnapshotFileMap(newerSnapshotId);
    std::set<std::string> allPaths;
    for (const auto& [p,_] : olderMap) allPaths.insert(p);
    for (const auto& [p,_] : newerMap) allPaths.insert(p);

    std::vector<ChangeEntry> out;
    for (const auto& path : allPaths) {
        const auto oi = olderMap.find(path);
        const auto ni = newerMap.find(path);
        if (oi==olderMap.end()) {
            ChangeEntry e{path,"added",0,ni->second.size,0,ni->second.mtime,"",ni->second.fileHash};
            out.push_back(std::move(e)); continue;
        }
        if (ni==newerMap.end()) {
            ChangeEntry e{path,"removed",oi->second.size,0,oi->second.mtime,0,oi->second.fileHash,""};
            out.push_back(std::move(e)); continue;
        }
        if (oi->second.size!=ni->second.size || oi->second.mtime!=ni->second.mtime ||
            oi->second.fileHash!=ni->second.fileHash) {
            ChangeEntry e{path,"modified",
                oi->second.size,ni->second.size,
                oi->second.mtime,ni->second.mtime,
                oi->second.fileHash,ni->second.fileHash};
            out.push_back(std::move(e));
        }
    }
    return out;
}

std::vector<ChangeEntry> StorageArchive::diffLatestVsPrevious() {
    const auto newer = latestSnapshotId();
    const auto older = previousSnapshotId();
    if (!newer||!older) return {};
    return diffSnapshots(*older, *newer);
}

} // namespace keeply

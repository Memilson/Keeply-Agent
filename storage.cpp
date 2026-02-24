#include "keeply.hpp"

#include <algorithm>
#include <cstring>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace keeply {

namespace {

// ============================== HELPERS HOT SQLITE ========================

static void finalizeStmt(sqlite3_stmt*& st) {
    if (st) {
        sqlite3_finalize(st);
        st = nullptr;
    }
}

static void resetStmt(sqlite3_stmt* st) {
    sqlite3_reset(st);
    sqlite3_clear_bindings(st);
}

static void stepDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    const int rc = sqlite3_step(st);
    if (rc != SQLITE_DONE) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static bool stepRowOrDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    const int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) return true;
    if (rc == SQLITE_DONE) return false;
    throw SqliteError(sqlite3_errmsg(db));
}

static void bindIntOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, int v) {
    if (sqlite3_bind_int(st, idx, v) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static void bindInt64OrThrow(sqlite3* db, sqlite3_stmt* st, int idx, sqlite3_int64 v) {
    if (sqlite3_bind_int64(st, idx, v) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static void bindTextOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, const std::string& v) {
    if (sqlite3_bind_text(st, idx, v.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static void bindTextStaticOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, const char* v) {
    if (sqlite3_bind_text(st, idx, v, -1, SQLITE_STATIC) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static void bindBlobOrThrow(sqlite3* db, sqlite3_stmt* st, int idx, const void* data, int len) {
    if (sqlite3_bind_blob(st, idx, data, len, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static void bindNullOrThrow(sqlite3* db, sqlite3_stmt* st, int idx) {
    if (sqlite3_bind_null(st, idx) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
}

static std::string colText(sqlite3_stmt* st, int idx) {
    const unsigned char* p = sqlite3_column_text(st, idx);
    return p ? reinterpret_cast<const char*>(p) : "";
}

} // namespace

SqliteError::SqliteError(const std::string& msg) : std::runtime_error(msg) {}

// ============================== STMT =====================================

Stmt::Stmt(sqlite3* db, const std::string& sql) : db_(db) {
    if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK) {
        throw SqliteError("sqlite prepare: " + std::string(sqlite3_errmsg(db_)) + " | SQL: " + sql);
    }
}

Stmt::~Stmt() {
    if (stmt_) sqlite3_finalize(stmt_);
}

sqlite3_stmt* Stmt::get() { return stmt_; }

void Stmt::bindInt(int idx, int v) {
    if (sqlite3_bind_int(stmt_, idx, v) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db_));
    }
}

void Stmt::bindInt64(int idx, sqlite3_int64 v) {
    if (sqlite3_bind_int64(stmt_, idx, v) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db_));
    }
}

void Stmt::bindText(int idx, const std::string& v) {
    if (sqlite3_bind_text(stmt_, idx, v.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db_));
    }
}

void Stmt::bindBlob(int idx, const void* data, int len) {
    if (sqlite3_bind_blob(stmt_, idx, data, len, SQLITE_TRANSIENT) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db_));
    }
}

void Stmt::bindNull(int idx) {
    if (sqlite3_bind_null(stmt_, idx) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db_));
    }
}

bool Stmt::stepRow() {
    int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) return true;
    if (rc == SQLITE_DONE) return false;
    throw SqliteError(sqlite3_errmsg(db_));
}

void Stmt::stepDone() {
    int rc = sqlite3_step(stmt_);
    if (rc != SQLITE_DONE) {
        throw SqliteError(sqlite3_errmsg(db_));
    }
}

// ============================== DB =======================================

DB::DB(const fs::path& p) {
    if (sqlite3_open(p.string().c_str(), &db_) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        db_ = nullptr;
        throw SqliteError("sqlite open: " + err);
    }

    // PRAGMAs essenciais
    exec("PRAGMA journal_mode=WAL;");
    exec("PRAGMA synchronous=NORMAL;");
    exec("PRAGMA foreign_keys=ON;");
    exec("PRAGMA temp_store=MEMORY;");

    // PRAGMAs de performance (best effort)
    auto tryExec = [this](const char* sql) {
        try { exec(sql); } catch (...) {}
    };
    tryExec("PRAGMA cache_size=-65536;");        // ~64MB cache
    tryExec("PRAGMA mmap_size=268435456;");      // 256MB
    tryExec("PRAGMA busy_timeout=5000;");
    // tryExec("PRAGMA locking_mode=EXCLUSIVE;"); // opcional: batch single-writer

    initSchema();
}

DB::~DB() {
    if (db_) sqlite3_close(db_);
}

sqlite3* DB::raw() { return db_; }

void DB::exec(const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "erro sqlite";
        sqlite3_free(err);
        throw SqliteError(msg + " | SQL: " + sql);
    }
}

sqlite3_int64 DB::lastInsertId() const {
    return sqlite3_last_insert_rowid(db_);
}

int DB::changes() const {
    return sqlite3_changes(db_);
}

void DB::initSchema() {
    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        );
    )SQL");

    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            source_root TEXT NOT NULL,
            label TEXT
        );
    )SQL");

    // file_hash mantido por compatibilidade com snapshots antigos
    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            size INTEGER NOT NULL,
            mtime INTEGER NOT NULL,
            file_hash TEXT,
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,
            UNIQUE(snapshot_id, path)
        );
    )SQL");

    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS chunks (
            hash_sha256 BLOB PRIMARY KEY,
            raw_size INTEGER NOT NULL,
            comp_size INTEGER NOT NULL,
            comp_algo TEXT NOT NULL,
            data BLOB NOT NULL
        );
    )SQL");

    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS file_chunks (
            file_id INTEGER NOT NULL,
            chunk_index INTEGER NOT NULL,
            chunk_hash_sha256 BLOB NOT NULL,
            raw_size INTEGER NOT NULL,
            PRIMARY KEY(file_id, chunk_index),
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
            FOREIGN KEY(chunk_hash_sha256) REFERENCES chunks(hash_sha256)
        );
    )SQL");

    // Remover índices redundantes (inclusive em bases já existentes)
    // UNIQUE(snapshot_id, path) já cria índice implícito que cobre:
    // - WHERE snapshot_id = ?
    // - WHERE snapshot_id = ? AND path = ?
    exec("DROP INDEX IF EXISTS idx_files_snapshot;");
    exec("DROP INDEX IF EXISTS idx_files_snapshot_path;");

    // PRIMARY KEY(file_id, chunk_index) já cria índice implícito
    exec("DROP INDEX IF EXISTS idx_file_chunks_file;");
}

// ============================== STORAGE ==================================

StorageArchive::StorageArchive(const fs::path& path)
    : path_(path), db_(path) {
    prepareHotStatements();
}

StorageArchive::~StorageArchive() {
    finalizeHotStatements();
}

void StorageArchive::prepareHotStatements() {
    auto prepare = [&](sqlite3_stmt*& out, const char* sql) {
        if (sqlite3_prepare_v2(db_.raw(), sql, -1, &out, nullptr) != SQLITE_OK) {
            throw SqliteError(std::string("sqlite prepare(hot): ")
                              + sqlite3_errmsg(db_.raw()) + " | SQL: " + sql);
        }
    };

    try {
        prepare(hot_.insertSnapshot,
            "INSERT INTO snapshots(created_at, source_root, label) VALUES(?,?,?);");

        prepare(hot_.insertFilePlaceholder,
            "INSERT INTO files(snapshot_id, path, size, mtime, file_hash) VALUES(?,?,?,?,NULL);");

        prepare(hot_.updateFileHash,
            "UPDATE files SET file_hash = ? WHERE id = ?;");

        prepare(hot_.deleteFileRecord,
            "DELETE FROM files WHERE id = ?;");

        prepare(hot_.cloneFileInsert,
            "INSERT INTO files(snapshot_id, path, size, mtime, file_hash) VALUES(?,?,?,?,?);");

        prepare(hot_.cloneFileChunks, R"SQL(
            INSERT INTO file_chunks(file_id, chunk_index, chunk_hash_sha256, raw_size)
            SELECT ?, chunk_index, chunk_hash_sha256, raw_size
            FROM file_chunks
            WHERE file_id = ?
            ORDER BY chunk_index;
        )SQL");

        prepare(hot_.insertChunkIfMissing, R"SQL(
            INSERT OR IGNORE INTO chunks(hash_sha256, raw_size, comp_size, comp_algo, data)
            VALUES(?,?,?,?,?);
        )SQL");

        prepare(hot_.addFileChunk,
            "INSERT INTO file_chunks(file_id, chunk_index, chunk_hash_sha256, raw_size) VALUES(?,?,?,?);");
    } catch (...) {
        finalizeHotStatements();
        throw;
    }
}

void StorageArchive::finalizeHotStatements() {
    finalizeStmt(hot_.insertSnapshot);

    finalizeStmt(hot_.insertFilePlaceholder);
    finalizeStmt(hot_.updateFileHash);
    finalizeStmt(hot_.deleteFileRecord);

    finalizeStmt(hot_.cloneFileInsert);
    finalizeStmt(hot_.cloneFileChunks);

    finalizeStmt(hot_.insertChunkIfMissing);
    finalizeStmt(hot_.addFileChunk);
}

void StorageArchive::begin() { db_.exec("BEGIN IMMEDIATE;"); }
void StorageArchive::commit() { db_.exec("COMMIT;"); }
void StorageArchive::rollback() {
    try { db_.exec("ROLLBACK;"); } catch (...) {}
}

sqlite3_int64 StorageArchive::createSnapshot(const std::string& sourceRoot, const std::string& label) {
    sqlite3_stmt* st = hot_.insertSnapshot;
    resetStmt(st);

    bindTextOrThrow(db_.raw(), st, 1, nowIsoLocal());
    bindTextOrThrow(db_.raw(), st, 2, sourceRoot);
    if (label.empty()) bindNullOrThrow(db_.raw(), st, 3);
    else bindTextOrThrow(db_.raw(), st, 3, label);

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

sqlite3_int64 StorageArchive::resolveSnapshotId(const std::string& userInput) {
    std::string s = trim(userInput);

    if (s == "latest" || s.empty()) {
        auto sid = latestSnapshotId();
        if (!sid) throw std::runtime_error("Nao ha snapshots no arquivo.");
        return *sid;
    }

    try {
        return static_cast<sqlite3_int64>(std::stoll(s));
    } catch (...) {
        throw std::runtime_error("Snapshot invalido.");
    }
}

std::map<std::string, FileInfo> StorageArchive::loadSnapshotFileMap(sqlite3_int64 snapshotId) {
    std::map<std::string, FileInfo> out;

    // Mantido com file_hash por compat (diff pode exibir hash legado)
    Stmt st(db_.raw(), "SELECT id, path, size, mtime, COALESCE(file_hash,'') FROM files WHERE snapshot_id = ?;");
    st.bindInt64(1, snapshotId);

    while (st.stepRow()) {
        FileInfo f;
        f.fileId = sqlite3_column_int64(st.get(), 0);
        f.size = sqlite3_column_int64(st.get(), 2);
        f.mtime = sqlite3_column_int64(st.get(), 3);
        f.fileHash = colText(st.get(), 4);

        std::string path = colText(st.get(), 1);
        out[path] = std::move(f);
    }

    return out;
}

std::map<std::string, FileInfo> StorageArchive::loadLatestSnapshotFileMap() {
    auto sid = latestSnapshotId();
    if (!sid) return {};
    return loadSnapshotFileMap(*sid);
}

sqlite3_int64 StorageArchive::insertFilePlaceholder(sqlite3_int64 snapshotId,
                                                    const std::string& relPath,
                                                    sqlite3_int64 size,
                                                    sqlite3_int64 mtime) {
    // file_hash fica NULL (hash de arquivo desativado no scan)
    sqlite3_stmt* st = hot_.insertFilePlaceholder;
    resetStmt(st);

    bindInt64OrThrow(db_.raw(), st, 1, snapshotId);
    bindTextOrThrow(db_.raw(), st, 2, relPath);
    bindInt64OrThrow(db_.raw(), st, 3, size);
    bindInt64OrThrow(db_.raw(), st, 4, mtime);

    stepDoneOrThrow(db_.raw(), st);
    return db_.lastInsertId();
}

void StorageArchive::updateFileHash(sqlite3_int64 fileId, const std::string& fileHash) {
    // Mantido por compatibilidade (snapshots antigos / futuras migrações)
    sqlite3_stmt* st = hot_.updateFileHash;
    resetStmt(st);

    bindTextOrThrow(db_.raw(), st, 1, fileHash);
    bindInt64OrThrow(db_.raw(), st, 2, fileId);

    stepDoneOrThrow(db_.raw(), st);
}

void StorageArchive::deleteFileRecord(sqlite3_int64 fileId) {
    sqlite3_stmt* st = hot_.deleteFileRecord;
    resetStmt(st);

    bindInt64OrThrow(db_.raw(), st, 1, fileId);
    stepDoneOrThrow(db_.raw(), st);
}

sqlite3_int64 StorageArchive::cloneFileFromPrevious(sqlite3_int64 snapshotId,
                                                    const std::string& relPath,
                                                    const FileInfo& prev) {
    sqlite3_stmt* ins = hot_.cloneFileInsert;
    resetStmt(ins);

    bindInt64OrThrow(db_.raw(), ins, 1, snapshotId);
    bindTextOrThrow(db_.raw(), ins, 2, relPath);
    bindInt64OrThrow(db_.raw(), ins, 3, prev.size);
    bindInt64OrThrow(db_.raw(), ins, 4, prev.mtime);
    if (prev.fileHash.empty()) bindNullOrThrow(db_.raw(), ins, 5);
    else bindTextOrThrow(db_.raw(), ins, 5, prev.fileHash);

    stepDoneOrThrow(db_.raw(), ins);

    const sqlite3_int64 newFileId = db_.lastInsertId();

    sqlite3_stmt* clone = hot_.cloneFileChunks;
    resetStmt(clone);

    bindInt64OrThrow(db_.raw(), clone, 1, newFileId);
    bindInt64OrThrow(db_.raw(), clone, 2, prev.fileId);

    stepDoneOrThrow(db_.raw(), clone);

    return newFileId;
}

bool StorageArchive::insertChunkIfMissing(const ChunkHash& sha,
                                          std::size_t rawSize,
                                          const std::vector<unsigned char>& comp,
                                          const std::string& compAlgo) {
    sqlite3_stmt* st = hot_.insertChunkIfMissing;
    resetStmt(st);

    bindBlobOrThrow(db_.raw(), st, 1, sha.data(), static_cast<int>(sha.size()));
    bindInt64OrThrow(db_.raw(), st, 2, static_cast<sqlite3_int64>(rawSize));
    bindInt64OrThrow(db_.raw(), st, 3, static_cast<sqlite3_int64>(comp.size()));
    bindTextOrThrow(db_.raw(), st, 4, compAlgo);
    bindBlobOrThrow(db_.raw(), st, 5, comp.data(), static_cast<int>(comp.size()));

    stepDoneOrThrow(db_.raw(), st);

    // INSERT OR IGNORE: >0 inseriu, 0 já existia
    return sqlite3_changes(db_.raw()) > 0;
}

void StorageArchive::addFileChunk(sqlite3_int64 fileId,
                                  int chunkIdx,
                                  const ChunkHash& chunkSha,
                                  std::size_t rawSize) {
    sqlite3_stmt* st = hot_.addFileChunk;
    resetStmt(st);

    bindInt64OrThrow(db_.raw(), st, 1, fileId);
    bindIntOrThrow(db_.raw(), st, 2, chunkIdx);
    bindBlobOrThrow(db_.raw(), st, 3, chunkSha.data(), static_cast<int>(chunkSha.size()));
    bindInt64OrThrow(db_.raw(), st, 4, static_cast<sqlite3_int64>(rawSize));

    stepDoneOrThrow(db_.raw(), st);
}

// Bulk insert (opcional, mais rápido que addFileChunk por linha)
// Uso ideal: acumular chunks do arquivo em memória e inserir no final do arquivo.
void StorageArchive::addFileChunksBulk(sqlite3_int64 fileId,
                                       const std::vector<PendingFileChunk>& rows) {
    if (rows.empty()) return;

    // 4 binds por linha. 128 => 512 binds (bem abaixo do limite clássico 999)
    constexpr std::size_t kBatchSize = 128;

    for (std::size_t i = 0; i < rows.size(); i += kBatchSize) {
        const std::size_t n = std::min(kBatchSize, rows.size() - i);

        std::ostringstream sql;
        sql << "INSERT INTO file_chunks(file_id, chunk_index, chunk_hash_sha256, raw_size) VALUES ";
        for (std::size_t j = 0; j < n; ++j) {
            if (j) sql << ",";
            sql << "(?,?,?,?)";
        }
        sql << ";";

        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db_.raw(), sql.str().c_str(), -1, &st, nullptr) != SQLITE_OK) {
            throw SqliteError(std::string("sqlite prepare(bulk file_chunks): ") + sqlite3_errmsg(db_.raw()));
        }

        try {
            int bindIdx = 1;
            for (std::size_t j = 0; j < n; ++j) {
                const auto& r = rows[i + j];
                bindInt64OrThrow(db_.raw(), st, bindIdx++, fileId);
                bindIntOrThrow(db_.raw(),  st, bindIdx++, r.chunkIdx);
                bindBlobOrThrow(db_.raw(), st, bindIdx++, r.chunkHash.data(),
                                static_cast<int>(r.chunkHash.size()));
                bindInt64OrThrow(db_.raw(), st, bindIdx++, static_cast<sqlite3_int64>(r.rawSize));
            }

            stepDoneOrThrow(db_.raw(), st);
        } catch (...) {
            sqlite3_finalize(st);
            throw;
        }

        sqlite3_finalize(st);
    }
}

std::vector<SnapshotRow> StorageArchive::listSnapshots() {
    std::vector<SnapshotRow> out;

    Stmt st(db_.raw(), R"SQL(
        SELECT s.id, s.created_at, s.source_root, COALESCE(s.label,''), COUNT(f.id)
        FROM snapshots s
        LEFT JOIN files f ON f.snapshot_id = s.id
        GROUP BY s.id, s.created_at, s.source_root, s.label
        ORDER BY s.id DESC;
    )SQL");

    while (st.stepRow()) {
        SnapshotRow r;
        r.id = sqlite3_column_int64(st.get(), 0);
        r.createdAt = colText(st.get(), 1);
        r.sourceRoot = colText(st.get(), 2);
        r.label = colText(st.get(), 3);
        r.fileCount = sqlite3_column_int64(st.get(), 4);
        out.push_back(std::move(r));
    }

    return out;
}

std::vector<ChangeEntry> StorageArchive::diffSnapshots(sqlite3_int64 olderSnapshotId,
                                                       sqlite3_int64 newerSnapshotId) {
    auto oldMap = loadSnapshotFileMap(olderSnapshotId);
    auto newMap = loadSnapshotFileMap(newerSnapshotId);

    std::set<std::string> allPaths;
    for (const auto& [p, _] : oldMap) allPaths.insert(p);
    for (const auto& [p, _] : newMap) allPaths.insert(p);

    std::vector<ChangeEntry> out;
    out.reserve(allPaths.size());

    for (const auto& path : allPaths) {
        auto itOld = oldMap.find(path);
        auto itNew = newMap.find(path);

        ChangeEntry c;
        c.path = path;

        if (itOld == oldMap.end() && itNew != newMap.end()) {
            c.status = "ADDED";
            c.newSize = itNew->second.size;
            c.newMtime = itNew->second.mtime;
            c.newHash = itNew->second.fileHash; // compat (pode ficar vazio)
        } else if (itOld != oldMap.end() && itNew == newMap.end()) {
            c.status = "REMOVED";
            c.oldSize = itOld->second.size;
            c.oldMtime = itOld->second.mtime;
            c.oldHash = itOld->second.fileHash; // compat (pode ficar vazio)
        } else {
            c.oldSize = itOld->second.size;
            c.oldMtime = itOld->second.mtime;
            c.oldHash = itOld->second.fileHash;

            c.newSize = itNew->second.size;
            c.newMtime = itNew->second.mtime;
            c.newHash = itNew->second.fileHash;

            // Igualdade sem depender de file_hash (hash de arquivo desativado)
            const bool same =
                (c.oldSize == c.newSize) &&
                (c.oldMtime == c.newMtime);

            c.status = same ? "UNCHANGED" : "MODIFIED";
        }

        out.push_back(std::move(c));
    }

    std::sort(out.begin(), out.end(), [](const ChangeEntry& a, const ChangeEntry& b) {
        if (a.status != b.status) return a.status < b.status;
        return a.path < b.path;
    });

    return out;
}

std::vector<ChangeEntry> StorageArchive::diffLatestVsPrevious() {
    auto newer = latestSnapshotId();
    auto older = previousSnapshotId();

    if (!newer) throw std::runtime_error("Nao ha snapshots.");
    if (!older) throw std::runtime_error("Nao ha snapshot anterior para comparar.");

    return diffSnapshots(*older, *newer);
}

// ============================== RESTORE QUERIES ==========================

std::optional<std::pair<sqlite3_int64, sqlite3_int64>>
StorageArchive::findFileBySnapshotAndPath(sqlite3_int64 snapshotId, const std::string& relPath) {
    Stmt st(db_.raw(), "SELECT id, size FROM files WHERE snapshot_id = ? AND path = ?;");
    st.bindInt64(1, snapshotId);
    st.bindText(2, relPath);

    if (!st.stepRow()) return std::nullopt;

    return std::make_pair(sqlite3_column_int64(st.get(), 0),
                          sqlite3_column_int64(st.get(), 1));
}

std::vector<StoredChunkRow> StorageArchive::loadFileChunks(sqlite3_int64 fileId) {
    std::vector<StoredChunkRow> out;

    Stmt st(db_.raw(), R"SQL(
        SELECT fc.chunk_index, fc.raw_size, c.comp_size, c.comp_algo, c.data
        FROM file_chunks fc
        JOIN chunks c ON c.hash_sha256 = fc.chunk_hash_sha256
        WHERE fc.file_id = ?
        ORDER BY fc.chunk_index ASC;
    )SQL");
    st.bindInt64(1, fileId);

    while (st.stepRow()) {
        StoredChunkRow row;
        row.chunkIndex = sqlite3_column_int(st.get(), 0);
        row.rawSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 1));
        row.compSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 2));
        row.algo = colText(st.get(), 3);

        const void* blobPtr = sqlite3_column_blob(st.get(), 4);
        int blobBytes = sqlite3_column_bytes(st.get(), 4);

        if (blobPtr == nullptr && blobBytes > 0) {
            throw std::runtime_error("Blob de chunk invalido.");
        }

        row.blob.resize(static_cast<std::size_t>(blobBytes));
        if (blobBytes > 0) {
            std::memcpy(row.blob.data(), blobPtr, static_cast<std::size_t>(blobBytes));
        }

        out.push_back(std::move(row));
    }

    return out;
}

std::vector<std::string> StorageArchive::listSnapshotPaths(sqlite3_int64 snapshotId) {
    std::vector<std::string> paths;

    Stmt st(db_.raw(), "SELECT path FROM files WHERE snapshot_id = ? ORDER BY path;");
    st.bindInt64(1, snapshotId);

    while (st.stepRow()) {
        paths.emplace_back(colText(st.get(), 0));
    }

    return paths;
}

} // namespace keeply

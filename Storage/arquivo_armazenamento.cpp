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
    std::cerr << "[keeply][storage][warn] " << msg << '\n';
}

static void ensureParentDir(const fs::path& p) {
    std::error_code ec;
    fs::create_directories(p.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando diretorio: " + ec.message());
}

static std::string blobToHex(const Blob& b) {
    return keeply::hexEncode(b.data(), b.size());
}

static std::string safeFileComponent(std::string value) {
    for (char& c : value) {
        const bool ok =
            (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.';
        if (!ok) c = '_';
    }
    while (!value.empty() && value.back() == '.') value.pop_back();
    return value.empty() ? std::string("bundle") : value;
}

static void stepDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    if (sqlite3_step(st) != SQLITE_DONE) throw SqliteError(sqlite3_errmsg(db));
}
static bool stepRowOrDoneOrThrow(sqlite3* db, sqlite3_stmt* st) {
    int rc = sqlite3_step(st);
    if (rc == SQLITE_ROW) return true;
    if (rc == SQLITE_DONE) return false;
    throw SqliteError(sqlite3_errmsg(db));
}
static std::string colText(sqlite3_stmt* st, int idx) {
    const unsigned char* p = sqlite3_column_text(st, idx);
    return p ? reinterpret_cast<const char*>(p) : "";
}
static Blob colBlob(sqlite3_stmt* st, int idx) {
    const void* p = sqlite3_column_blob(st, idx);
    const int n = sqlite3_column_bytes(st, idx);
    if (!p || n <= 0) return {};
    const auto* b = static_cast<const unsigned char*>(p);
    return Blob(b, b + n);
}



using SqlTxn = keeply::SharedSqlTransaction;

static bool tableHasColumn(sqlite3* db, const char* table, const char* colName) {
    std::string sql = "PRAGMA table_info(" + std::string(table) + ");";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
    bool found = false;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char* name = sqlite3_column_text(st, 1);
        if (name && std::string(reinterpret_cast<const char*>(name)) == colName) {
            found = true;
            break;
        }
    }
    sqlite3_finalize(st);
    return found;
}

static bool fileChunksReferencesTable(sqlite3* db, const char* tableName) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, "PRAGMA foreign_key_list(file_chunks);", -1, &st, nullptr) != SQLITE_OK) {
        throw SqliteError(sqlite3_errmsg(db));
    }
    bool found = false;
    while (sqlite3_step(st) == SQLITE_ROW) {
        const unsigned char* target = sqlite3_column_text(st, 2);
        if (target && std::string(reinterpret_cast<const char*>(target)) == tableName) {
            found = true;
            break;
        }
    }
    sqlite3_finalize(st);
    return found;
}

static int readSchemaVersion(sqlite3* db) {
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, "SELECT version FROM schema_version LIMIT 1;", -1, &st, nullptr) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db));
    int ver = 0;
    if (sqlite3_step(st) == SQLITE_ROW) ver = sqlite3_column_int(st, 0);
    else {
        sqlite3_finalize(st);
        throw SqliteError("schema_version vazio ou invalido.");
    }
    sqlite3_finalize(st);
    return ver;
}

static void writeSchemaVersion(sqlite3* db, int ver) {
    char* err = nullptr;
    const std::string sql = "INSERT OR REPLACE INTO schema_version(rowid,version) VALUES(1," + std::to_string(ver) + ");";
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string m = err ? err : "erro sqlite";
        if (err) sqlite3_free(err);
        throw SqliteError(m);
    }
}

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
            if (!create) throw std::runtime_error("Falha criando pack.");
        }
        if (!fs::exists(paths_.indexPath)) {
            std::ofstream create(paths_.indexPath, std::ios::binary);
            if (!create) throw std::runtime_error("Falha criando index do pack.");
        }
        packTxnStart_ = fileSizeNoThrow(paths_.packPath);
        indexTxnStart_ = fileSizeNoThrow(paths_.indexPath);
        pack_.open(paths_.packPath, std::ios::binary | std::ios::in | std::ios::out);
        if (!pack_) throw std::runtime_error("Falha abrindo pack.");
        idx_.open(paths_.indexPath, std::ios::binary | std::ios::in | std::ios::out | std::ios::app);
        if (!idx_) throw std::runtime_error("Falha abrindo index do pack.");
        inTxn_ = true;
    }

    void commitSession() override {
        if (!inTxn_) return;
        pack_.flush();
        idx_.flush();
        pack_.close();
        idx_.close();
        inTxn_ = false;
    }

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
        if (ec2) throw std::runtime_error("Falha no rollback do index do pack: " + ec2.message());
    }

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
        return off;
    }

private:
    static std::uint64_t fileSizeNoThrow(const fs::path& p) {
        std::error_code ec;
        const auto sz = fs::file_size(p, ec);
        return ec ? 0ull : static_cast<std::uint64_t>(sz);
    }

    ArchiveStoragePaths paths_;
    std::fstream pack_;
    std::fstream idx_;
    std::uint64_t packTxnStart_ = 0;
    std::uint64_t indexTxnStart_ = 0;
    bool inTxn_ = false;
};

class LocalCloudExporter final : public StorageCloudExporter {
public:
    explicit LocalCloudExporter(const fs::path& archivePath)
        : paths_(describeArchiveStorage(archivePath)) {}

    StorageArchive::CloudBundleExport exportBundle(const fs::path& tempRoot,
                                                   std::uint64_t blobMaxBytes) const override;
    StorageArchive::CloudBundleFile materializeBlob(const StorageArchive::CloudBundleExport& bundle,
                                                    std::size_t partIndex) const override;

private:
    ArchiveStoragePaths paths_;
};

StorageArchive::CloudBundleExport LocalCloudExporter::exportBundle(const fs::path& tempRoot,
                                                                  std::uint64_t blobMaxBytes) const {
    const std::string bundleId = "bundle-" + safeFileComponent(nowIsoLocal());
    const fs::path rootDir = tempRoot / bundleId;
    std::error_code ec;
    fs::create_directories(rootDir, ec);
    if (ec) throw std::runtime_error("Falha criando root do bundle: " + ec.message());

    const fs::path dbCopy = rootDir / paths_.archivePath.filename();
    const fs::path idxCopy = rootDir / paths_.indexPath.filename();

    fs::copy_file(paths_.archivePath, dbCopy, fs::copy_options::overwrite_existing, ec);
    if (ec) throw std::runtime_error("Falha copiando DB do bundle: " + ec.message());
    if (fs::exists(paths_.indexPath)) {
        fs::copy_file(paths_.indexPath, idxCopy, fs::copy_options::overwrite_existing, ec);
        if (ec) throw std::runtime_error("Falha copiando index do bundle: " + ec.message());
    }

    StorageArchive::CloudBundleExport out;
    out.bundleId = bundleId;
    out.rootDir = rootDir;
    out.packPath = paths_.packPath;
    out.blobMaxBytes = blobMaxBytes;

    StorageArchive::CloudBundleFile dbFile;
    dbFile.path = dbCopy;
    dbFile.uploadName = dbCopy.filename().string();
    dbFile.objectKey = bundleId + "/" + dbFile.uploadName;
    dbFile.size = fs::file_size(dbCopy, ec);
    out.files.push_back(dbFile);

    if (fs::exists(idxCopy)) {
        StorageArchive::CloudBundleFile idxFile;
        idxFile.path = idxCopy;
        idxFile.uploadName = idxCopy.filename().string();
        idxFile.objectKey = bundleId + "/" + idxFile.uploadName;
        idxFile.size = fs::file_size(idxCopy, ec);
        out.files.push_back(idxFile);
    }

    const std::uint64_t packSize =
        fs::exists(paths_.packPath) ? static_cast<std::uint64_t>(fs::file_size(paths_.packPath, ec)) : 0ull;
    if (blobMaxBytes == 0 || packSize <= blobMaxBytes) {
        const fs::path packCopy = rootDir / paths_.packPath.filename();
        fs::copy_file(paths_.packPath, packCopy, fs::copy_options::overwrite_existing, ec);
        if (ec) throw std::runtime_error("Falha copiando pack do bundle: " + ec.message());
        StorageArchive::CloudBundleFile packFile;
        packFile.path = packCopy;
        packFile.uploadName = packCopy.filename().string();
        packFile.objectKey = bundleId + "/" + packFile.uploadName;
        packFile.size = fs::file_size(packCopy, ec);
        packFile.blobPart = true;
        out.files.push_back(packFile);
        out.blobPartCount = 1;
    } else {
        out.blobPartCount = static_cast<std::size_t>((packSize + blobMaxBytes - 1) / blobMaxBytes);
    }

    const fs::path manifest = rootDir / "bundle.manifest";
    {
        std::ofstream mf(manifest, std::ios::binary | std::ios::trunc);
        if (!mf) throw std::runtime_error("Falha criando manifest do bundle.");
        mf << "bundle_id=" << bundleId << "\n";
        mf << "archive_file=" << paths_.archivePath.filename().string() << "\n";
        mf << "pack_file=" << paths_.packPath.filename().string() << "\n";
        mf << "index_file=" << paths_.indexPath.filename().string() << "\n";
        mf << "blob_max_bytes=" << blobMaxBytes << "\n";
        mf << "blob_part_count=" << out.blobPartCount << "\n";
    }
    StorageArchive::CloudBundleFile mfFile;
    mfFile.path = manifest;
    mfFile.uploadName = manifest.filename().string();
    mfFile.objectKey = bundleId + "/" + mfFile.uploadName;
    mfFile.size = fs::file_size(manifest, ec);
    mfFile.manifest = true;
    out.files.push_back(mfFile);

    return out;
}

StorageArchive::CloudBundleFile LocalCloudExporter::materializeBlob(const StorageArchive::CloudBundleExport& bundle,
                                                                    std::size_t partIndex) const {
    if (bundle.blobPartCount == 0) throw std::runtime_error("Bundle nao possui blobs.");
    if (partIndex >= bundle.blobPartCount) throw std::runtime_error("blob partIndex invalido.");

    std::ifstream in(bundle.packPath, std::ios::binary);
    if (!in) throw std::runtime_error("Falha abrindo pack para materializar blob.");

    const std::uint64_t offset = bundle.blobMaxBytes * static_cast<std::uint64_t>(partIndex);
    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!in) throw std::runtime_error("Falha posicionando no pack para blob.");

    const std::string name =
        bundle.packPath.stem().string() + ".part" + std::to_string(partIndex) + bundle.packPath.extension().string();
    const fs::path outPath = bundle.rootDir / name;
    std::ofstream out(outPath, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("Falha criando arquivo blob do bundle.");

    Blob buf(1024 * 1024);
    std::uint64_t remaining = bundle.blobMaxBytes;
    while (remaining > 0 && in) {
        const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, buf.size()));
        in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(n));
        const std::streamsize got = in.gcount();
        if (got <= 0) break;
        out.write(reinterpret_cast<const char*>(buf.data()), got);
        if (!out) throw std::runtime_error("Falha escrevendo blob do bundle.");
        remaining -= static_cast<std::uint64_t>(got);
    }

    StorageArchive::CloudBundleFile f;
    f.path = outPath;
    f.uploadName = outPath.filename().string();
    f.objectKey = bundle.bundleId + "/" + f.uploadName;
    std::error_code ec;
    f.size = fs::file_size(outPath, ec);
    f.blobPart = true;
    return f;
}

} 

ArchiveStoragePaths describeArchiveStorage(const fs::path& archivePath) {
    ArchiveStoragePaths paths;
    paths.archivePath = archivePath;
    paths.packPath = archivePath.parent_path() / (archivePath.stem().string() + ".klyp.pack");
    paths.indexPath = archivePath.parent_path() / (archivePath.stem().string() + ".klyp.idx");
    return paths;
}

void ensureArchiveStorageParent(const fs::path& archivePath) {
    if (archivePath.parent_path().empty()) return;
    std::error_code ec;
    fs::create_directories(archivePath.parent_path(), ec);
    if (ec) throw std::runtime_error("Falha criando pasta do arquivo: " + ec.message());
}

std::shared_ptr<StorageBackend> makeLocalStorageBackend(const fs::path& archivePath) {
    return std::make_shared<LocalPackBackend>(archivePath);
}

std::unique_ptr<StorageCloudExporter> makeLocalCloudExporter(const fs::path& archivePath) {
    return std::make_unique<LocalCloudExporter>(archivePath);
}

SqliteError::SqliteError(const std::string& msg) : std::runtime_error(msg) {}

Stmt::Stmt(sqlite3* db, const std::string& sql) : db_(db) {
    if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK)
        throw SqliteError("sqlite prepare falhou: " + sql + " — " + sqlite3_errmsg(db_));
}
Stmt::~Stmt() {
    if (stmt_) sqlite3_finalize(stmt_);
}
sqlite3_stmt* Stmt::get() { return stmt_; }
void Stmt::bindInt(int idx, int v) {
    if (sqlite3_bind_int(stmt_, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));
}
void Stmt::bindInt64(int idx, sqlite3_int64 v) {
    if (sqlite3_bind_int64(stmt_, idx, v) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));
}
void Stmt::bindText(int idx, const std::string& v) {
    if (sqlite3_bind_text(stmt_, idx, v.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));
}
void Stmt::bindBlob(int idx, const void* d, int len) {
    if (sqlite3_bind_blob(stmt_, idx, d, len, SQLITE_TRANSIENT) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));
}
void Stmt::bindNull(int idx) {
    if (sqlite3_bind_null(stmt_, idx) != SQLITE_OK) throw SqliteError(sqlite3_errmsg(db_));
}
void Stmt::reset() {
    sqlite3_reset(stmt_);
    sqlite3_clear_bindings(stmt_);
}
bool Stmt::stepRow() { return stepRowOrDoneOrThrow(db_, stmt_); }
void Stmt::stepDone() { stepDoneOrThrow(db_, stmt_); }

DB::DB(const fs::path& p) {
    ensureArchiveStorageParent(p);
    if (sqlite3_open_path(p, &db_) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw SqliteError("sqlite open: " + err);
    }
    exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA foreign_keys=ON; PRAGMA temp_store=MEMORY;");
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
        if (err) sqlite3_free(err);
        throw SqliteError(msg + " | SQL: " + sql);
    }
}
sqlite3_int64 DB::lastInsertId() const { return sqlite3_last_insert_rowid(db_); }
int DB::changes() const { return sqlite3_changes(db_); }

void DB::initSchema() {
    exec("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);");
    exec("INSERT OR IGNORE INTO schema_version(rowid,version) SELECT 1,0 WHERE NOT EXISTS (SELECT 1 FROM schema_version);");
    const int ver = readSchemaVersion(db_);

    if (ver < 1) {
        exec("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);");
        exec("CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT,created_at TEXT NOT NULL,source_root TEXT NOT NULL,label TEXT);");
        exec("CREATE TABLE IF NOT EXISTS chunks(chunk_hash BLOB PRIMARY KEY,raw_size INTEGER NOT NULL,comp_size INTEGER NOT NULL,comp_algo TEXT NOT NULL,pack_id TEXT NOT NULL DEFAULT 'main',pack_offset INTEGER NOT NULL,storage_state TEXT NOT NULL DEFAULT 'ready');");
        exec("CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY AUTOINCREMENT,snapshot_id INTEGER NOT NULL,path TEXT NOT NULL,size INTEGER NOT NULL,mtime INTEGER NOT NULL,file_hash BLOB,FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,UNIQUE(snapshot_id, path));");
        exec("CREATE TABLE IF NOT EXISTS file_chunks(file_id INTEGER NOT NULL,chunk_index INTEGER NOT NULL,chunk_hash BLOB NOT NULL,raw_size INTEGER NOT NULL,PRIMARY KEY(file_id,chunk_index),FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,FOREIGN KEY(chunk_hash) REFERENCES chunks(chunk_hash));");
        writeSchemaVersion(db_, 1);
    }

    if (ver < 2) {
        if (!tableHasColumn(db_, "snapshots", "cbt_token")) exec("ALTER TABLE snapshots ADD COLUMN cbt_token INTEGER NOT NULL DEFAULT 0;");
        writeSchemaVersion(db_, 2);
    }

    if (ver < 3) {
        if (tableHasColumn(db_, "chunks", "hash_sha256")) {
            SqlTxn tx(db_);
            exec("ALTER TABLE file_chunks RENAME TO _fc_old;");
            exec("ALTER TABLE chunks RENAME TO _ch_old;");
            exec("CREATE TABLE chunks(chunk_hash BLOB PRIMARY KEY,raw_size INTEGER NOT NULL,comp_size INTEGER NOT NULL,comp_algo TEXT NOT NULL,pack_offset INTEGER NOT NULL);");
            exec("INSERT INTO chunks SELECT hash_sha256,raw_size,comp_size,comp_algo,pack_offset FROM _ch_old;");
            exec("CREATE TABLE file_chunks(file_id INTEGER NOT NULL,chunk_index INTEGER NOT NULL,chunk_hash BLOB NOT NULL,raw_size INTEGER NOT NULL,PRIMARY KEY(file_id,chunk_index),FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,FOREIGN KEY(chunk_hash) REFERENCES chunks(chunk_hash));");
            exec("INSERT INTO file_chunks SELECT file_id,chunk_index,chunk_hash_sha256,raw_size FROM _fc_old;");
            exec("DROP TABLE _fc_old;");
            exec("DROP TABLE _ch_old;");
            tx.commit();
        }
        writeSchemaVersion(db_, 3);
    }

    if (ver < 4) {
        if (!tableHasColumn(db_, "chunks", "pack_id")) exec("ALTER TABLE chunks ADD COLUMN pack_id TEXT NOT NULL DEFAULT 'main';");
        writeSchemaVersion(db_, 4);
    }

    if (ver < 5) {
        if (!tableHasColumn(db_, "chunks", "storage_state")) exec("ALTER TABLE chunks ADD COLUMN storage_state TEXT NOT NULL DEFAULT 'ready';");
        writeSchemaVersion(db_, 5);
    }

    if (ver < 6) {
        SqlTxn tx(db_);
        exec("ALTER TABLE files RENAME TO _files_old;");
        exec("CREATE TABLE files(id INTEGER PRIMARY KEY AUTOINCREMENT,snapshot_id INTEGER NOT NULL,path TEXT NOT NULL,size INTEGER NOT NULL,mtime INTEGER NOT NULL,file_hash BLOB,FOREIGN KEY(snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,UNIQUE(snapshot_id, path));");
        exec("INSERT INTO files(id,snapshot_id,path,size,mtime,file_hash) SELECT id,snapshot_id,path,size,mtime,file_hash FROM _files_old;");
        exec("DROP TABLE _files_old;");
        tx.commit();
        writeSchemaVersion(db_, 6);
    }

    if (fileChunksReferencesTable(db_, "_files_old")) {
        SqlTxn tx(db_);
        exec("ALTER TABLE file_chunks RENAME TO _fc_fix_old;");
        exec("CREATE TABLE file_chunks(file_id INTEGER NOT NULL,chunk_index INTEGER NOT NULL,chunk_hash BLOB NOT NULL,raw_size INTEGER NOT NULL,PRIMARY KEY(file_id,chunk_index),FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,FOREIGN KEY(chunk_hash) REFERENCES chunks(chunk_hash));");
        exec("INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) SELECT file_id,chunk_index,chunk_hash,raw_size FROM _fc_fix_old;");
        exec("DROP TABLE _fc_fix_old;");
        tx.commit();
    }

    exec("CREATE INDEX IF NOT EXISTS idx_files_snapshot ON files(snapshot_id);");
    exec("CREATE INDEX IF NOT EXISTS idx_fc_file ON file_chunks(file_id);");
    exec("CREATE INDEX IF NOT EXISTS idx_chunks_state_hash ON chunks(storage_state, chunk_hash);");
}

StorageArchive::StorageArchive(const fs::path& path) : path_(path), db_(path) {
    prepareHotStatements();
}
StorageArchive::~StorageArchive() {
    try {
        if (writeTxnActive_) rollback();
        else if (readTxnActive_) endRead();
    } catch (const std::exception& ex) {
        logStorageWarning(std::string("rollback falhou no destrutor: ") + ex.what());
    } catch (...) {
        logStorageWarning("rollback falhou no destrutor com excecao desconhecida.");
    }
    finalizeHotStatements();
}

void StorageArchive::begin() {
    if (writeTxnActive_ || readTxnActive_) throw std::runtime_error("StorageArchive ja possui transacao ativa.");
    db_.exec("BEGIN IMMEDIATE;");
    if (!backendOpaque_) backendOpaque_ = makeLocalStorageBackend(path_);
    std::static_pointer_cast<StorageBackend>(backendOpaque_)->beginSession();
    writeTxnActive_ = true;
}

void StorageArchive::beginRead() {
    if (writeTxnActive_ || readTxnActive_) throw std::runtime_error("StorageArchive ja possui transacao ativa.");
    db_.exec("BEGIN DEFERRED;");
    readTxnActive_ = true;
}

void StorageArchive::commit() {
    if (!writeTxnActive_) throw std::runtime_error("Nenhuma transacao de escrita ativa.");
    if (backendOpaque_) std::static_pointer_cast<StorageBackend>(backendOpaque_)->commitSession();
    db_.exec("COMMIT;");
    packIn_.reset();
    writeTxnActive_ = false;
}

void StorageArchive::endRead() {
    if (!readTxnActive_) return;
    db_.exec("ROLLBACK;");
    packIn_.reset();
    readTxnActive_ = false;
}

void StorageArchive::rollback() {
    if (!writeTxnActive_) return;
    try {
        if (backendOpaque_) std::static_pointer_cast<StorageBackend>(backendOpaque_)->rollbackSession();
    } catch (...) {
        try { db_.exec("ROLLBACK;"); } catch (...) {}
        packIn_.reset();
        writeTxnActive_ = false;
        throw;
    }
    db_.exec("ROLLBACK;");
    packIn_.reset();
    writeTxnActive_ = false;
}

void StorageArchive::prepareHotStatements() {
    auto prepare = [&](std::unique_ptr<Stmt>& out, const char* sql) {
        out = std::make_unique<Stmt>(db_.raw(), sql);
    };
    prepare(hot_.insertSnapshot, "INSERT INTO snapshots(created_at,source_root,label) VALUES(?,?,?);");
    prepare(hot_.updateSnapshotCbtToken, "UPDATE snapshots SET cbt_token=? WHERE id=?;");
    prepare(hot_.insertFilePlaceholder, "INSERT INTO files(snapshot_id,path,size,mtime,file_hash) VALUES(?,?,?,?,NULL);");
    prepare(hot_.cloneFileInsert, "INSERT INTO files(snapshot_id,path,size,mtime,file_hash) VALUES(?,?,?,?,?);");
    prepare(hot_.cloneFileChunks, "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) SELECT ?,chunk_index,chunk_hash,raw_size FROM file_chunks WHERE file_id=? ORDER BY chunk_index;");
    prepare(hot_.insertChunkIfMissing, "INSERT OR IGNORE INTO chunks(chunk_hash,raw_size,comp_size,comp_algo,pack_id,pack_offset,storage_state) VALUES(?,?,?,?,?,-1,'pending');");
    prepare(hot_.updateChunkOffset, "UPDATE chunks SET pack_id=?, pack_offset=?, storage_state='ready' WHERE chunk_hash=? AND storage_state='pending';");
    prepare(hot_.addFileChunk, "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) VALUES(?,?,?,?);");
}

void StorageArchive::finalizeHotStatements() {
    hot_.insertSnapshot.reset();
    hot_.updateSnapshotCbtToken.reset();
    hot_.insertFilePlaceholder.reset();
    hot_.cloneFileInsert.reset();
    hot_.cloneFileChunks.reset();
    hot_.insertChunkIfMissing.reset();
    hot_.updateChunkOffset.reset();
    hot_.addFileChunk.reset();
}

sqlite3_int64 StorageArchive::createSnapshot(const std::string& sourceRoot, const std::string& label) {
    Stmt& st = *hot_.insertSnapshot;
    st.reset();
    st.bindText(1, nowIsoLocal());
    st.bindText(2, sourceRoot);
    if (label.empty()) st.bindNull(3);
    else st.bindText(3, label);
    st.stepDone();
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
    if (input.empty() || input == "latest" || input == "last") {
        const auto l = latestSnapshotId();
        if (!l) throw std::runtime_error("Nenhum snapshot encontrado.");
        return *l;
    }
    if (input == "previous" || input == "prev") {
        const auto p = previousSnapshotId();
        if (!p) throw std::runtime_error("Nao existe snapshot anterior.");
        return *p;
    }
    try {
        const sqlite3_int64 id = static_cast<sqlite3_int64>(std::stoll(input));
        Stmt st(db_.raw(), "SELECT 1 FROM snapshots WHERE id=? LIMIT 1;");
        st.bindInt64(1, id);
        if (!st.stepRow()) throw std::runtime_error("Snapshot nao encontrado: " + userInput);
        return id;
    } catch (const std::invalid_argument&) {
        throw std::runtime_error("Identificador de snapshot invalido: " + userInput);
    } catch (const std::out_of_range&) {
        throw std::runtime_error("Identificador de snapshot fora do intervalo: " + userInput);
    }
}

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
        out[colText(st.get(), 1)] = std::move(f);
    }
    return out;
}

std::map<std::string, FileInfo> StorageArchive::loadLatestSnapshotFileMap() {
    const auto l = latestSnapshotId();
    if (!l) return {};
    return loadSnapshotFileMap(*l);
}

sqlite3_int64 StorageArchive::insertFilePlaceholder(sqlite3_int64 snapshotId, const std::string& relPath, sqlite3_int64 size, sqlite3_int64 mtime) {
    Stmt& st = *hot_.insertFilePlaceholder;
    st.reset();
    st.bindInt64(1, snapshotId);
    st.bindText(2, relPath);
    st.bindInt64(3, size);
    st.bindInt64(4, mtime);
    st.stepDone();
    return db_.lastInsertId();
}

void StorageArchive::updateFileHash(sqlite3_int64 fileId, const Blob& fileHash) {
    Stmt st(db_.raw(), "UPDATE files SET file_hash=? WHERE id=?;");
    if (fileHash.empty()) st.bindNull(1);
    else st.bindBlob(1, fileHash.data(), static_cast<int>(fileHash.size()));
    st.bindInt64(2, fileId);
    st.stepDone();
    if (db_.changes() != 1) throw std::runtime_error("Falha atualizando hash do arquivo.");
}

void StorageArchive::deleteFileRecord(sqlite3_int64 fileId) {
    Stmt st(db_.raw(), "DELETE FROM files WHERE id=?;");
    st.bindInt64(1, fileId);
    st.stepDone();
    if (db_.changes() != 1) throw std::runtime_error("Falha removendo registro do arquivo.");
}

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
    return newId;
}

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
        if (db_.changes() != 1) throw std::runtime_error("Falha atualizando metadata do chunk.");
    }
    return true;
}

bool StorageArchive::hasChunk(const ChunkHash& hash) {
    Stmt chk(db_.raw(), "SELECT 1 FROM chunks WHERE chunk_hash=? AND storage_state='ready' AND pack_offset>=0 LIMIT 1;");
    chk.bindBlob(1, hash.data(), static_cast<int>(hash.size()));
    return chk.stepRow();
}

void StorageArchive::addFileChunk(sqlite3_int64 fileId, int chunkIdx, const ChunkHash& chunkHash, std::size_t rawSize) {
    Stmt& st = *hot_.addFileChunk;
    st.reset();
    st.bindInt64(1, fileId);
    st.bindInt(2, chunkIdx);
    st.bindBlob(3, chunkHash.data(), static_cast<int>(chunkHash.size()));
    st.bindInt64(4, static_cast<sqlite3_int64>(rawSize));
    st.stepDone();
}

void StorageArchive::addFileChunksBulk(sqlite3_int64 fileId, const std::vector<PendingFileChunk>& rows) {
    if (rows.empty()) return;
    Stmt st(db_.raw(), "INSERT INTO file_chunks(file_id,chunk_index,chunk_hash,raw_size) VALUES(?,?,?,?);");
    for (const auto& r : rows) {
        st.reset();
        st.bindInt64(1, fileId);
        st.bindInt(2, r.chunkIdx);
        st.bindBlob(3, r.chunkHash.data(), static_cast<int>(r.chunkHash.size()));
        st.bindInt64(4, static_cast<sqlite3_int64>(r.rawSize));
        st.stepDone();
    }
}

std::optional<RestorableFileRef> StorageArchive::findFileBySnapshotAndPath(sqlite3_int64 snapshotId, const std::string& relPath) {
    Stmt st(db_.raw(), "SELECT id,size,mtime FROM files WHERE snapshot_id=? AND path=?;");
    st.bindInt64(1, snapshotId);
    st.bindText(2, relPath);
    if (!st.stepRow()) return std::nullopt;
    RestorableFileRef out;
    out.fileId = sqlite3_column_int64(st.get(), 0);
    out.size = sqlite3_column_int64(st.get(), 1);
    out.mtime = sqlite3_column_int64(st.get(), 2);
    return out;
}

std::vector<unsigned char> StorageArchive::readPackAt(sqlite3_int64 recordOffset, const ChunkHash& expectedSha, std::size_t expectedRawSize, std::size_t expectedCompSize, const std::string& expectedAlgo) const {
    const fs::path p = describeArchiveStorage(path_).packPath;
    if (!packIn_ || !packIn_->is_open()) {
        packIn_ = std::make_unique<std::ifstream>(p, std::ios::binary);
        if (!packIn_ || !*packIn_) throw std::runtime_error("Falha abrindo pack para leitura.");
    }
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

    Blob raw;
    if (algo == "zstd") Compactador::zstdDecompress(comp.data(), comp.size(), expectedRawSize, raw);
    else if (algo == "zlib") Compactador::zlibDecompress(comp.data(), comp.size(), expectedRawSize, raw);
    else throw std::runtime_error("Algoritmo de compressao nao suportado: " + algo);
    if (raw.size() != expectedRawSize) throw std::runtime_error("Tamanho bruto divergente apos descompressao.");
    return raw;
}

std::vector<StoredChunkRow> StorageArchive::loadFileChunks(sqlite3_int64 fileId) {
    std::vector<StoredChunkRow> out;
    Stmt st(db_.raw(), "SELECT fc.chunk_index, fc.raw_size, c.comp_size, c.comp_algo, c.pack_id, c.pack_offset, c.storage_state, fc.chunk_hash FROM file_chunks fc JOIN chunks c ON c.chunk_hash = fc.chunk_hash WHERE fc.file_id=? ORDER BY fc.chunk_index ASC;");
    st.bindInt64(1, fileId);
    while (st.stepRow()) {
        StoredChunkRow row;
        row.chunkIndex = sqlite3_column_int(st.get(), 0);
        row.rawSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 1));
        row.compSize = static_cast<std::size_t>(sqlite3_column_int64(st.get(), 2));
        row.algo = colText(st.get(), 3);
        const std::string packId = colText(st.get(), 4);
        const sqlite3_int64 packOffset = sqlite3_column_int64(st.get(), 5);
        const std::string storageState = colText(st.get(), 6);
        const void* hp = sqlite3_column_blob(st.get(), 7);
        const int hb = sqlite3_column_bytes(st.get(), 7);

        if (storageState != "ready") throw std::runtime_error("Chunk ainda nao esta pronto para leitura.");
        if (packOffset < 0) throw std::runtime_error("Chunk com pack_offset invalido.");
        if (packId.empty()) throw std::runtime_error("Chunk sem pack_id.");
        if (!hp || hb != static_cast<int>(ChunkHash{}.size())) throw std::runtime_error("chunk_hash invalido vindo do SQLite.");

        ChunkHash ch{};
        std::memcpy(ch.data(), hp, ch.size());
        row.blob = readPackAt(packOffset, ch, row.rawSize, row.compSize, row.algo);
        out.push_back(std::move(row));
    }
    return out;
}

std::vector<std::string> StorageArchive::listSnapshotPaths(sqlite3_int64 snapshotId) {
    std::vector<std::string> paths;
    Stmt st(db_.raw(), "SELECT path FROM files WHERE snapshot_id=? ORDER BY path;");
    st.bindInt64(1, snapshotId);
    while (st.stepRow()) paths.emplace_back(colText(st.get(), 0));
    return paths;
}

void StorageArchive::updateSnapshotCbtToken(sqlite3_int64 snapshotId, std::uint64_t token) {
    Stmt& st = *hot_.updateSnapshotCbtToken;
    st.reset();
    st.bindInt64(1, static_cast<sqlite3_int64>(token));
    st.bindInt64(2, snapshotId);
    st.stepDone();
    if (sqlite3_changes(db_.raw()) != 1) throw std::runtime_error("Falha atualizando cbt_token do snapshot.");
}

std::vector<SnapshotRow> StorageArchive::listSnapshots() {
    std::vector<SnapshotRow> out;
    Stmt st(db_.raw(), "SELECT s.id,s.created_at,s.source_root,COALESCE(s.label,''),COUNT(f.id) FROM snapshots s LEFT JOIN files f ON f.snapshot_id=s.id GROUP BY s.id,s.created_at,s.source_root,s.label ORDER BY s.id ASC;");
    while (st.stepRow()) {
        SnapshotRow row;
        row.id = sqlite3_column_int64(st.get(), 0);
        row.createdAt = colText(st.get(), 1);
        row.sourceRoot = colText(st.get(), 2);
        row.label = colText(st.get(), 3);
        row.fileCount = sqlite3_column_int64(st.get(), 4);
        out.push_back(std::move(row));
    }
    return out;
}

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
            continue;
        }
        if (ni == newerMap.end()) {
            ChangeEntry e{path, "removed", oi->second.size, 0, oi->second.mtime, 0, oi->second.fileHash, {}};
            out.push_back(std::move(e));
            continue;
        }
        if (oi->second.size != ni->second.size || oi->second.mtime != ni->second.mtime || oi->second.fileHash != ni->second.fileHash) {
            ChangeEntry e{path, "modified", oi->second.size, ni->second.size, oi->second.mtime, ni->second.mtime, oi->second.fileHash, ni->second.fileHash};
            out.push_back(std::move(e));
        }
    }
    return out;
}

std::vector<ChangeEntry> StorageArchive::diffLatestVsPrevious() {
    const auto newer = latestSnapshotId();
    const auto older = previousSnapshotId();
    if (!newer || !older) return {};
    return diffSnapshots(*older, *newer);
}

StorageArchive::CloudBundleExport StorageArchive::exportCloudBundle(const fs::path& tempRoot, std::uint64_t blobMaxBytes) const {
    return makeLocalCloudExporter(path_)->exportBundle(tempRoot, blobMaxBytes);
}

StorageArchive::CloudBundleFile StorageArchive::materializeCloudBundleBlob(const CloudBundleExport& bundle, std::size_t partIndex) const {
    return makeLocalCloudExporter(path_)->materializeBlob(bundle, partIndex);
}

} 

    #include "keeply.hpp"

    #include <algorithm>
    #include <cstring>
    #include <fstream>
    #include <memory>
    #include <mutex>
    #include <set>
    #include <sstream>
    #include <stdexcept>
    #include <unordered_map>
    #include <utility>
    #include <cstdint>

    namespace keeply {

    namespace {
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

    // ============================== EXTERNAL CHUNK PACK ======================
    // Pack append-only para armazenar blobs comprimidos fora do SQLite.
    // SQLite guarda apenas offset + metadados.

    static constexpr char kChunkPackFileMagic[8] = {'K','P','C','H','U','N','K','1'};
    static constexpr char kChunkPackRecMagic[8]  = {'K','P','R','E','C','0','0','1'};

    // Limites de sanidade (anti-corruption / anti-OOM)
    // CHUNK_SIZE vem de keeply.hpp (tamanho maximo do chunk bruto)
    static constexpr std::size_t kMaxChunkRawSize = static_cast<std::size_t>(CHUNK_SIZE);

    // Limite conservador para payload comprimido.
    // Mantem folga para overhead de formato/compressao sem aceitar valores absurdos.
    static constexpr std::size_t kMaxChunkCompSize =
        (static_cast<std::size_t>(CHUNK_SIZE) * 2u) + 4096u;

    // Antes havia "64" magic number no read path.
    static constexpr std::uint32_t kMaxCompAlgoLen = 64;

    #pragma pack(push, 1)
    struct ChunkPackRecHeader {
        char magic[8];               // kChunkPackRecMagic
        std::uint64_t rawSize;       // tamanho original do chunk
        std::uint64_t compSize;      // tamanho comprimido (payload)
        unsigned char sha256[32];    // hash do chunk (chave de dedupe)
        std::uint32_t algoLen;       // bytes de comp_algo (sem '\0')
        std::uint32_t version;       // 1
    };
    #pragma pack(pop)

    static_assert(sizeof(ChunkPackRecHeader) == 64, "ChunkPackRecHeader inesperado");

    static fs::path chunkPackPathFromArchive(const fs::path& archiveDbPath) {
        return fs::path(archiveDbPath.string() + ".keeply");
    }

    static void writeExact(std::ostream& os, const void* data, std::size_t len) {
        if (len == 0) return;
        os.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(len));
        if (!os) throw std::runtime_error("Falha escrevendo pack de chunks.");
    }

    static void readExact(std::istream& is, void* data, std::size_t len) {
        if (len == 0) return;
        is.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(len));
        if (!is) throw std::runtime_error("Falha lendo pack de chunks.");
    }

    static void initializeChunkPackFile(const fs::path& packPath) {
        std::ofstream out(packPath, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Nao foi possivel criar pack de chunks: " + packPath.string());
        }

        writeExact(out, kChunkPackFileMagic, sizeof(kChunkPackFileMagic));
        out.flush();
        if (!out) {
            throw std::runtime_error("Falha inicializando pack de chunks: " + packPath.string());
        }
    }

    // Somente valida (nao cria arquivo, nao cria diretorio)
    static void validateChunkPackFileForRead(const fs::path& packPath) {
        std::error_code ec;
        if (!fs::exists(packPath, ec) || ec) {
            throw std::runtime_error("Pack de chunks ausente: " + packPath.string());
        }

        const auto sz = fs::file_size(packPath, ec);
        if (ec) {
            throw std::runtime_error("Nao foi possivel ler tamanho do pack de chunks: " + packPath.string());
        }
        if (sz < sizeof(kChunkPackFileMagic)) {
            throw std::runtime_error("Pack de chunks truncado/invalido: " + packPath.string());
        }

        std::ifstream in(packPath, std::ios::binary);
        if (!in) {
            throw std::runtime_error("Nao foi possivel abrir pack de chunks: " + packPath.string());
        }

        char magic[8];
        readExact(in, magic, sizeof(magic));
        if (std::memcmp(magic, kChunkPackFileMagic, sizeof(magic)) != 0) {
            throw std::runtime_error("Pack de chunks invalido (magic incorreto): " + packPath.string());
        }
    }

    // Garante pack pronto para escrita (pode criar/inicializar se ausente)
    static void ensureChunkPackFileReadyForWrite(const fs::path& packPath) {
        std::error_code ec;
        if (!packPath.parent_path().empty()) {
            fs::create_directories(packPath.parent_path(), ec); // best effort
        }

        if (!fs::exists(packPath, ec) || fs::file_size(packPath, ec) == 0) {
            initializeChunkPackFile(packPath);
            return;
        }

        validateChunkPackFileForRead(packPath);
    }

    static std::string chunkPackLockKey(const fs::path& packPath) {
        std::error_code ec;
        fs::path abs = fs::absolute(packPath, ec);
        if (ec) return packPath.lexically_normal().string();
        return abs.lexically_normal().string();
    }

    static std::shared_ptr<std::mutex> appendMutexForChunkPack(const fs::path& packPath) {
        static std::mutex registryMu;
        static std::unordered_map<std::string, std::shared_ptr<std::mutex>> registry;

        const std::string key = chunkPackLockKey(packPath);
        std::lock_guard<std::mutex> lock(registryMu);

        auto it = registry.find(key);
        if (it != registry.end()) {
            return it->second;
        }

        auto m = std::make_shared<std::mutex>();
        registry.emplace(key, m);
        return m;
    }

    static sqlite3_int64 appendChunkToPack(const fs::path& packPath,
                                        const ChunkHash& sha,
                                        std::size_t rawSize,
                                        const std::vector<unsigned char>& comp,
                                        const std::string& compAlgo) {
        // Intra-process lock por pack. Nao cobre multiplos processos.
        auto packAppendMu = appendMutexForChunkPack(packPath);
        std::lock_guard<std::mutex> appendLock(*packAppendMu);

        // Sanidade no write path (simetrico ao read path)
        if (rawSize > kMaxChunkRawSize) {
            throw std::runtime_error("rawSize excede limite de sanidade para chunk.");
        }
        if (comp.size() > kMaxChunkCompSize) {
            throw std::runtime_error("compSize excede limite de sanidade para chunk.");
        }
        if (compAlgo.size() > static_cast<std::size_t>(kMaxCompAlgoLen)) {
            throw std::runtime_error("comp_algo excede limite de tamanho.");
        }

        ensureChunkPackFileReadyForWrite(packPath);

        std::fstream io(packPath, std::ios::binary | std::ios::in | std::ios::out);
        if (!io) {
            throw std::runtime_error("Falha abrindo pack de chunks para append: " + packPath.string());
        }

        io.seekp(0, std::ios::end);
        const std::streamoff off = io.tellp();
        if (off < 0) {
            throw std::runtime_error("tellp falhou no pack de chunks.");
        }

        ChunkPackRecHeader hdr{};
        std::memcpy(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic));
        hdr.rawSize = static_cast<std::uint64_t>(rawSize);
        hdr.compSize = static_cast<std::uint64_t>(comp.size());
        std::memcpy(hdr.sha256, sha.data(), sha.size());
        hdr.algoLen = static_cast<std::uint32_t>(compAlgo.size());
        hdr.version = 1;

        writeExact(io, &hdr, sizeof(hdr));
        writeExact(io, compAlgo.data(), compAlgo.size());
        writeExact(io, comp.data(), comp.size());

        io.flush();
        if (!io) {
            throw std::runtime_error("Falha finalizando append no pack de chunks.");
        }

        return static_cast<sqlite3_int64>(off);
    }

    class ChunkPackReader {
    public:
        explicit ChunkPackReader(const fs::path& packPath)
            : in_() {
            // Importante: valida para leitura sem criar/mutar estado.
            validateChunkPackFileForRead(packPath);

            in_.open(packPath, std::ios::binary);
            if (!in_) {
                throw std::runtime_error("Falha abrindo pack de chunks para leitura: " + packPath.string());
            }
        }

        std::vector<unsigned char> readAt(sqlite3_int64 recordOffset,
                                        const ChunkHash& expectedSha,
                                        std::size_t expectedRawSize,
                                        std::size_t expectedCompSize,
                                        const std::string& expectedAlgo) {
            if (recordOffset < static_cast<sqlite3_int64>(sizeof(kChunkPackFileMagic))) {
                throw std::runtime_error("Offset invalido no pack de chunks.");
            }

            // Defesa contra SQLite corrompido (mesmo antes de ler header)
            if (expectedRawSize > kMaxChunkRawSize) {
                throw std::runtime_error("raw_size invalido no SQLite (acima do limite).");
            }
            if (expectedCompSize > kMaxChunkCompSize) {
                throw std::runtime_error("comp_size invalido no SQLite (acima do limite).");
            }
            if (expectedAlgo.size() > static_cast<std::size_t>(kMaxCompAlgoLen)) {
                throw std::runtime_error("comp_algo invalido no SQLite (acima do limite).");
            }

            in_.clear(); // limpa eof/fail caso tenha ocorrido leitura anterior
            in_.seekg(static_cast<std::streamoff>(recordOffset), std::ios::beg);
            if (!in_) {
                throw std::runtime_error("Offset invalido no pack de chunks.");
            }

            ChunkPackRecHeader hdr{};
            readExact(in_, &hdr, sizeof(hdr));

            if (std::memcmp(hdr.magic, kChunkPackRecMagic, sizeof(hdr.magic)) != 0) {
                throw std::runtime_error("Record invalido no pack de chunks (magic).");
            }
            if (hdr.version != 1) {
                throw std::runtime_error("Versao de record de chunk nao suportada.");
            }

            // Defesa explicita contra header corrompido/malicioso:
            // mesmo se SQLite e pack "concordarem", nao aceitamos valores absurdos.
            if (hdr.rawSize > static_cast<std::uint64_t>(kMaxChunkRawSize)) {
                throw std::runtime_error("raw_size invalido no pack (acima do limite).");
            }
            if (hdr.compSize > static_cast<std::uint64_t>(kMaxChunkCompSize)) {
                throw std::runtime_error("comp_size invalido no pack (acima do limite).");
            }
            if (hdr.algoLen > kMaxCompAlgoLen) {
                throw std::runtime_error("comp_algo com tamanho invalido no pack.");
            }

            if (hdr.rawSize != static_cast<std::uint64_t>(expectedRawSize)) {
                throw std::runtime_error("raw_size divergente entre SQLite e pack.");
            }
            if (hdr.compSize != static_cast<std::uint64_t>(expectedCompSize)) {
                throw std::runtime_error("comp_size divergente entre SQLite e pack.");
            }
            if (std::memcmp(hdr.sha256, expectedSha.data(), expectedSha.size()) != 0) {
                throw std::runtime_error("SHA-256 do chunk divergente entre SQLite e pack.");
            }

            std::string algo(hdr.algoLen, '\0');
            if (hdr.algoLen > 0) {
                readExact(in_, algo.data(), hdr.algoLen);
            }
            if (algo != expectedAlgo) {
                throw std::runtime_error("comp_algo divergente entre SQLite e pack.");
            }

            std::vector<unsigned char> comp(static_cast<std::size_t>(hdr.compSize));
            if (hdr.compSize > 0) {
                readExact(in_, comp.data(), static_cast<std::size_t>(hdr.compSize));
            }

            return comp;
        }

    private:
        std::ifstream in_;
    };

    static bool tableHasColumn(sqlite3* db, const char* table, const char* colName) {
        std::string sql = "PRAGMA table_info(" + std::string(table) + ");";
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
            throw SqliteError(sqlite3_errmsg(db));
        }

        bool found = false;
        try {
            while (true) {
                int rc = sqlite3_step(st);
                if (rc == SQLITE_ROW) {
                    // col 1 = name
                    const unsigned char* name = sqlite3_column_text(st, 1);
                    if (name && std::string(reinterpret_cast<const char*>(name)) == colName) {
                        found = true;
                        break;
                    }
                } else if (rc == SQLITE_DONE) {
                    break;
                } else {
                    throw SqliteError(sqlite3_errmsg(db));
                }
            }
        } catch (...) {
            sqlite3_finalize(st);
            throw;
        }

        sqlite3_finalize(st);
        return found;
    }

    static void assertChunksExternalSchemaOrThrow(sqlite3* db) {
        const bool hasPackOffset = tableHasColumn(db, "chunks", "pack_offset");
        const bool hasDataBlob   = tableHasColumn(db, "chunks", "data");

        if (!hasPackOffset) {
            if (hasDataBlob) {
                throw std::runtime_error(
                    "Schema antigo detectado: tabela chunks ainda usa coluna BLOB 'data'. "
                    "Este build usa pack externo (chunks.pack_offset). "
                    "Recrie o arquivo/archive ou rode migracao blob->pack antes de abrir."
                );
            }
            throw std::runtime_error(
                "Tabela chunks sem coluna pack_offset. Schema incompativel com storage externo."
            );
        }
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

        // SOMENTE metadados no SQLite. Conteudo comprimido vai para pack externo.
        exec(R"SQL(
            CREATE TABLE IF NOT EXISTS chunks (
                hash_sha256 BLOB PRIMARY KEY,
                raw_size INTEGER NOT NULL,
                comp_size INTEGER NOT NULL,
                comp_algo TEXT NOT NULL,
                pack_offset INTEGER NOT NULL
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
        // Garante schema novo (sem BLOB).
        // Importante: nao criar/validar pack aqui para evitar efeito colateral em fluxos de leitura/restore.
        // O pack sera:
        // - criado no write path (appendChunkToPack -> ensureChunkPackFileReadyForWrite)
        // - validado no read path (ChunkPackReader -> validateChunkPackFileForRead)
        assertChunksExternalSchemaOrThrow(db_.raw());
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
                INSERT OR IGNORE INTO chunks(hash_sha256, raw_size, comp_size, comp_algo, pack_offset)
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
        // Dedupe rapido antes de escrever no pack (evita append inutil)
        {
            Stmt check(db_.raw(), "SELECT 1 FROM chunks WHERE hash_sha256 = ? LIMIT 1;");
            check.bindBlob(1, sha.data(), static_cast<int>(sha.size()));
            if (check.stepRow()) {
                return false; // ja existe
            }
        }

        // Grava o blob comprimido no pack externo (append-only)
        // Nota: se SQLite der rollback depois, pode sobrar chunk orfao no pack.
        // Isso eh aceitavel em MVP append-only (depois um GC resolve).
        const fs::path packPath = chunkPackPathFromArchive(path_);
        const sqlite3_int64 packOffset = appendChunkToPack(packPath, sha, rawSize, comp, compAlgo);

        // Salva apenas metadados + offset no SQLite
        sqlite3_stmt* st = hot_.insertChunkIfMissing;
        resetStmt(st);

        bindBlobOrThrow(db_.raw(), st, 1, sha.data(), static_cast<int>(sha.size()));
        bindInt64OrThrow(db_.raw(), st, 2, static_cast<sqlite3_int64>(rawSize));
        bindInt64OrThrow(db_.raw(), st, 3, static_cast<sqlite3_int64>(comp.size()));
        bindTextOrThrow(db_.raw(), st, 4, compAlgo);
        bindInt64OrThrow(db_.raw(), st, 5, packOffset);

        stepDoneOrThrow(db_.raw(), st);

        // INSERT OR IGNORE: >0 inseriu, 0 ja existia
        // (Em corrida rara, pode sobrar record orfao no pack - normal em append-only)
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

    // Bulk insert de file_chunks reutilizando o prepared statement "hot".
    // Se o caller ainda nao abriu transacao, abrimos uma local para manter desempenho.
    void StorageArchive::addFileChunksBulk(sqlite3_int64 fileId,
                                        const std::vector<PendingFileChunk>& rows) {
        if (rows.empty()) return;

        const bool startedLocalTx = (sqlite3_get_autocommit(db_.raw()) != 0);
        if (startedLocalTx) begin();

        try {
            for (const auto& r : rows) {
                addFileChunk(fileId, r.chunkIdx, r.chunkHash, r.rawSize);
            }
            if (startedLocalTx) commit();
        } catch (...) {
            if (startedLocalTx) rollback();
            throw;
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
        ChunkPackReader reader(chunkPackPathFromArchive(path_));

        Stmt st(db_.raw(), R"SQL(
            SELECT
                fc.chunk_index,
                fc.raw_size,
                c.comp_size,
                c.comp_algo,
                c.pack_offset,
                fc.chunk_hash_sha256
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

            const sqlite3_int64 packOffset = sqlite3_column_int64(st.get(), 4);
            ChunkHash chunkSha{};
            const void* hashPtr = sqlite3_column_blob(st.get(), 5);
            const int hashBytes = sqlite3_column_bytes(st.get(), 5);
            if (hashPtr == nullptr || hashBytes != static_cast<int>(chunkSha.size())) {
                throw std::runtime_error("Hash de chunk invalido ao carregar restore.");
            }
            std::memcpy(chunkSha.data(), hashPtr, chunkSha.size());
            row.blob = reader.readAt(packOffset, chunkSha, row.rawSize, row.compSize, row.algo);
            out.push_back(std::move(row));}
        return out;}
    std::vector<std::string> StorageArchive::listSnapshotPaths(sqlite3_int64 snapshotId) {
        std::vector<std::string> paths;
        Stmt st(db_.raw(), "SELECT path FROM files WHERE snapshot_id = ? ORDER BY path;");
        st.bindInt64(1, snapshotId);
        while (st.stepRow()) {
            paths.emplace_back(colText(st.get(), 0));}
        return paths;
    }
    }
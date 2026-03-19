#pragma once

// =============================================================================
// sqlite_util.hpp
// RAII wrappers compartilhados para SQLite — elimina SqlTxn/SqlTransaction
// duplicados em arquivo_armazenamento.cpp e rastreamento_mudancas.cpp.
// =============================================================================

#include <sqlite3.h>
#include <stdexcept>
#include <string>

namespace keeply {

/// RAII transaction wrapper para SQLite.
/// Faz ROLLBACK automático no destrutor se commit() não foi chamado.
/// Substitui SqlTxn (arquivo_armazenamento.cpp) e SqlTransaction (rastreamento_mudancas.cpp).
class SharedSqlTransaction {
    sqlite3* db_ = nullptr;
    bool committed_ = false;

public:
    explicit SharedSqlTransaction(sqlite3* db) : db_(db) {
        char* err = nullptr;
        if (sqlite3_exec(db_, "BEGIN IMMEDIATE;", nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);
        }
    }

    ~SharedSqlTransaction() {
        if (!committed_) sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
    }

    SharedSqlTransaction(const SharedSqlTransaction&) = delete;
    SharedSqlTransaction& operator=(const SharedSqlTransaction&) = delete;

    void commit() {
        char* err = nullptr;
        if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);
        }
        committed_ = true;
    }

    bool committed() const noexcept { return committed_; }
};

/// RAII wrapper para sqlite3_stmt prepared statements.
/// Substitui SqlStmt (rastreamento_mudancas.cpp) local.
class SharedSqlStmt {
    sqlite3_stmt* st_ = nullptr;

public:
    SharedSqlStmt(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &st_, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db));
    }

    ~SharedSqlStmt() {
        if (st_) sqlite3_finalize(st_);
    }

    SharedSqlStmt(const SharedSqlStmt&) = delete;
    SharedSqlStmt& operator=(const SharedSqlStmt&) = delete;

    sqlite3_stmt* get() const noexcept { return st_; }
};

/// Executa SQL arbitrário, lançando exceção com contexto em caso de falha.
inline void execSqlOrThrow(sqlite3* db, const char* sql, const char* ctx = nullptr) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "erro sqlite";
        if (err) sqlite3_free(err);
        if (ctx) msg = std::string(ctx) + ": " + msg;
        throw std::runtime_error(msg);
    }
}

} // namespace keeply

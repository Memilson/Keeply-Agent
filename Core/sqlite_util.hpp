#pragma once
#include <sqlite3.h>
#include <filesystem>
#include <stdexcept>
#include <string>
namespace keeply {
namespace fs = std::filesystem;
int sqlite3_open_path(const fs::path& path, sqlite3** db);
class SharedSqlTransaction {
    sqlite3* db_ = nullptr;
    bool committed_ = false;
public:
    explicit SharedSqlTransaction(sqlite3* db) : db_(db) {
        char* err = nullptr;
        if (sqlite3_exec(db_, "BEGIN IMMEDIATE;", nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);}}
    ~SharedSqlTransaction() {
        if (!committed_) sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);}
    SharedSqlTransaction(const SharedSqlTransaction&) = delete;
    SharedSqlTransaction& operator=(const SharedSqlTransaction&) = delete;
    void commit() {
        char* err = nullptr;
        if (sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, &err) != SQLITE_OK) {
            std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error(msg);}
        committed_ = true;}
    bool committed() const noexcept { return committed_; }
};
class SharedSqlStmt {
    sqlite3_stmt* st_ = nullptr;
public:
    SharedSqlStmt(sqlite3* db, const char* sql) {
        if (sqlite3_prepare_v2(db, sql, -1, &st_, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db));}
    ~SharedSqlStmt() {
        if (st_) sqlite3_finalize(st_);}
    SharedSqlStmt(const SharedSqlStmt&) = delete;
    SharedSqlStmt& operator=(const SharedSqlStmt&) = delete;
    sqlite3_stmt* get() const noexcept { return st_; }
};
inline void execSqlOrThrow(sqlite3* db, const char* sql, const char* ctx = nullptr) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "erro sqlite";
        if (err) sqlite3_free(err);
        if (ctx) msg = std::string(ctx) + ": " + msg;
        throw std::runtime_error(msg);}}
// Opens a sqlite3 DB at `path` and applies Keeply's standard PRAGMAs
// (incremental auto_vacuum, WAL, normal sync, foreign keys, in-memory temp store).
// On failure throws std::runtime_error with `errorCtx` prepended.
// Caller owns the returned handle and must sqlite3_close() it.
inline sqlite3* openKeeplyDb(const fs::path& path, const char* errorCtx = "sqlite open") {
    sqlite3* db = nullptr;
    if (sqlite3_open_path(path, &db) != SQLITE_OK) {
        std::string msg = db ? sqlite3_errmsg(db) : "sqlite open failed";
        if (db) sqlite3_close(db);
        throw std::runtime_error(std::string(errorCtx) + ": " + msg);}
    // auto_vacuum só tem efeito se setado antes de qualquer CREATE TABLE em DB novo;
    // em DBs existentes é no-op silencioso (intencional).
    sqlite3_exec(db, "PRAGMA auto_vacuum=INCREMENTAL;", nullptr, nullptr, nullptr);
    try {
        execSqlOrThrow(db, "PRAGMA journal_mode=WAL;", errorCtx);
        execSqlOrThrow(db, "PRAGMA synchronous=NORMAL;", errorCtx);
        execSqlOrThrow(db, "PRAGMA foreign_keys=ON;", errorCtx);
        execSqlOrThrow(db, "PRAGMA temp_store=MEMORY;", errorCtx);
    } catch (...) {
        sqlite3_close(db);
        throw;}
    return db;}}
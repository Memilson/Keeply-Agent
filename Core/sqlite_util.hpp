#pragma once
#include <sqlite3.h>
#include <stdexcept>
#include <string>
namespace keeply {
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
        throw std::runtime_error(msg);}}}
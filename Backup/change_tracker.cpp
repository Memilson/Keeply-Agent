// =============================================================================
// change_tracker.cpp  —  Keeply Change-Based Tracking
//
// Correções aplicadas:
//  [FIX-7]  Linux inotify marcado como NOT AVAILABLE para CBT incremental:
//           inotify NÃO persiste entre execuções — watches são destruídas
//           ao fechar o processo. Arquivos modificados enquanto o keeply
//           não está rodando são SILENCIOSAMENTE PERDIDOS pelo inotify.
//           O token anterior era um incremento falso (lastToken+1).
//           → LinuxFullScanFallback retorna isAvailable()=false para forçar
//             full scan seguro até fanotify+daemon ser implementado.
//  [FIX-7b] Windows USNTracker mantido: USN Journal é persistido pelo
//           kernel no disco NTFS e funciona corretamente entre execuções.
// =============================================================================

#include "keeply.hpp"
#include "change_tracker.hpp"
#include <iostream>
#include <system_error>
#include <unordered_set>
#include <unordered_map>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <filesystem>
#include <cstdint>
#include <sqlite3.h>

// =============================================================================
// Windows — USN Journal (persiste no disco entre execuções: correto)
// =============================================================================
#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>

namespace keeply {

static std::string toLowerAscii(std::string s) {
    for (char& c : s) if (c>='A'&&c<='Z') c=char(c-('A'-'a'));
    return s;
}
static std::string normWinPath(std::string s) {
    for (char& c : s) if (c=='/') c='\\';
    if (s.rfind("\\\\?\\",0)==0) s=s.substr(4);
    while (!s.empty()&&s.back()=='\\') s.pop_back();
    return toLowerAscii(s);
}

struct WinHandle {
    HANDLE h = INVALID_HANDLE_VALUE;
    ~WinHandle(){ if(h!=INVALID_HANDLE_VALUE) CloseHandle(h); }
    WinHandle() = default;
    explicit WinHandle(HANDLE x) : h(x) {}
    WinHandle(const WinHandle&) = delete;
    WinHandle& operator=(const WinHandle&) = delete;
    WinHandle(WinHandle&& o) noexcept { h=o.h; o.h=INVALID_HANDLE_VALUE; }
    WinHandle& operator=(WinHandle&& o) noexcept {
        if (this!=&o) {
            if (h!=INVALID_HANDLE_VALUE) CloseHandle(h);
            h=o.h; o.h=INVALID_HANDLE_VALUE;
        }
        return *this;
    }
    bool ok() const { return h!=INVALID_HANDLE_VALUE; }
};

class WindowsUSNTracker : public ChangeTracker {
    WinHandle             hVol;
    std::filesystem::path root;
    std::string           rootNorm;

    std::string resolvePathFromFileId(DWORDLONG fileId) {
        FILE_ID_DESCRIPTOR fd{};
        fd.dwSize=sizeof(FILE_ID_DESCRIPTOR); fd.Type=FileIdType;
        fd.FileId.QuadPart=fileId;
        HANDLE hFile=OpenFileById(hVol.h,&fd,0,
            FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
            nullptr,FILE_FLAG_BACKUP_SEMANTICS);
        if (hFile==INVALID_HANDLE_VALUE) return {};
        WinHandle hf(hFile);
        DWORD need=GetFinalPathNameByHandleA(hf.h,nullptr,0,FILE_NAME_NORMALIZED);
        if (need==0) return {};
        std::string buf; buf.resize(need);
        DWORD got=GetFinalPathNameByHandleA(hf.h,buf.data(),need,FILE_NAME_NORMALIZED);
        if (got==0||got>=need) return {};
        buf.resize(got);
        return buf;
    }

public:
    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "usn_journal"; }

    void startTracking(const std::filesystem::path& rootPath) override {
        root=std::filesystem::absolute(rootPath);
        auto rn=root.root_name().string();
        if (rn.size()<2||rn[1]!=':')
            throw std::runtime_error("Root invalido para USN Tracker.");
        const std::string volPath="\\\\.\\"+rn;
        HANDLE hv=CreateFileA(volPath.c_str(),GENERIC_READ,
            FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,
            nullptr,OPEN_EXISTING,0,nullptr);
        if (hv==INVALID_HANDLE_VALUE)
            throw std::runtime_error(
                "Falha ao abrir volume NTFS. Keeply precisa rodar como Administrador.");
        hVol=WinHandle(hv);
        rootNorm=normWinPath(root.string());
        USN_JOURNAL_DATA_V0 jd{}; DWORD br=0;
        if (!DeviceIoControl(hVol.h,FSCTL_QUERY_USN_JOURNAL,nullptr,0,
                             &jd,sizeof(jd),&br,nullptr))
            throw std::runtime_error("Falha ao consultar USN Journal. O volume e NTFS?");
    }

    std::vector<ChangedFile> getChanges(std::uint64_t lastToken,
                                         std::uint64_t& newToken) override {
        std::vector<ChangedFile> changes;
        std::unordered_set<std::string> seen;

        USN_JOURNAL_DATA_V0 jd{}; DWORD br=0;
        if (!DeviceIoControl(hVol.h,FSCTL_QUERY_USN_JOURNAL,nullptr,0,
                             &jd,sizeof(jd),&br,nullptr))
            throw std::runtime_error("Falha ao consultar USN Journal.");

        if (lastToken==0) {
            newToken=static_cast<std::uint64_t>(jd.NextUsn);
            return changes;
        }

        READ_USN_JOURNAL_DATA_V0 rd{};
        rd.StartUsn   =static_cast<USN>(lastToken);
        rd.ReasonMask =USN_REASON_DATA_OVERWRITE|USN_REASON_DATA_EXTEND|
                       USN_REASON_DATA_TRUNCATION|USN_REASON_FILE_CREATE|
                       USN_REASON_FILE_DELETE|USN_REASON_RENAME_OLD_NAME|
                       USN_REASON_RENAME_NEW_NAME|USN_REASON_BASIC_INFO_CHANGE|
                       USN_REASON_SECURITY_CHANGE;
        rd.ReturnOnlyOnClose=FALSE; rd.Timeout=0; rd.BytesToWaitFor=0;
        rd.UsnJournalID=jd.UsnJournalID;

        alignas(8) char buffer[1<<16]; DWORD bytesRead=0;
        newToken=static_cast<std::uint64_t>(jd.NextUsn);

        while (true) {
            if (!DeviceIoControl(hVol.h,FSCTL_READ_USN_JOURNAL,&rd,sizeof(rd),
                                 buffer,sizeof(buffer),&bytesRead,nullptr)) break;
            if (bytesRead<=sizeof(USN)) break;
            USN* nextUsn=reinterpret_cast<USN*>(buffer);
            rd.StartUsn=*nextUsn;
            newToken=static_cast<std::uint64_t>(*nextUsn);
            auto* rec=reinterpret_cast<USN_RECORD*>(
                reinterpret_cast<unsigned char*>(buffer)+sizeof(USN));
            while (reinterpret_cast<unsigned char*>(rec) <
                   reinterpret_cast<unsigned char*>(buffer)+bytesRead) {
                if (rec->RecordLength==0) break;
                if ((rec->FileAttributes&FILE_ATTRIBUTE_DIRECTORY)==0) {
                    const bool isDel=(rec->Reason&USN_REASON_FILE_DELETE)!=0;
                    std::string full=resolvePathFromFileId(
                        static_cast<DWORDLONG>(rec->FileReferenceNumber));
                    if (!full.empty()) {
                        std::string fn=normWinPath(full);
                        if (fn.size()>=rootNorm.size() &&
                            fn.compare(0,rootNorm.size(),rootNorm)==0) {
                            std::string rel=full.substr(root.string().size());
                            if (!rel.empty()&&(rel[0]=='\\'||rel[0]=='/'))
                                rel.erase(rel.begin());
                            if (!rel.empty()&&seen.insert(rel).second)
                                changes.push_back({rel,isDel});
                        }
                    }
                }
                rec=reinterpret_cast<USN_RECORD*>(
                    reinterpret_cast<unsigned char*>(rec)+rec->RecordLength);
            }
        }
        return changes;
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<WindowsUSNTracker>();
}

} // namespace keeply

// =============================================================================
// Linux — CBT persistido em SQLite temporario
//
// O backend antigo baseado apenas em inotify nao sobrevivia entre execucoes.
// Aqui o Keeply persiste o ultimo estado conhecido dos arquivos em:
//
//   /tmp/keeply/keeplyintf.kipy
//
// A cada execucao ele compara o estado atual do filesystem com esse snapshot
// temporario e produz a lista de arquivos criados/alterados/removidos.
//
// Limite importante: como o estado fica em /tmp, uma limpeza do diretorio ou
// reboot pode apagar o banco auxiliar. Nesses casos o chamador deve cair para
// full scan seguro.
// =============================================================================
#elif defined(__linux__)

namespace keeply {

namespace {

struct LinuxTrackedFile {
    sqlite3_int64 size = 0;
    sqlite3_int64 mtime = 0;
};

class LinuxTrackerDb {
    sqlite3* db_ = nullptr;

public:
    explicit LinuxTrackerDb(const fs::path& path) {
        if (sqlite3_open(path.string().c_str(), &db_) != SQLITE_OK) {
            std::string msg = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
            if (db_) sqlite3_close(db_);
            throw std::runtime_error("Falha abrindo banco CBT Linux: " + msg);
        }
        exec("PRAGMA journal_mode=WAL;");
        exec("PRAGMA synchronous=NORMAL;");
        exec("PRAGMA foreign_keys=ON;");
        exec("CREATE TABLE IF NOT EXISTS roots ("
             "id INTEGER PRIMARY KEY AUTOINCREMENT,"
             "root_path TEXT NOT NULL UNIQUE,"
             "token INTEGER NOT NULL DEFAULT 0);");
        exec("CREATE TABLE IF NOT EXISTS files ("
             "root_id INTEGER NOT NULL,"
             "rel_path TEXT NOT NULL,"
             "size INTEGER NOT NULL,"
             "mtime INTEGER NOT NULL,"
             "PRIMARY KEY(root_id, rel_path),"
             "FOREIGN KEY(root_id) REFERENCES roots(id) ON DELETE CASCADE);");
    }

    ~LinuxTrackerDb() {
        if (db_) sqlite3_close(db_);
    }

    LinuxTrackerDb(const LinuxTrackerDb&) = delete;
    LinuxTrackerDb& operator=(const LinuxTrackerDb&) = delete;

    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_, sql, nullptr, nullptr, &err) != SQLITE_OK) {
            const std::string msg = err ? err : "erro sqlite";
            if (err) sqlite3_free(err);
            throw std::runtime_error("Falha no banco CBT Linux: " + msg);
        }
    }

    sqlite3* raw() { return db_; }
};

static constexpr const char* kLinuxTrackerBackendName = "linux_tmp_sqlite";
static const fs::path kLinuxTrackerDbPath = "/tmp/keeply/keeplyintf.kipy";

static std::string normalizeLinuxRelPath(const fs::path& p) {
    return p.generic_string();
}

static std::unordered_map<std::string, LinuxTrackedFile>
scanCurrentLinuxFiles(const fs::path& rootPath) {
    std::unordered_map<std::string, LinuxTrackedFile> files;
    std::error_code itEc;
    fs::recursive_directory_iterator it(rootPath, fs::directory_options::skip_permission_denied, itEc);
    fs::recursive_directory_iterator end;

    for (; it != end; it.increment(itEc)) {
        if (itEc) {
            itEc.clear();
            continue;
        }

        std::error_code typeEc;
        if (!it->is_regular_file(typeEc)) continue;

        std::error_code relEc;
        const fs::path relPath = fs::relative(it->path(), rootPath, relEc);
        if (relEc) continue;

        std::error_code sizeEc;
        const auto fileSize = it->file_size(sizeEc);
        if (sizeEc) continue;

        std::error_code timeEc;
        const auto mtimeRaw = fs::last_write_time(it->path(), timeEc);
        if (timeEc) continue;

        files.emplace(normalizeLinuxRelPath(relPath),
                      LinuxTrackedFile{
                          static_cast<sqlite3_int64>(fileSize),
                          static_cast<sqlite3_int64>(fileTimeToUnixSeconds(mtimeRaw))
                      });
    }

    return files;
}

} // namespace

class LinuxStateTracker : public ChangeTracker {
    fs::path root;
    std::unique_ptr<LinuxTrackerDb> db;
    sqlite3_int64 rootId = 0;
    sqlite3_int64 storedToken = 0;

    void ensureRootRow() {
        sqlite3_stmt* ins = nullptr;
        if (sqlite3_prepare_v2(db->raw(),
                               "INSERT OR IGNORE INTO roots(root_path, token) VALUES(?, 0);",
                               -1, &ins, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Falha preparando INSERT do CBT Linux.");
        }
        sqlite3_bind_text(ins, 1, root.string().c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(ins) != SQLITE_DONE) {
            sqlite3_finalize(ins);
            throw std::runtime_error("Falha inserindo root no CBT Linux.");
        }
        sqlite3_finalize(ins);

        sqlite3_stmt* sel = nullptr;
        if (sqlite3_prepare_v2(db->raw(),
                               "SELECT id, token FROM roots WHERE root_path=?;",
                               -1, &sel, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Falha preparando SELECT do CBT Linux.");
        }
        sqlite3_bind_text(sel, 1, root.string().c_str(), -1, SQLITE_TRANSIENT);
        if (sqlite3_step(sel) != SQLITE_ROW) {
            sqlite3_finalize(sel);
            throw std::runtime_error("Root do CBT Linux nao encontrado.");
        }
        rootId = sqlite3_column_int64(sel, 0);
        storedToken = sqlite3_column_int64(sel, 1);
        sqlite3_finalize(sel);
    }

    std::unordered_map<std::string, LinuxTrackedFile> loadTrackedFiles() const {
        std::unordered_map<std::string, LinuxTrackedFile> files;
        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db->raw(),
                               "SELECT rel_path, size, mtime FROM files WHERE root_id=?;",
                               -1, &st, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Falha preparando leitura do CBT Linux.");
        }
        sqlite3_bind_int64(st, 1, rootId);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const unsigned char* relText = sqlite3_column_text(st, 0);
            if (!relText) continue;
            files.emplace(
                reinterpret_cast<const char*>(relText),
                LinuxTrackedFile{
                    sqlite3_column_int64(st, 1),
                    sqlite3_column_int64(st, 2)
                }
            );
        }
        sqlite3_finalize(st);
        return files;
    }

    void persistCurrentState(const std::unordered_map<std::string, LinuxTrackedFile>& files,
                             sqlite3_int64 newToken) {
        db->exec("BEGIN IMMEDIATE;");
        try {
            sqlite3_stmt* updRoot = nullptr;
            if (sqlite3_prepare_v2(db->raw(),
                                   "UPDATE roots SET token=? WHERE id=?;",
                                   -1, &updRoot, nullptr) != SQLITE_OK) {
                throw std::runtime_error("Falha preparando update do token CBT Linux.");
            }
            sqlite3_bind_int64(updRoot, 1, newToken);
            sqlite3_bind_int64(updRoot, 2, rootId);
            if (sqlite3_step(updRoot) != SQLITE_DONE) {
                sqlite3_finalize(updRoot);
                throw std::runtime_error("Falha atualizando token do CBT Linux.");
            }
            sqlite3_finalize(updRoot);

            sqlite3_stmt* delFiles = nullptr;
            if (sqlite3_prepare_v2(db->raw(),
                                   "DELETE FROM files WHERE root_id=?;",
                                   -1, &delFiles, nullptr) != SQLITE_OK) {
                throw std::runtime_error("Falha preparando limpeza do CBT Linux.");
            }
            sqlite3_bind_int64(delFiles, 1, rootId);
            if (sqlite3_step(delFiles) != SQLITE_DONE) {
                sqlite3_finalize(delFiles);
                throw std::runtime_error("Falha limpando arquivos do CBT Linux.");
            }
            sqlite3_finalize(delFiles);

            sqlite3_stmt* insFile = nullptr;
            if (sqlite3_prepare_v2(db->raw(),
                                   "INSERT INTO files(root_id, rel_path, size, mtime) VALUES(?,?,?,?);",
                                   -1, &insFile, nullptr) != SQLITE_OK) {
                throw std::runtime_error("Falha preparando insert de arquivos CBT Linux.");
            }
            for (const auto& [relPath, info] : files) {
                sqlite3_reset(insFile);
                sqlite3_clear_bindings(insFile);
                sqlite3_bind_int64(insFile, 1, rootId);
                sqlite3_bind_text(insFile, 2, relPath.c_str(), -1, SQLITE_TRANSIENT);
                sqlite3_bind_int64(insFile, 3, info.size);
                sqlite3_bind_int64(insFile, 4, info.mtime);
                if (sqlite3_step(insFile) != SQLITE_DONE) {
                    sqlite3_finalize(insFile);
                    throw std::runtime_error("Falha gravando arquivos do CBT Linux.");
                }
            }
            sqlite3_finalize(insFile);

            db->exec("COMMIT;");
            storedToken = newToken;
        } catch (...) {
            try { db->exec("ROLLBACK;"); } catch (...) {}
            throw;
        }
    }

public:
    bool isAvailable() const override {
        return true;
    }
    const char* backendName() const override {
        return kLinuxTrackerBackendName;
    }
    void startTracking(const std::filesystem::path& rootPath) override {
        root = fs::absolute(rootPath).lexically_normal();
        std::error_code mkEc;
        fs::create_directories(kLinuxTrackerDbPath.parent_path(), mkEc);
        if (mkEc) {
            throw std::runtime_error("Falha criando diretorio do CBT Linux: " + mkEc.message());
        }
        db = std::make_unique<LinuxTrackerDb>(kLinuxTrackerDbPath);
        ensureRootRow();
    }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken,
                                         std::uint64_t& newToken) override {
        if (!db || rootId == 0) {
            throw std::runtime_error("CBT Linux nao inicializado.");
        }

        if (lastToken != 0 && storedToken != static_cast<sqlite3_int64>(lastToken)) {
            throw std::runtime_error(
                "Estado temporario do CBT Linux fora de sincronia. "
                "Banco auxiliar ausente ou token divergente."
            );
        }

        const auto previousFiles = loadTrackedFiles();
        const auto currentFiles = scanCurrentLinuxFiles(root);
        std::vector<ChangedFile> changes;
        changes.reserve(currentFiles.size());

        for (const auto& [relPath, info] : currentFiles) {
            const auto prevIt = previousFiles.find(relPath);
            if (prevIt == previousFiles.end() ||
                prevIt->second.size != info.size ||
                prevIt->second.mtime != info.mtime) {
                changes.push_back({relPath, false});
            }
        }

        for (const auto& [relPath, prevInfo] : previousFiles) {
            static_cast<void>(prevInfo);
            if (currentFiles.find(relPath) == currentFiles.end()) {
                changes.push_back({relPath, true});
            }
        }

        const sqlite3_int64 nextToken = storedToken + 1;
        persistCurrentState(currentFiles, nextToken);
        newToken = static_cast<std::uint64_t>(nextToken);
        return changes;
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<LinuxStateTracker>();
}

} // namespace keeply

// =============================================================================
// macOS / outros — sem CBT, full scan
// =============================================================================
#else

namespace keeply {

class FallbackScanner : public ChangeTracker {
public:
    bool isAvailable() const override { return false; }
    const char* backendName() const override { return "unsupported"; }
    void startTracking(const std::filesystem::path&) override {}
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken,
                                         std::uint64_t& newToken) override {
        newToken = lastToken;
        return {};
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<FallbackScanner>();
}

} // namespace keeply

#endif

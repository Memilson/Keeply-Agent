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

#include "change_tracker.hpp"
#include <iostream>
#include <unordered_set>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <filesystem>
#include <cstdint>

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
// Linux — inotify DESABILITADO para CBT
//
// [FIX-7] Por que inotify não funciona para backup incremental entre runs:
//
//   1. inotify watches são recursos voláteis de kernel (file descriptors).
//      Eles existem APENAS enquanto o processo está rodando.
//
//   2. Quando o keeply encerra, o kernel destrói TODAS as watches
//      automaticamente — sem gravar nada em disco.
//
//   3. Na próxima execução, startTracking() cria novas watches do zero,
//      mas o período entre o shutdown anterior e este startup é um
//      PONTO CEGO TOTAL: qualquer modificação neste intervalo é invisível.
//
//   4. O "token" anterior era newToken = lastToken + 1, um número
//      arbitrário sem nenhum estado real por trás.
//
// SOLUÇÃO FUTURA: daemon persistente com fanotify FAN_REPORT_FID
//   (Linux kernel 5.1+). O daemon grava eventos em arquivo de log no disco.
//   O keeply lê este log na inicialização para CBT real entre execuções.
//   Até esta implementação, isAvailable()=false força full scan seguro.
// =============================================================================
#elif defined(__linux__)

namespace keeply {

class LinuxFullScanFallback : public ChangeTracker {
public:
    bool isAvailable() const override {
        return false; // força full scan — correto e seguro
    }
    const char* backendName() const override {
        return "inotify_disabled";
    }
    void startTracking(const std::filesystem::path&) override {}
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken,
                                         std::uint64_t& newToken) override {
        newToken = lastToken;
        return {};
    }
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker() {
    return std::make_unique<LinuxFullScanFallback>();
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

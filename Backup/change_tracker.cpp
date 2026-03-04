#include "change_tracker.hpp"
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <filesystem>
#include <cstdint>
#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>
namespace keeply {
static std::string toLowerAscii(std::string s){for(char& c:s){if(c>='A'&&c<='Z') c=char(c-('A'-'a'));}return s;}
static std::string normWinPath(std::string s){for(char& c:s){if(c=='/') c='\\';}if(s.rfind("\\\\?\\",0)==0) s=s.substr(4);while(!s.empty()&&s.back()=='\\') s.pop_back();return toLowerAscii(s);}
struct WinHandle { HANDLE h=INVALID_HANDLE_VALUE; ~WinHandle(){if(h!=INVALID_HANDLE_VALUE) CloseHandle(h);} WinHandle(){} explicit WinHandle(HANDLE x):h(x){} WinHandle(const WinHandle&)=delete; WinHandle& operator=(const WinHandle&)=delete; WinHandle(WinHandle&& o) noexcept {h=o.h;o.h=INVALID_HANDLE_VALUE;} WinHandle& operator=(WinHandle&& o) noexcept {if(this!=&o){if(h!=INVALID_HANDLE_VALUE) CloseHandle(h);h=o.h;o.h=INVALID_HANDLE_VALUE;}return *this;} bool ok() const {return h!=INVALID_HANDLE_VALUE;} };
class WindowsUSNTracker : public ChangeTracker {
    WinHandle hVol;
    std::filesystem::path root;
    std::string rootNorm;
    std::string resolvePathFromFileId(DWORDLONG fileId) {
        FILE_ID_DESCRIPTOR fd{}; fd.dwSize=sizeof(FILE_ID_DESCRIPTOR); fd.Type=FileIdType; fd.FileId.QuadPart=fileId;
        HANDLE hFile=OpenFileById(hVol.h,&fd,0,FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,nullptr,FILE_FLAG_BACKUP_SEMANTICS);
        if(hFile==INVALID_HANDLE_VALUE) return {};
        WinHandle hf(hFile);
        DWORD need=GetFinalPathNameByHandleA(hf.h,nullptr,0,FILE_NAME_NORMALIZED);
        if(need==0) return {};
        std::string buf; buf.resize(need);
        DWORD got=GetFinalPathNameByHandleA(hf.h,buf.data(),need,FILE_NAME_NORMALIZED);
        if(got==0||got>=need) return {};
        buf.resize(got);
        return buf;
    }
public:
    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "usn_journal"; }
    void startTracking(const std::filesystem::path& rootPath) override {
        root=std::filesystem::absolute(rootPath);
        auto rn=root.root_name().string();
        if(rn.size()<2||rn[1]!=':') throw std::runtime_error("Root invalido para USN Tracker.");
        const std::string volPath="\\\\.\\"+rn;
        HANDLE hv=CreateFileA(volPath.c_str(),GENERIC_READ,FILE_SHARE_READ|FILE_SHARE_WRITE|FILE_SHARE_DELETE,nullptr,OPEN_EXISTING,0,nullptr);
        if(hv==INVALID_HANDLE_VALUE) throw std::runtime_error("Falha ao abrir o volume NTFS. Keeply precisa rodar como Administrador.");
        hVol=WinHandle(hv);
        rootNorm=normWinPath(root.string());
        USN_JOURNAL_DATA_V0 jd{}; DWORD br=0;
        if(!DeviceIoControl(hVol.h,FSCTL_QUERY_USN_JOURNAL,nullptr,0,&jd,sizeof(jd),&br,nullptr)) throw std::runtime_error("Falha ao consultar o USN Journal. O volume e NTFS?");
    }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        std::vector<ChangedFile> changes;
        std::unordered_set<std::string> seen;
        USN_JOURNAL_DATA_V0 jd{}; DWORD br=0;
        if(!DeviceIoControl(hVol.h,FSCTL_QUERY_USN_JOURNAL,nullptr,0,&jd,sizeof(jd),&br,nullptr)) throw std::runtime_error("Falha ao consultar o USN Journal. O volume e NTFS?");
        if(lastToken==0){newToken=static_cast<std::uint64_t>(jd.NextUsn);return changes;}
        READ_USN_JOURNAL_DATA_V0 rd{};
        rd.StartUsn=static_cast<USN>(lastToken);
        rd.ReasonMask=USN_REASON_DATA_OVERWRITE|USN_REASON_DATA_EXTEND|USN_REASON_DATA_TRUNCATION|USN_REASON_FILE_CREATE|USN_REASON_FILE_DELETE|USN_REASON_RENAME_OLD_NAME|USN_REASON_RENAME_NEW_NAME|USN_REASON_BASIC_INFO_CHANGE|USN_REASON_SECURITY_CHANGE;
        rd.ReturnOnlyOnClose=FALSE; rd.Timeout=0; rd.BytesToWaitFor=0; rd.UsnJournalID=jd.UsnJournalID;
        alignas(8) char buffer[1<<16]; DWORD bytesRead=0; newToken=static_cast<std::uint64_t>(jd.NextUsn);
        while(true){
            if(!DeviceIoControl(hVol.h,FSCTL_READ_USN_JOURNAL,&rd,sizeof(rd),buffer,sizeof(buffer),&bytesRead,nullptr)) break;
            if(bytesRead<=sizeof(USN)) break;
            USN* nextUsn=reinterpret_cast<USN*>(buffer);
            rd.StartUsn=*nextUsn;
            newToken=static_cast<std::uint64_t>(*nextUsn);
            auto* rec=reinterpret_cast<USN_RECORD*>(reinterpret_cast<unsigned char*>(buffer)+sizeof(USN));
            while(reinterpret_cast<unsigned char*>(rec)<reinterpret_cast<unsigned char*>(buffer)+bytesRead){
                if(rec->RecordLength==0) break;
                if((rec->FileAttributes&FILE_ATTRIBUTE_DIRECTORY)==0){
                    const bool isDeleted=(rec->Reason&USN_REASON_FILE_DELETE)!=0;
                    std::string full=resolvePathFromFileId(static_cast<DWORDLONG>(rec->FileReferenceNumber));
                    if(!full.empty()){
                        std::string fullNorm=normWinPath(full);
                        if(fullNorm.size()>=rootNorm.size()&&(fullNorm.compare(0,rootNorm.size(),rootNorm)==0)){
                            std::string rel=full.substr(full.size()>root.string().size()?root.string().size():root.string().size());
                            if(!rel.empty()&&(rel[0]=='\\'||rel[0]=='/')) rel.erase(rel.begin());
                            if(!rel.empty()&&seen.insert(rel).second) changes.push_back({rel,isDeleted});
                        }
                    }
                }
                rec=reinterpret_cast<USN_RECORD*>(reinterpret_cast<unsigned char*>(rec)+rec->RecordLength);
            }
        }
        return changes;
    }
};
std::unique_ptr<ChangeTracker> createPlatformChangeTracker() { return std::make_unique<WindowsUSNTracker>(); }
}
#elif defined(__linux__)
#include <fcntl.h>
#include <sys/inotify.h>
#include <unistd.h>
namespace keeply {
struct Fd { int fd=-1; ~Fd(){if(fd!=-1) close(fd);} Fd(){} explicit Fd(int x):fd(x){} Fd(const Fd&)=delete; Fd& operator=(const Fd&)=delete; Fd(Fd&& o) noexcept {fd=o.fd;o.fd=-1;} Fd& operator=(Fd&& o) noexcept {if(this!=&o){if(fd!=-1) close(fd);fd=o.fd;o.fd=-1;}return *this;} bool ok() const {return fd!=-1;} };
static std::string normPosixRoot(std::filesystem::path p){p=p.lexically_normal();auto s=p.string();while(s.size()>1&&s.back()=='/') s.pop_back();return s;}
class LinuxInotifyTracker : public ChangeTracker {
    Fd ino;
    std::filesystem::path root;
    std::string rootStr;
    std::unordered_map<int,std::string> wdToPath;
    void addWatchOne(const std::filesystem::path& p){
        const uint32_t mask=IN_CLOSE_WRITE|IN_CREATE|IN_DELETE|IN_MOVED_FROM|IN_MOVED_TO|IN_ATTRIB|IN_DELETE_SELF|IN_MOVE_SELF;
        int wd=inotify_add_watch(ino.fd,p.c_str(),mask);
        if(wd!=-1) wdToPath[wd]=p.string();
    }
    void addWatchRecursive(const std::filesystem::path& p){
        addWatchOne(p);
        std::error_code ec;
        std::filesystem::directory_options opt=std::filesystem::directory_options::skip_permission_denied;
        for(auto it=std::filesystem::directory_iterator(p,opt,ec);!ec&&it!=std::filesystem::end(it);it.increment(ec)){
            auto& e=*it;
            if(e.is_directory(ec)&&!e.is_symlink(ec)) addWatchRecursive(e.path());
        }
    }
public:
    bool isAvailable() const override { return true; }
    const char* backendName() const override { return "inotify"; }
    void startTracking(const std::filesystem::path& rootPath) override {
        root=std::filesystem::absolute(rootPath);
        rootStr=normPosixRoot(root);
        int fd=inotify_init1(IN_NONBLOCK);
        if(fd==-1) throw std::runtime_error("Falha ao inicializar inotify.");
        ino=Fd(fd);
        std::cout << "[Keeply] Indexando watches do inotify recursivamente...\n";
        addWatchRecursive(root);
    }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override {
        newToken=lastToken+1;
        std::vector<ChangedFile> changes;
        std::unordered_set<std::string> seen;
        alignas(struct inotify_event) char buffer[1<<16];
        for(;;){
            ssize_t len=read(ino.fd,buffer,sizeof(buffer));
            if(len<=0) break;
            char* ptr=buffer;
            while(ptr<buffer+len){
                const auto* ev=reinterpret_cast<const struct inotify_event*>(ptr);
                auto it=wdToPath.find(ev->wd);
                if(it!=wdToPath.end()){
                    std::string base=it->second;
                    std::string name=(ev->len>0)?std::string(ev->name):std::string();
                    std::string full=name.empty()?base:(base+"/"+name);
                    bool isDir=(ev->mask&IN_ISDIR)!=0;
                    bool del=(ev->mask&IN_DELETE)!=0||(ev->mask&IN_MOVED_FROM)!=0;
                    bool cre=(ev->mask&IN_CREATE)!=0||(ev->mask&IN_MOVED_TO)!=0;
                    if(!isDir){
                        if(full.size()>rootStr.size()+1&&full.rfind(rootStr+"/",0)==0){
                            std::string rel=full.substr(rootStr.size()+1);
                            if(!rel.empty()&&seen.insert(rel).second) changes.push_back({rel,del});
                        }
                    }
                    if(isDir&&cre){
                        std::error_code ec;
                        if(std::filesystem::is_directory(full,ec)) addWatchRecursive(full);
                    }
                }
                ptr+=static_cast<std::ptrdiff_t>(sizeof(struct inotify_event)+ev->len);
            }
        }
        return changes;
    }
};
std::unique_ptr<ChangeTracker> createPlatformChangeTracker() { return std::make_unique<LinuxInotifyTracker>(); }
}
#else
namespace keeply {
class FallbackScanner : public ChangeTracker {
public:
    bool isAvailable() const override { return false; }
    const char* backendName() const override { return "unsupported"; }
    void startTracking(const std::filesystem::path&) override { std::cout << "[Keeply] Aviso: SO nao suporta CBT nativo. Rastreamento incremental puro indisponivel.\n"; }
    std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) override { newToken=lastToken; return {}; }
};
std::unique_ptr<ChangeTracker> createPlatformChangeTracker() { return std::make_unique<FallbackScanner>(); }
}
#endif
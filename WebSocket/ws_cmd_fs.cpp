#include "websocket_agente.hpp"
#include "websocket_interno.hpp"
#include "../HTTP/http_util.hpp"
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>
namespace {
namespace fs = std::filesystem;
using namespace keeply::ws_internal;
struct FsListRow{
    bool isDir=false;
    std::string name;
    std::string fullPath;
    std::uintmax_t size=0;};
struct LsblkEntry{
    std::string name;
    std::string label;
    std::string model;
    std::string type;
    std::string mountpoint;
    std::string parentName;};
struct DiskMountCandidate{
    std::string label;
    std::string mountpoint;
    int score=0;};
std::map<std::string,std::string> parseKeyValueLine(const std::string& line){
    std::map<std::string,std::string> out;
    std::size_t i=0;
    while(i<line.size()){
        while(i<line.size()&&std::isspace(static_cast<unsigned char>(line[i]))) ++i;
        if(i>=line.size()) break;
        const std::size_t eq=line.find('=',i);
        if(eq==std::string::npos) break;
        const std::string key=line.substr(i,eq-i);
        i=eq+1;
        std::string value;
        if(i<line.size()&&line[i]=='"'){
            ++i;
            while(i<line.size()){
                if(line[i]=='"'&&(i==0||line[i-1]!='\\')){
                    ++i;
                    break;}
                value.push_back(line[i++]);}
        }else{
            const std::size_t next=line.find(' ',i);
            value=line.substr(i,next==std::string::npos?std::string::npos:next-i);
            i=next==std::string::npos?line.size():next;}
        out[key]=value;}
    return out;}
bool shouldSkipMountType(const std::string& fsType){
    static const std::set<std::string> skipped={
        "proc","sysfs","tmpfs","devtmpfs","devpts","cgroup","cgroup2","mqueue","overlay","squashfs",
        "nsfs","tracefs","securityfs","pstore","debugfs","configfs","fusectl","binfmt_misc","ramfs",
        "autofs","hugetlbfs","rpc_pipefs","selinuxfs","bpf"
    };
    return skipped.find(fsType)!=skipped.end();}
void appendRootRow(std::vector<FsListRow>& rows,std::unordered_set<std::string>& seen,const fs::path& path){
    if(path.empty()) return;
    std::error_code ec;
    const fs::path absolutePath=fs::absolute(path,ec);
    const fs::path finalPath=ec?path:absolutePath;
    const std::string fullPath=finalPath.string();
    if(fullPath.empty()||seen.count(fullPath)) return;
    if(!fs::exists(finalPath,ec)||ec||!fs::is_directory(finalPath,ec)||ec) return;
    seen.insert(fullPath);
    FsListRow row;
    row.isDir=true;
    row.fullPath=fullPath;
    row.name=finalPath.filename().string();
    if(row.name.empty()) row.name=fullPath;
    rows.push_back(std::move(row));}
std::vector<FsListRow> listRootRows(){
    std::vector<FsListRow> rows;
    std::unordered_set<std::string> seen;
#if defined(__linux__)
    std::vector<LsblkEntry> entries;
    if(FILE* pipe=popen("lsblk -P -o NAME,LABEL,MODEL,TYPE,MOUNTPOINT,PKNAME 2>/dev/null","r")){
        char buffer[4096];
        while(std::fgets(buffer,sizeof(buffer),pipe)){
            const auto kv=parseKeyValueLine(buffer);
            LsblkEntry entry;
            entry.name=keeply::trim(kv.count("NAME")?kv.at("NAME"):"");
            entry.label=keeply::trim(kv.count("LABEL")?kv.at("LABEL"):"");
            entry.model=keeply::trim(kv.count("MODEL")?kv.at("MODEL"):"");
            entry.type=keeply::trim(kv.count("TYPE")?kv.at("TYPE"):"");
            entry.mountpoint=keeply::trim(kv.count("MOUNTPOINT")?kv.at("MOUNTPOINT"):"");
            entry.parentName=keeply::trim(kv.count("PKNAME")?kv.at("PKNAME"):"");
            if(!entry.name.empty()) entries.push_back(std::move(entry));}
        pclose(pipe);}
    std::map<std::string,std::string> diskLabels;
    for(const auto& entry:entries){
        if(entry.type!="disk"||entry.name.rfind("loop",0)==0) continue;
        const std::string trimmedLabel=keeply::trim(entry.label);
        const std::string trimmedModel=keeply::trim(entry.model);
        diskLabels[entry.name]=!trimmedLabel.empty()?trimmedLabel:!trimmedModel.empty()?trimmedModel:entry.name;}
    std::map<std::string,DiskMountCandidate> bestMountByDisk;
    for(const auto& entry:entries){
        if(entry.mountpoint.empty()||entry.type=="loop") continue;
        std::string diskKey=!entry.parentName.empty()?entry.parentName:entry.name;
        if(diskKey.rfind("loop",0)==0) continue;
        std::string label=diskLabels.count(diskKey)?diskLabels[diskKey]:(!entry.model.empty()?entry.model:!entry.label.empty()?entry.label:diskKey);
        int score=10;
        if(entry.mountpoint=="/") score=100;
        else if(entry.mountpoint.rfind("/media/",0)==0||entry.mountpoint.rfind("/run/media/",0)==0) score=80;
        else if(entry.mountpoint.rfind("/mnt/",0)==0) score=70;
        else if(entry.mountpoint=="/boot/efi") score=5;
        auto it=bestMountByDisk.find(diskKey);
        if(it==bestMountByDisk.end()||score>it->second.score) bestMountByDisk[diskKey]=DiskMountCandidate{label,entry.mountpoint,score};}
    for(const auto& [diskKey,candidate]:bestMountByDisk){
        FsListRow row;
        row.isDir=true;
        row.name=candidate.label;
        row.fullPath=candidate.mountpoint;
        if(!row.fullPath.empty()&&!seen.count(row.fullPath)){
            seen.insert(row.fullPath);
            rows.push_back(std::move(row));}}
    if(rows.empty()){
        std::ifstream mounts("/proc/mounts");
        std::string device;
        std::string mountPoint;
        std::string fsType;
        while(mounts>>device>>mountPoint>>fsType){
            std::string restOfLine;
            std::getline(mounts,restOfLine);
            if(shouldSkipMountType(fsType)) continue;
            if(mountPoint=="/"||mountPoint.rfind("/mnt/",0)==0||mountPoint.rfind("/media/",0)==0||mountPoint.rfind("/run/media/",0)==0) appendRootRow(rows,seen,fs::path(mountPoint));}}
    appendRootRow(rows,seen,fs::path("/"));
#endif
    std::sort(rows.begin(),rows.end(),[](const FsListRow& a,const FsListRow& b){return toLower(a.name)<toLower(b.name);});
    return rows;}}
namespace keeply {
std::string KeeplyAgentWsClient::buildFsDisksJson_(const std::string& requestId) const{
    const auto rows=listRootRows();
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"fs.disks\","<<"\"requestId\":\""<<escapeJson(trim(requestId))<<"\","<<"\"path\":\"\","<<"\"parentPath\":\"\","<<"\"truncated\":false,"<<"\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& row=rows[i];
        oss<<"{"<<"\"name\":\""<<escapeJson(row.name)<<"\","<<"\"path\":\""<<escapeJson(row.fullPath)<<"\","<<"\"kind\":\"dir\","<<"\"size\":0"<<"}";}
    oss<<"]}";
    return oss.str();}
std::string KeeplyAgentWsClient::buildFsListJson_(const std::string& requestId,const std::string& requestedPath) const{
    std::string targetRaw=trim(requestedPath);
    const auto& s=api_->state();
    if(targetRaw.empty()) targetRaw=trim(s.source);
    std::error_code ec;
    fs::path targetPath=targetRaw.empty()?fs::current_path(ec):fs::path(targetRaw);
    if(ec){
        ec.clear();
        targetPath=fs::path(".");}
    if(!targetPath.is_absolute()){
        targetPath=fs::absolute(targetPath,ec);
        if(ec){
            std::ostringstream err;
            err<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson(trim(requestId))<<"\",\"path\":\""<<escapeJson(requestedPath)<<"\",\"error\":\"falha-ao-resolver-caminho\",\"items\":[]}";
            return err.str();}}
    if(!fs::exists(targetPath,ec)||ec){
        std::ostringstream err;
        err<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson(trim(requestId))<<"\",\"path\":\""<<escapeJson(targetPath.string())<<"\",\"error\":\"caminho-nao-encontrado\",\"items\":[]}";
        return err.str();}
    if(!fs::is_directory(targetPath,ec)||ec){
        ec.clear();
        const fs::path parent=targetPath.parent_path();
        if(parent.empty()||!fs::is_directory(parent,ec)||ec){
            std::ostringstream err;
            err<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson(trim(requestId))<<"\",\"path\":\""<<escapeJson(targetPath.string())<<"\",\"error\":\"caminho-nao-e-diretorio\",\"items\":[]}";
            return err.str();}
        targetPath=parent;}
    static constexpr std::size_t kFsListMaxItems=120;
    static constexpr std::size_t kFsListMaxEstimatedJsonBytes=32*1024;
    std::vector<FsListRow> rows;
    rows.reserve(kFsListMaxItems);
    bool truncated=false;
    std::size_t estimatedJsonBytes=256;
    for(fs::directory_iterator it(targetPath,fs::directory_options::skip_permission_denied,ec),end;it!=end;it.increment(ec)){
        if(ec){ec.clear();continue;}
        const fs::path itemPath=it->path();
        const std::string name=itemPath.filename().string();
        if(name.empty()) continue;
        FsListRow row;
        row.name=name;
        row.fullPath=itemPath.string();
        row.isDir=it->is_directory(ec);
        if(ec){
            ec.clear();
            row.isDir=false;}
        if(!row.isDir) continue;
        row.size=0;
        const std::size_t rowEstimate=row.name.size()+row.fullPath.size()+96u;
        if(!rows.empty()&&estimatedJsonBytes+rowEstimate>kFsListMaxEstimatedJsonBytes){
            truncated=true;
            break;}
        rows.push_back(std::move(row));
        estimatedJsonBytes+=rowEstimate;
        if(rows.size()>=kFsListMaxItems){
            truncated=true;
            break;}}
    std::sort(rows.begin(),rows.end(),[](const FsListRow& a,const FsListRow& b){
        if(a.isDir!=b.isDir) return a.isDir>b.isDir;
        return toLower(a.name)<toLower(b.name);
    });
    const std::string parentPath=targetPath.has_parent_path()?targetPath.parent_path().string():"";
    std::ostringstream oss;
    oss<<"{\"type\":\"fs.list\",\"requestId\":\""<<escapeJson(trim(requestId))<<"\",\"path\":\""<<escapeJson(targetPath.string())<<"\",\"parentPath\":\""<<escapeJson(parentPath)<<"\",\"truncated\":"<<(truncated?"true":"false")<<",\"items\":[";
    for(std::size_t i=0;i<rows.size();++i){
        if(i) oss<<",";
        const auto& row=rows[i];
        oss<<"{\"name\":\""<<escapeJson(row.name)<<"\",\"path\":\""<<escapeJson(row.fullPath)<<"\",\"kind\":\""<<(row.isDir?"dir":"file")<<"\",\"size\":"<<row.size<<"}";}
    oss<<"]}";
    return oss.str();}}
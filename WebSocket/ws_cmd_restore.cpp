#include "websocket_agente.hpp"
#include "websocket_interno.hpp"
#include "../HTTP/http_util.hpp"
#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>
namespace {
namespace fs = std::filesystem;
using namespace keeply::ws_internal;
std::string jsonStrField(const std::string& key,const std::string& value){return "\"" + key + "\":\"" + escapeJson(value) + "\"";}
std::vector<std::string> splitNonEmptyPipe(const std::string& value){
    std::vector<std::string> out;
    for(const auto& part : splitPipe(value)){
        const std::string trimmed = keeply::trim(part);
        if(!trimmed.empty()) out.push_back(trimmed);}
    return out;}
void ensureDirectory(const fs::path& path){
    std::error_code ec;
    fs::create_directories(path, ec);
    if(ec) throw std::runtime_error("Falha criando diretorio temporario de restore: " + ec.message());}
void writeBinaryFile(const fs::path& path,const std::string& bytes){
    if(!path.parent_path().empty()) ensureDirectory(path.parent_path());
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if(!out) throw std::runtime_error("Falha criando arquivo temporario: " + path.string());
    if(!bytes.empty()) out.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if(!out) throw std::runtime_error("Falha gravando arquivo temporario: " + path.string());}
void appendFileToStream(std::ofstream& out,const fs::path& path){
    std::ifstream in(path, std::ios::binary);
    if(!in) throw std::runtime_error("Falha abrindo parte do bundle: " + path.string());
    std::array<char, 64 * 1024> buffer{};
    while(in){
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize got = in.gcount();
        if(got <= 0) break;
        out.write(buffer.data(), got);
        if(!out) throw std::runtime_error("Falha concatenando pack temporario.");}
    if(!in.eof() && in.fail()) throw std::runtime_error("Falha lendo parte do bundle: " + path.string());}
std::string ensureRelativeHttpPath(std::string path){
    path = keeply::trim(path);
    if(path.empty()) throw std::runtime_error("downloadPathBase ausente para restore cloud.");
    if(path.front() != '/') path.insert(path.begin(), '/');
    while(path.size() > 1 && path.back() == '/') path.pop_back();
    return path;}
std::string buildBundleDownloadPath(const std::string& downloadPathBase,const std::string& fileName){
    return ensureRelativeHttpPath(downloadPathBase) + "/" + urlEncode(fileName);}
bool hasSnapshotPathPrefix(const std::string& path,const std::string& prefix){
    if(prefix.empty()) return true;
    if(path == prefix) return true;
    if(path.size() <= prefix.size()) return false;
    return path.rfind(prefix + "/", 0) == 0;}}
namespace keeply {
void KeeplyAgentWsClient::runRestoreFileCommand_(const std::string& snapshot,const std::string& relPath,const std::string& outRootRaw){
    if(snapshot.empty()||relPath.empty()) throw std::runtime_error("Formato restore.file invalido. Use restore.file:snapshot|relPath|outRoot(opcional)");
    const std::optional<fs::path> outRoot=!outRootRaw.empty()?std::optional<fs::path>(fs::path(outRootRaw)):std::nullopt;
    api_->restoreFile(snapshot,relPath,outRoot);
    sendJson_(std::string("{\"type\":\"restore.file.finished\",\"snapshot\":\"")+escapeJson(snapshot)+"\",\"path\":\""+escapeJson(relPath)+"\"}");}
void KeeplyAgentWsClient::runRestoreSnapshotCommand_(const std::string& snapshot,const std::string& outRootRaw){
    if(snapshot.empty()) throw std::runtime_error("Formato restore.snapshot invalido. Use restore.snapshot:snapshot|outRoot(opcional)");
    const std::optional<fs::path> outRoot=!outRootRaw.empty()?std::optional<fs::path>(fs::path(outRootRaw)):std::nullopt;
    api_->restoreSnapshot(snapshot,outRoot);
    sendJson_(std::string("{\"type\":\"restore.snapshot.finished\",\"snapshot\":\"")+escapeJson(snapshot)+"\"}");}
void KeeplyAgentWsClient::runRestoreCloudSnapshotCommand_(const WsCommand& cmd){
    if(cmd.snapshot.empty()) throw std::runtime_error("restore.cloud.snapshot requer snapshot.");
    if(cmd.downloadPathBase.empty()) throw std::runtime_error("restore.cloud.snapshot requer downloadPathBase.");
    if(cmd.archiveFile.empty()) throw std::runtime_error("restore.cloud.snapshot requer archiveFile.");
    const std::vector<std::string> blobFiles = splitNonEmptyPipe(cmd.blobFiles);
    if(blobFiles.empty()) throw std::runtime_error("restore.cloud.snapshot requer ao menos um blob do pack.");
    const std::string requestId = cmd.requestId.empty() ? randomDigits(12) : cmd.requestId;
    const std::string packFileName = !cmd.packFile.empty() ? cmd.packFile : blobFiles.front();
    const std::size_t filesTotal = 1 + (cmd.indexFile.empty() ? 0 : 1) + blobFiles.size();
    const std::string resolvedOutRoot = !cmd.outRoot.empty() ? cmd.outRoot : api_->state().restoreRoot;
    std::ostringstream startedJson;
    startedJson<<"{"<<"\"type\":\"restore.cloud.started\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("backupId",cmd.backupId)<<","<<jsonStrField("backupRef",cmd.backupRef)<<","<<jsonStrField("bundleId",cmd.bundleId)<<","<<jsonStrField("snapshot",cmd.snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<"\"filesTotal\":"<<filesTotal<<"}";
    sendJson_(startedJson.str());
    const fs::path tempRoot = defaultKeeplyTempDir() / "cloud_restore" / requestId;
    std::error_code cleanupEc;
    fs::remove_all(tempRoot, cleanupEc);
    ensureDirectory(tempRoot);
    try{
        std::size_t completedDownloads = 0;
        auto sendProgress = [&](const std::string& phase,const std::string& currentFile){
            std::ostringstream progressJson;
            progressJson<<"{"<<"\"type\":\"restore.cloud.progress\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("backupId",cmd.backupId)<<","<<jsonStrField("bundleId",cmd.bundleId)<<","<<jsonStrField("snapshot",cmd.snapshot)<<","<<jsonStrField("phase",phase)<<","<<jsonStrField("currentFile",currentFile)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<"\"filesCompleted\":"<<completedDownloads<<","<<"\"filesTotal\":"<<filesTotal<<"}";
            sendJson_(progressJson.str());
        };
        auto downloadOne = [&](const std::string& fileName){
            sendProgress("downloading", fileName);
            const std::string path = buildBundleDownloadPath(cmd.downloadPathBase, fileName);
            const std::string url = httpUrlFromWsUrl(config_.url, path);
            const HttpResponse response = httpGet(url, std::nullopt, std::nullopt, config_.allowInsecureTls);
            if(response.status < 200 || response.status >= 300){
                throw std::runtime_error(
                    "Falha baixando arquivo do bundle: HTTP " + std::to_string(response.status) + " | " + fileName
                );}
            const fs::path outPath = tempRoot / fileName;
            writeBinaryFile(outPath, response.body);
            ++completedDownloads;
            sendProgress("downloaded", fileName);
            return outPath;
        };
        const fs::path archivePath = downloadOne(cmd.archiveFile);
        if(!cmd.indexFile.empty()) downloadOne(cmd.indexFile);
        std::vector<fs::path> blobPaths;
        blobPaths.reserve(blobFiles.size());
        for(const auto& blobFile : blobFiles){
            blobPaths.push_back(downloadOne(blobFile));}
        const fs::path finalPackPath = tempRoot / packFileName;
        if(blobPaths.size() == 1 && blobPaths.front().filename() == finalPackPath.filename()){
        }else{
            sendProgress("assembling", packFileName);
            if(!finalPackPath.parent_path().empty()) ensureDirectory(finalPackPath.parent_path());
            std::ofstream packOut(finalPackPath, std::ios::binary | std::ios::trunc);
            if(!packOut) throw std::runtime_error("Falha criando pack temporario para restore cloud.");
            for(const auto& partPath : blobPaths){
                appendFileToStream(packOut, partPath);}
            packOut.close();
            if(!packOut) throw std::runtime_error("Falha finalizando pack temporario para restore cloud.");}
        sendProgress("restoring", cmd.snapshot);
        const std::optional<fs::path> outRoot = resolvedOutRoot.empty()
            ? std::nullopt
            : std::optional<fs::path>(fs::path(resolvedOutRoot));
        StorageArchive archive(archivePath);
        const sqlite3_int64 snapshotId = archive.resolveSnapshotId(cmd.snapshot);
        const fs::path finalOutRoot = outRoot.value_or(pathFromUtf8(api_->state().restoreRoot));
        const std::string normalizedRelPath = normalizeRelPath(cmd.relPath);
        const std::string normalizedEntryType = trim(cmd.entryType);
        if(normalizedRelPath.empty()){
            RestoreEngine::restoreSnapshot(archivePath, snapshotId, finalOutRoot);
        }else if(normalizedEntryType == "folder"){
            const auto paths = archive.listSnapshotPaths(snapshotId);
            std::size_t restoredCount = 0;
            for(const auto& path : paths){
                if(!hasSnapshotPathPrefix(path, normalizedRelPath)) continue;
                RestoreEngine::restoreFile(archivePath, snapshotId, path, finalOutRoot);
                ++restoredCount;}
            if(restoredCount == 0){
                throw std::runtime_error("Nenhum arquivo encontrado para a pasta selecionada: " + normalizedRelPath);}
        }else{
            RestoreEngine::restoreFile(archivePath, snapshotId, normalizedRelPath, finalOutRoot);}
        std::ostringstream finishedJson;
        finishedJson<<"{"<<"\"type\":\"restore.cloud.finished\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("backupId",cmd.backupId)<<","<<jsonStrField("backupRef",cmd.backupRef)<<","<<jsonStrField("bundleId",cmd.bundleId)<<","<<jsonStrField("snapshot",cmd.snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<jsonStrField("path",normalizedRelPath)<<","<<jsonStrField("entryType",normalizedEntryType)<<"}";
        sendJson_(finishedJson.str());
        sendJson_(std::string("{\"type\":\"restore.snapshot.finished\",\"snapshot\":\"")+escapeJson(cmd.snapshot)+"\",\"requestId\":\""+escapeJson(requestId)+"\",\"outRoot\":\""+escapeJson(resolvedOutRoot)+"\",\"path\":\""+escapeJson(normalizedRelPath)+"\",\"entryType\":\""+escapeJson(normalizedEntryType)+"\"}");
    }catch(const std::exception& e){
        std::ostringstream failedJson;
        failedJson<<"{"<<"\"type\":\"restore.cloud.failed\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("backupId",cmd.backupId)<<","<<jsonStrField("backupRef",cmd.backupRef)<<","<<jsonStrField("bundleId",cmd.bundleId)<<","<<jsonStrField("snapshot",cmd.snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<jsonStrField("path",cmd.relPath)<<","<<jsonStrField("entryType",cmd.entryType)<<","<<jsonStrField("message",e.what())<<"}";
        sendJson_(failedJson.str());
        fs::remove_all(tempRoot, cleanupEc);
        throw;}
    fs::remove_all(tempRoot, cleanupEc);}
}

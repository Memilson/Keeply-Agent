#include "websocket_agente.hpp"
#include "websocket_interno.hpp"
#include "../HTTP/http_util.hpp"
#include "../Storage/backend_armazenamento.hpp"
#include "../Core/tipos.hpp"
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
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
fs::path maybeInflateArchiveForRestore(const fs::path& path){
    const std::string fileName = path.filename().string();
    if(fileName.size()<4||fileName.substr(fileName.size()-4)!=".zst") return path;
    std::ifstream in(path,std::ios::binary);
    if(!in) throw std::runtime_error("Falha abrindo archive compactado: " + path.string());
    in.seekg(0,std::ios::end);
    const std::streamoff endPos=in.tellg();
    if(endPos<static_cast<std::streamoff>(sizeof(std::uint64_t))) throw std::runtime_error("Archive compactado invalido.");
    in.seekg(0,std::ios::beg);
    std::vector<unsigned char> payload(static_cast<std::size_t>(endPos));
    in.read(reinterpret_cast<char*>(payload.data()),static_cast<std::streamsize>(payload.size()));
    if(!in) throw std::runtime_error("Falha lendo archive compactado.");
    std::uint64_t rawSize=0;
    std::memcpy(&rawSize,payload.data(),sizeof(rawSize));
    std::vector<unsigned char> raw;
    keeply::Compactador::zstdDecompress(payload.data()+sizeof(rawSize),payload.size()-sizeof(rawSize),static_cast<std::size_t>(rawSize),raw);
    fs::path outPath=path;
    outPath.replace_extension({});
    std::ofstream out(outPath,std::ios::binary|std::ios::trunc);
    if(!out) throw std::runtime_error("Falha criando archive descompactado: " + outPath.string());
    if(!raw.empty()) out.write(reinterpret_cast<const char*>(raw.data()),static_cast<std::streamsize>(raw.size()));
    if(!out) throw std::runtime_error("Falha gravando archive descompactado: " + outPath.string());
    return outPath;}
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
void KeeplyAgentWsClient::runRestoreFileCommand_(const std::string& requestId,const std::string& snapshot,const std::string& relPath,const std::string& outRootRaw){
    if(snapshot.empty()||relPath.empty()) throw std::runtime_error("Formato restore.file invalido. Use restore.file:snapshot|relPath|outRoot(opcional)");
    const std::string resolvedOutRoot=!outRootRaw.empty()?outRootRaw:api_->state().restoreRoot;
    std::ostringstream startedJson;
    startedJson<<"{"<<"\"type\":\"restore.file.started\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("snapshot",snapshot)<<","<<jsonStrField("path",relPath)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<"}";
    sendJson_(startedJson.str());
    try{
        const std::optional<fs::path> outRoot=!outRootRaw.empty()?std::optional<fs::path>(fs::path(outRootRaw)):std::nullopt;
        api_->restoreFile(snapshot,relPath,outRoot);
        std::ostringstream finishedJson;
        finishedJson<<"{"<<"\"type\":\"restore.file.finished\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("snapshot",snapshot)<<","<<jsonStrField("path",relPath)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<"}";
        sendJson_(finishedJson.str());
    }catch(const std::exception& e){
        std::ostringstream failedJson;
        failedJson<<"{"<<"\"type\":\"restore.file.failed\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("snapshot",snapshot)<<","<<jsonStrField("path",relPath)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<jsonStrField("message",e.what())<<"}";
        sendJson_(failedJson.str());
        throw;}}
void KeeplyAgentWsClient::runRestoreSnapshotCommand_(const std::string& requestId,const std::string& snapshot,const std::string& outRootRaw){
    if(snapshot.empty()) throw std::runtime_error("Formato restore.snapshot invalido. Use restore.snapshot:snapshot|outRoot(opcional)");
    const std::string resolvedOutRoot=!outRootRaw.empty()?outRootRaw:api_->state().restoreRoot;
    std::ostringstream startedJson;
    startedJson<<"{"<<"\"type\":\"restore.snapshot.started\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("snapshot",snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<"}";
    sendJson_(startedJson.str());
    try{
        const std::optional<fs::path> outRoot=!outRootRaw.empty()?std::optional<fs::path>(fs::path(outRootRaw)):std::nullopt;
        api_->restoreSnapshot(snapshot,outRoot);
        std::ostringstream finishedJson;
        finishedJson<<"{"<<"\"type\":\"restore.snapshot.finished\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("snapshot",snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<"}";
        sendJson_(finishedJson.str());
    }catch(const std::exception& e){
        std::ostringstream failedJson;
        failedJson<<"{"<<"\"type\":\"restore.snapshot.failed\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("snapshot",snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<jsonStrField("message",e.what())<<"}";
        sendJson_(failedJson.str());
        throw;}}
void KeeplyAgentWsClient::runRestoreCloudSnapshotCommand_(const WsCommand& cmd){
    if(cmd.snapshot.empty()) throw std::runtime_error("restore.cloud.snapshot requer snapshot.");
    if(cmd.downloadPathBase.empty()) throw std::runtime_error("restore.cloud.snapshot requer downloadPathBase.");
    if(cmd.archiveFile.empty()) throw std::runtime_error("restore.cloud.snapshot requer archiveFile.");
    const std::string requestId = cmd.requestId.empty() ? randomDigits(12) : cmd.requestId;
    const std::string resolvedOutRoot = !cmd.outRoot.empty() ? cmd.outRoot : api_->state().restoreRoot;
    const fs::path tempRoot = defaultKeeplyTempDir() / "cloud_restore" / requestId;
    std::error_code cleanupEc;
    fs::remove_all(tempRoot, cleanupEc);
    ensureDirectory(tempRoot);
    try{
        std::size_t completedDownloads = 0;
        std::size_t filesTotal = 1;
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
        const fs::path downloadedArchivePath = downloadOne(cmd.archiveFile);
        const fs::path archivePath = maybeInflateArchiveForRestore(downloadedArchivePath);
        StorageArchive archive(archivePath);
        const sqlite3_int64 snapshotId = archive.resolveSnapshotId(cmd.snapshot);
        const auto plan = archive.prepareCloudUpload(identity_.deviceId);
        const std::vector<ChunkHash> snapshotHashes = archive.snapshotChunkHashes(snapshotId);
        std::set<std::string> neededHashes;
        neededHashes.clear();
        for(const auto& hash : snapshotHashes) neededHashes.insert(hexEncode(hash.data(), hash.size()));
        std::vector<StorageArchive::CloudChunkRef> neededChunks;
        neededChunks.reserve(neededHashes.size());
        for(const auto& chunk : plan.chunks){
            const std::string hashHex = hexEncode(chunk.hash.data(), chunk.hash.size());
            if(neededHashes.find(hashHex) != neededHashes.end()) neededChunks.push_back(chunk);}
        if(neededChunks.empty()) throw std::runtime_error("Nenhum chunk encontrado para o snapshot solicitado.");
        filesTotal = 1 + neededChunks.size();
        std::ostringstream startedJson;
        startedJson<<"{"<<"\"type\":\"restore.cloud.started\","<<jsonStrField("requestId",requestId)<<","<<jsonStrField("backupId",cmd.backupId)<<","<<jsonStrField("backupRef",cmd.backupRef)<<","<<jsonStrField("bundleId",cmd.bundleId)<<","<<jsonStrField("snapshot",cmd.snapshot)<<","<<jsonStrField("outRoot",resolvedOutRoot)<<","<<"\"filesTotal\":"<<filesTotal<<"}";
        sendJson_(startedJson.str());
        const fs::path finalPackPath = describeArchiveStorage(archivePath).packPath;
        if(!finalPackPath.parent_path().empty()) ensureDirectory(finalPackPath.parent_path());
        sendProgress("assembling", finalPackPath.filename().string());
        std::ofstream packOut(finalPackPath, std::ios::binary | std::ios::trunc);
        if(!packOut) throw std::runtime_error("Falha criando pack temporario para restore cloud.");
        std::uint64_t currentOffset = 0;
        for(const auto& chunk : neededChunks){
            const std::string hashHex = hexEncode(chunk.hash.data(), chunk.hash.size());
            const std::string chunkFileName = hashHex + ".bin";
            const fs::path chunkPath = downloadOne(chunkFileName);
            std::ifstream in(chunkPath, std::ios::binary);
            if(!in) throw std::runtime_error("Falha abrindo chunk baixado: " + chunkPath.string());
            packOut.seekp(static_cast<std::streamoff>(currentOffset), std::ios::beg);
            if(!packOut) throw std::runtime_error("Falha posicionando pack temporario.");
            appendFileToStream(packOut, chunkPath);
            archive.setChunkPackOffsetForRestore(chunk.hash, currentOffset);
            currentOffset += chunk.recordSize;
        }
        packOut.close();
        if(!packOut) throw std::runtime_error("Falha finalizando pack temporario para restore cloud.");
        sendProgress("restoring", cmd.snapshot);
        const std::optional<fs::path> outRoot = resolvedOutRoot.empty()
            ? std::nullopt
            : std::optional<fs::path>(fs::path(resolvedOutRoot));
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

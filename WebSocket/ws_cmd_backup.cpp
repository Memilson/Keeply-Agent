#include "websocket_agente.hpp"
#include "websocket_interno.hpp"
#include "../HTTP/http_util.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <chrono>
namespace {
namespace fs = std::filesystem;
using namespace keeply::ws_internal;
std::string backupPhaseLabelForLog(const std::string& phase){
    if(phase=="discovery") return "scan";
    if(phase=="backup") return "compactacao";
    if(phase=="commit") return "finalizacao";
    if(phase=="done") return "concluido";
    if(phase=="uploading") return "uploading";
    return phase.empty()?"idle":phase;}
std::string storageModeToString(BackupStorageMode mode){
    switch(mode){
        case BackupStorageMode::CloudOnly: return "cloud_only";
        default: return "cloud_only";}}}
namespace keeply {
void KeeplyAgentWsClient::sendBackupProgress_(const std::string& label,const BackupProgress& progress){
    std::ostringstream progressJson;
    progressJson<<"{"<<"\"type\":\"backup.progress\","<<"\"label\":\""<<escapeJson(label)<<"\","<<"\"source\":\""<<escapeJson(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"phase\":\""<<escapeJson(progress.phase)<<"\","<<"\"discoveryComplete\":"<<(progress.discoveryComplete?"true":"false")<<","<<"\"currentFile\":\""<<escapeJson(progress.currentFile)<<"\","<<"\"filesQueued\":"<<progress.filesQueued<<","<<"\"filesCompleted\":"<<progress.filesCompleted<<","<<"\"filesScanned\":"<<progress.stats.scanned<<","<<"\"filesAdded\":"<<progress.stats.added<<","<<"\"filesUnchanged\":"<<progress.stats.reused<<","<<"\"chunksNew\":"<<progress.stats.uniqueChunksInserted<<","<<"\"bytesRead\":"<<progress.stats.bytesRead<<","<<"\"warnings\":"<<progress.stats.warnings<<"}";
    sendJson_(progressJson.str());}
void KeeplyAgentWsClient::sendBackupFinished_(const std::string& label,const BackupStats& stats){
    const std::size_t chunksReused=stats.chunks>=stats.uniqueChunksInserted?stats.chunks-stats.uniqueChunksInserted:0;
    std::ostringstream oss;
    oss<<"{"<<"\"type\":\"backup.finished\","<<"\"label\":\""<<escapeJson(label)<<"\","<<"\"source\":\""<<escapeJson(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"filesScanned\":"<<stats.scanned<<","<<"\"filesAdded\":"<<stats.added<<","<<"\"filesUnchanged\":"<<stats.reused<<","<<"\"chunksNew\":"<<stats.uniqueChunksInserted<<","<<"\"chunksReused\":"<<chunksReused<<","<<"\"bytesRead\":"<<stats.bytesRead<<","<<"\"warnings\":"<<stats.warnings<<"}";
    sendJson_(oss.str());}
void KeeplyAgentWsClient::sendBackupFailed_(const std::string& label,const BackupProgress& latestProgress,const std::string& message){
    std::ostringstream failedJson;
    failedJson<<"{"<<"\"type\":\"backup.failed\","<<"\"label\":\""<<escapeJson(label)<<"\","<<"\"source\":\""<<escapeJson(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"phase\":\"failed\","<<"\"discoveryComplete\":"<<(latestProgress.discoveryComplete?"true":"false")<<","<<"\"currentFile\":\""<<escapeJson(latestProgress.currentFile)<<"\","<<"\"filesQueued\":"<<latestProgress.filesQueued<<","<<"\"filesCompleted\":"<<latestProgress.filesCompleted<<","<<"\"filesScanned\":"<<latestProgress.stats.scanned<<","<<"\"filesAdded\":"<<latestProgress.stats.added<<","<<"\"filesUnchanged\":"<<latestProgress.stats.reused<<","<<"\"chunksNew\":"<<latestProgress.stats.uniqueChunksInserted<<","<<"\"bytesRead\":"<<latestProgress.stats.bytesRead<<","<<"\"warnings\":"<<latestProgress.stats.warnings<<","<<"\"message\":\""<<escapeJson(message)<<"\""<<"}";
    sendJson_(failedJson.str());}
void KeeplyAgentWsClient::runBackupUpload_(const std::string& label,const BackupStoragePolicy& storagePolicy){
    try{
        const std::string storageMode=storageModeToString(storagePolicy.mode);
        std::cout<<"[backup][fase] uploading"<<" | destino="<<storageMode<<"\n";
        sendJson_(std::string("{\"type\":\"backup.upload.started\",\"label\":\"")+escapeJson(label)+"\",\"source\":\""+escapeJson(api_->state().source)+"\",\"scanScope\":"+buildScanScopeJson_()+",\"storageMode\":\""+escapeJson(storageMode)+"\"}");
        const UploadBundleResult uploadResp=uploadArchiveBackup(
            config_,
            identity_,
            api_->state(),
            label,
            [this,label,&storageMode](const UploadProgressSnapshot& progress){
                std::ostringstream uploadProgressJson;
                uploadProgressJson<<"{"<<"\"type\":\"backup.upload.progress\","<<"\"label\":\""<<escapeJson(label)<<"\","<<"\"source\":\""<<escapeJson(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson(storageMode)<<"\","<<"\"phase\":\"uploading\","<<"\"currentObject\":\""<<escapeJson(progress.currentObject)<<"\","<<"\"bundleRole\":\""<<escapeJson(progress.bundleRole)<<"\","<<"\"filesUploaded\":"<<progress.filesUploaded<<","<<"\"filesTotal\":"<<progress.filesTotal<<","<<"\"blobPartIndex\":"<<progress.blobPartIndex<<","<<"\"blobPartCount\":"<<progress.blobPartCount<<"}";
                sendJson_(uploadProgressJson.str());}
        );
        const std::string storageId=trim(extractJsonStringField(uploadResp.manifestResponse.body,"storageId"));
        const std::string storageScope=trim(extractJsonStringField(uploadResp.manifestResponse.body,"storageScope"));
        const std::string uri=trim(extractJsonStringField(uploadResp.manifestResponse.body,"uri"));
        const std::string bucket=trim(extractJsonStringField(uploadResp.manifestResponse.body,"bucket"));
        if(storagePolicy.deleteLocalAfterUpload) deleteLocalArchiveArtifacts(keeply::fs::path(api_->state().archive));
        std::ostringstream uploadedJson;
        uploadedJson<<"{"<<"\"type\":\"backup.uploaded\","<<"\"label\":\""<<escapeJson(label)<<"\","<<"\"source\":\""<<escapeJson(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson(storageMode)<<"\","<<"\"storageId\":\""<<escapeJson(storageId)<<"\","<<"\"storageScope\":\""<<escapeJson(storageScope)<<"\","<<"\"bucket\":\""<<escapeJson(bucket)<<"\","<<"\"uri\":\""<<escapeJson(uri)<<"\","<<"\"bundleId\":\""<<escapeJson(uploadResp.bundleId)<<"\","<<"\"filesUploaded\":"<<uploadResp.filesUploaded<<","<<"\"blobPartCount\":"<<uploadResp.blobPartCount<<"}";
        sendJson_(uploadedJson.str());
    }catch(const std::exception& uploadEx){
        const std::string storageMode=storageModeToString(storagePolicy.mode);
        std::ostringstream uploadFailedJson;
        uploadFailedJson<<"{"<<"\"type\":\"backup.upload.failed\","<<"\"label\":\""<<escapeJson(label)<<"\","<<"\"source\":\""<<escapeJson(api_->state().source)<<"\","<<"\"scanScope\":"<<buildScanScopeJson_()<<","<<"\"storageMode\":\""<<escapeJson(storageMode)<<"\","<<"\"message\":\""<<escapeJson(uploadEx.what())<<"\""<<"}";
        sendJson_(uploadFailedJson.str());}}
void KeeplyAgentWsClient::runBackupCommand_(const std::string& label,const std::string& storage){
    BackupStoragePolicy storagePolicy;
    BackupProgress latestProgress;
    if(!storage.empty()) storagePolicy=ws_internal::parseBackupStoragePolicy(storage);
    std::cout<<"[backup] iniciado";
    if(!label.empty()) std::cout<<" | label="<<label;
    std::cout<<" | source="<<api_->state().source<<"\n";
    sendJson_(std::string("{\"type\":\"backup.started\",\"label\":\"")+escapeJson(label)+"\",\"source\":\""+escapeJson(api_->state().source)+"\",\"scanScope\":"+buildScanScopeJson_()+"}");
    try{
        if (storagePolicy.uploadCloud && !keeply::fs::exists(keeply::pathFromUtf8(api_->state().archive))) {
            try {
                std::string urlStr = ws_internal::httpUrlFromWsUrl(config_.url, "/api/agent/backups/latest-kply");
                urlStr += "?userId=" + ws_internal::urlEncode(identity_.userId);
                urlStr += "&agentId=" + ws_internal::urlEncode(identity_.deviceId);
                urlStr += "&folderName=" + ws_internal::urlEncode(label);
                urlStr += "&sourcePath=" + ws_internal::urlEncode(api_->state().source);
                const ws_internal::HttpResponse latestResp = ws_internal::httpGet(urlStr, std::nullopt, std::nullopt, config_.allowInsecureTls);
                if (latestResp.status >= 200 && latestResp.status < 300) {
                    const std::string preSignedUrl = ws_internal::extractJsonStringField(latestResp.body, "url");
                    if (!preSignedUrl.empty()) {
                        std::cout << "[backup] Baixando base incremental da nuvem..." << std::endl;
                        const ws_internal::HttpResponse s3Resp = ws_internal::httpGet(preSignedUrl, std::nullopt, std::nullopt, true);
                        if (s3Resp.status >= 200 && s3Resp.status < 300) {
                            std::ofstream out(keeply::pathFromUtf8(api_->state().archive), std::ios::binary);
                            out.write(s3Resp.body.data(), s3Resp.body.size());
                            std::cout << "[backup] Base incremental baixada com sucesso (" << s3Resp.body.size() << " bytes)." << std::endl;}}}
            } catch (const std::exception& e) {
                std::cerr << "[backup] Falha ignorada ao baixar base incremental: " << e.what() << "\n";}}
        auto lastProgressSent=std::chrono::steady_clock::now()-std::chrono::seconds(10);
        std::string lastPhase;
        const BackupStats stats=api_->runBackup(label,[this,label,&latestProgress,&lastProgressSent,&lastPhase](const BackupProgress& progress){
            latestProgress=progress;
            const auto now=std::chrono::steady_clock::now();
            const bool phaseChanged=progress.phase!=lastPhase;
            if(phaseChanged){
                std::cout<<"[backup][fase] "<<backupPhaseLabelForLog(progress.phase);
                if(!progress.currentFile.empty()) std::cout<<" | arquivo="<<progress.currentFile;
                std::cout<<"\n";
            }
            const bool forceSend=phaseChanged||progress.phase=="discovery"||progress.phase=="commit"||progress.phase=="done";
            if(forceSend||now-lastProgressSent>=std::chrono::seconds(2)){
                sendBackupProgress_(label,progress);
                lastProgressSent=now;}
            lastPhase=progress.phase;
        });
        sendBackupFinished_(label,stats);
        runBackupUpload_(label,storagePolicy);
        std::cout<<"[backup] concluido"<<" | scanned="<<stats.scanned<<" added="<<stats.added<<" unchanged="<<stats.reused<<" bytes="<<stats.bytesRead<<" warnings="<<stats.warnings<<"\n";
        std::cout<<"[agent] backup finalizado. Agente segue online aguardando comandos.\n";
    }catch(const std::exception& e){
        sendBackupFailed_(label,latestProgress,e.what());
        std::cout<<"[backup] falhou";
        if(!label.empty()) std::cout<<" | label="<<label;
        std::cout<<" | error="<<e.what()<<"\n";
        std::cout<<"[agent] agente segue online aguardando comandos.\n";}}
}

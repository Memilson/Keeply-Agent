#pragma once

#include "websocket_agente.hpp"
#include "../http_interno.hpp"

#include <cstddef>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

struct ssl_st;

namespace keeply::ws_internal {

enum class BackupStorageMode{
    LocalOnly,
    CloudOnly,
    Hybrid
};

using ParsedUrl = http_internal::ParsedUrl;
using HttpResponse = http_internal::HttpResponse;
using MultipartField = http_internal::MultipartField;

struct UploadBundleResult{
    HttpResponse manifestResponse;
    std::size_t filesUploaded=0;
    std::size_t blobPartCount=0;
    std::string bundleId;
};

struct UploadProgressSnapshot{
    std::size_t filesUploaded=0;
    std::size_t filesTotal=0;
    std::size_t blobPartIndex=0;
    std::size_t blobPartCount=0;
    std::string currentObject;
    std::string bundleRole;
};

struct BackupStoragePolicy{
    BackupStorageMode mode=BackupStorageMode::LocalOnly;
    bool keepLocal=true;
    bool uploadCloud=false;
    bool deleteLocalAfterUpload=false;
};

[[nodiscard]] inline std::string toLower(std::string value){ return http_internal::toLower(std::move(value)); }
[[nodiscard]] std::string hexEncode(const unsigned char* data,std::size_t size);
[[nodiscard]] std::string base64Encode(const unsigned char* data,std::size_t size);
[[nodiscard]] std::string randomBase64(std::size_t bytes);
[[nodiscard]] std::string randomDigits(std::size_t digits);

[[nodiscard]] inline ParsedUrl parseUrlCommon(const std::string& url){ return http_internal::parseUrlCommon(url); }
[[nodiscard]] inline std::string urlEncode(const std::string& value){ return http_internal::urlEncode(value); }
[[nodiscard]] inline std::string escapeJson(const std::string& value){ return http_internal::escapeJson(value); }
[[nodiscard]] std::string httpUrlFromWsUrl(const std::string& wsUrl,const std::string& path);

[[nodiscard]] inline int openTcpSocket(const std::string& host,int port){ return http_internal::openTcpSocket(host, port); }
[[nodiscard]] inline std::size_t writeAllFd(int fd,const void* data,std::size_t size){ return http_internal::writeAllFd(fd, data, size); }
[[nodiscard]] std::size_t writeAllSsl(ssl_st* sslHandle,const void* data,std::size_t size);
[[nodiscard]] inline std::size_t readSomeFd(int fd,void* data,std::size_t size){ return http_internal::readSomeFd(fd, data, size); }
[[nodiscard]] std::size_t readSomeSsl(ssl_st* sslHandle,void* data,std::size_t size);

[[nodiscard]] HttpResponse httpPostJson(const std::string& url,
                                        const std::string& body,
                                        const std::optional<fs::path>& certPemPath,
                                        const std::optional<fs::path>& keyPemPath,
                                        bool allowInsecureTls);

[[nodiscard]] std::map<std::string,std::string> loadIdentityMeta(const fs::path& metaPath);
void saveIdentityMeta(const AgentIdentity& identity);

[[nodiscard]] std::string extractJsonStringField(const std::string& json,const std::string& field);
[[nodiscard]] std::vector<std::string> splitPipe(const std::string& value);
[[nodiscard]] BackupStoragePolicy parseBackupStoragePolicy(const std::string& raw);

void deleteLocalArchiveArtifacts(const fs::path& archivePath);

[[nodiscard]] UploadBundleResult uploadArchiveBackup(const WsClientConfig& config,
                                                     const AgentIdentity& identity,
                                                     const AppState& state,
                                                     const std::string& label,
                                                     const std::function<void(const UploadProgressSnapshot&)>& onProgress={});

} // namespace keeply::ws_internal

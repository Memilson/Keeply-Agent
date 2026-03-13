#pragma once

#include "ws.hpp"

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

struct ParsedUrl{
    std::string scheme;
    std::string host;
    int port=0;
    std::string target="/";
};

struct HttpResponse{
    int status=0;
    std::string body;
    std::map<std::string,std::string> headers;
};

struct MultipartField{
    std::string name;
    std::string value;
};

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

[[nodiscard]] std::string toLower(std::string value);
[[nodiscard]] std::string hexEncode(const unsigned char* data,std::size_t size);
[[nodiscard]] std::string base64Encode(const unsigned char* data,std::size_t size);
[[nodiscard]] std::string randomBase64(std::size_t bytes);
[[nodiscard]] std::string randomDigits(std::size_t digits);

[[nodiscard]] ParsedUrl parseUrlCommon(const std::string& url);
[[nodiscard]] std::string urlEncode(const std::string& value);
[[nodiscard]] std::string escapeJson(const std::string& value);
[[nodiscard]] std::string httpUrlFromWsUrl(const std::string& wsUrl,const std::string& path);

[[nodiscard]] int openTcpSocket(const std::string& host,int port);
[[nodiscard]] std::size_t writeAllFd(int fd,const void* data,std::size_t size);
[[nodiscard]] std::size_t writeAllSsl(ssl_st* sslHandle,const void* data,std::size_t size);
[[nodiscard]] std::size_t readSomeFd(int fd,void* data,std::size_t size);
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
#pragma once

#include "ws.hpp"

#include <array>
#include <cstddef>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace keeply::ws_internal {

struct ParsedUrl {
    std::string scheme;
    std::string host;
    int port = 0;
    std::string target = "/";
};

struct HttpResponse {
    int status = 0;
    std::string body;
};

struct MultipartField {
    std::string name;
    std::string value;
};

struct UploadBundleResult {
    HttpResponse manifestResponse;
    std::size_t filesUploaded = 0;
    std::size_t blobPartCount = 0;
    std::string bundleId;
};

struct UploadProgressSnapshot {
    std::size_t filesUploaded = 0;
    std::size_t filesTotal = 0;
    std::size_t blobPartIndex = 0;
    std::size_t blobPartCount = 0;
    std::string currentObject;
    std::string bundleRole;
};

struct BackupStoragePolicy {
    std::string mode = "local_only";
    bool keepLocal = true;
    bool uploadCloud = false;
    bool deleteLocalAfterUpload = false;
};

std::string toLower(std::string value);
std::string hexEncode(const unsigned char* data, std::size_t size);
std::string base64Encode(const unsigned char* data, std::size_t size);
std::string randomBase64(std::size_t bytes);
std::string randomDigits(std::size_t digits);
ParsedUrl parseUrlCommon(const std::string& url);
std::string urlEncode(const std::string& value);
std::string escapeJson(const std::string& value);
int openTcpSocket(const std::string& host, int port);
std::size_t writeAllFd(int fd, const void* data, std::size_t size);
std::size_t writeAllSsl(void* sslHandle, const void* data, std::size_t size);
std::size_t readSomeFd(int fd, void* data, std::size_t size);
std::size_t readSomeSsl(void* sslHandle, void* data, std::size_t size);
HttpResponse httpPostJson(const std::string& url,
                          const std::string& body,
                          const std::optional<fs::path>& certPemPath,
                          const std::optional<fs::path>& keyPemPath,
                          bool allowInsecureTls);
std::map<std::string, std::string> loadIdentityMeta(const fs::path& metaPath);
void saveIdentityMeta(const AgentIdentity& identity);
std::string extractJsonStringField(const std::string& json, const std::string& field);
std::vector<std::string> splitPipe(const std::string& value);
BackupStoragePolicy parseBackupStoragePolicy(const std::string& raw);
void deleteLocalArchiveArtifacts(const fs::path& archivePath);
std::string httpUrlFromWsUrl(const std::string& wsUrl, const std::string& path);
UploadBundleResult uploadArchiveBackup(const WsClientConfig& config,
                                       const AgentIdentity& identity,
                                       const AppState& state,
                                       const std::string& label,
                                       const std::function<void(const UploadProgressSnapshot&)>& onProgress = {});

} // namespace keeply::ws_internal

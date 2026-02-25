#pragma once

#include "keeply.hpp"
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace keeply {

// Request/Response genéricos para desacoplar da lib HTTP
struct RestRequest {
    std::string method; // GET, POST...
    std::string path;   // /api/v1/snapshots
    std::unordered_map<std::string, std::string> query;
    std::unordered_map<std::string, std::string> headers;
    std::string body;   // JSON raw (por enquanto)
};

struct RestResponse {
    int status = 200;
    std::string contentType = "application/json; charset=utf-8";
    std::string body;
};

class IWsNotifier {
public:
    virtual ~IWsNotifier() = default;
    virtual void broadcastJson(const std::string& json) = 0;
    virtual void publishJobEvent(const std::string& jobId, const std::string& json) = 0;
};

// Adaptador REST: traduz HTTP <-> KeeplyApi
class KeeplyRestApi {
public:
    KeeplyRestApi(std::shared_ptr<KeeplyApi> api,
                  std::shared_ptr<IWsNotifier> wsNotifier = nullptr);

    RestResponse handle(const RestRequest& req);

private:
    std::shared_ptr<KeeplyApi> api_;
    std::shared_ptr<IWsNotifier> ws_;
    mutable std::mutex mu_; // proteção inicial simples (state_ / chamadas concorrentes)

    // helpers
    RestResponse handleGetHealth();
    RestResponse handleGetState();
    RestResponse handlePostConfigSource(const RestRequest& req);
    RestResponse handlePostConfigArchive(const RestRequest& req);
    RestResponse handlePostConfigRestoreRoot(const RestRequest& req);

    RestResponse handlePostBackup(const RestRequest& req);
    RestResponse handleGetSnapshots();
    RestResponse handleGetDiff(const RestRequest& req);
    RestResponse handleGetSnapshotPaths(const RestRequest& req);

    RestResponse handlePostRestoreFile(const RestRequest& req);
    RestResponse handlePostRestoreSnapshot(const RestRequest& req);

    // utilitários
    static RestResponse jsonOk(const std::string& json);
    static RestResponse jsonCreated(const std::string& json);
    static RestResponse jsonBadRequest(const std::string& message);
    static RestResponse jsonNotFound(const std::string& message);
    static RestResponse jsonError(const std::string& message);

    static std::string escapeJson(const std::string& s);
    static std::string getPathParamAfterPrefix(const std::string& fullPath, const std::string& prefix);
};

} // namespace keeply                                                                                                                                                   
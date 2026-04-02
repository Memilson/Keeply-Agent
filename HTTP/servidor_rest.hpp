#pragma once
#include "../keeply.hpp"
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <string>
#include <unordered_map>
namespace keeply {
struct RestRequest{
    std::string method;
    std::string path;
    std::unordered_map<std::string,std::string> query;
    std::unordered_map<std::string,std::string> headers;
    std::string body;
};
struct RestResponse{
    int status=200;
    std::string contentType="application/json; charset=utf-8";
    std::string body;
};
class IWsNotifier{
public:
    virtual ~IWsNotifier()=default;
    virtual void broadcastJson(const std::string& json)=0;
    virtual void publishJobEvent(const std::string& jobId,const std::string& json)=0;
};
class KeeplyRestApi{
public:
    KeeplyRestApi(std::shared_ptr<KeeplyApi> api,std::shared_ptr<IWsNotifier> wsNotifier=nullptr);
    ~KeeplyRestApi();
    KeeplyRestApi(const KeeplyRestApi&)=delete;
    KeeplyRestApi& operator=(const KeeplyRestApi&)=delete;
    KeeplyRestApi(KeeplyRestApi&&) noexcept=default;
    KeeplyRestApi& operator=(KeeplyRestApi&&) noexcept=default;
    RestResponse handle(const RestRequest& req);
private:
    struct Impl;
    std::shared_ptr<KeeplyApi> api_;
    std::shared_ptr<IWsNotifier> ws_;
    std::shared_ptr<Impl> impl_;
    RestResponse handleGetHealth();
    RestResponse handleGetState();
    RestResponse handlePostConfigSource(const RestRequest& req);
    RestResponse handlePostConfigArchive(const RestRequest& req);
    RestResponse handlePostConfigRestoreRoot(const RestRequest& req);
    RestResponse handlePostBackup(const RestRequest& req);
    RestResponse handleGetBackupJobs();
    RestResponse handleGetBackupJob(const RestRequest& req);
    RestResponse handleGetSnapshots();
    RestResponse handleGetDiff(const RestRequest& req);
    RestResponse handleGetSnapshotPaths(const RestRequest& req);
    RestResponse handlePostRestoreFile(const RestRequest& req);
    RestResponse handlePostRestoreSnapshot(const RestRequest& req);
    static RestResponse jsonOk(const std::string& json);
    static RestResponse jsonCreated(const std::string& json);
    static RestResponse jsonAccepted(const std::string& json);
    static RestResponse jsonBadRequest(const std::string& message);
    static RestResponse jsonMethodNotAllowed(const std::string& message);
    static RestResponse jsonNotFound(const std::string& message);
    static RestResponse jsonError(const std::string& message);
    static std::string escapeJson(const std::string& s);
    static std::string getPathParamAfterPrefix(const std::string& fullPath,const std::string& prefix);};
struct RestHttpServerConfig{
    std::shared_ptr<KeeplyApi> api;
    std::shared_ptr<IWsNotifier> wsNotifier;
    int port=8080;
};
int runRestHttpServer(const RestHttpServerConfig& config);}

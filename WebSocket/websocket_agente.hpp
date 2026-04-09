#pragma once
#include "../HTTP/servidor_rest.hpp"
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
namespace keeply {
namespace ws_internal {
struct BackupStoragePolicy;}
namespace fs = std::filesystem;
class IWsClientSession{
public:
    virtual ~IWsClientSession()=default;
    virtual std::string id() const=0;
    virtual void sendText(const std::string& payload)=0;
};
class KeeplyWsHub final:public IWsNotifier{
public:
    void addClient(IWsClientSession* client);
    void removeClient(IWsClientSession* client);
    void broadcastJson(const std::string& json) override;
    void publishJobEvent(const std::string& jobId,const std::string& json) override;
    void onClientMessage(IWsClientSession* client,const std::string& payload);
private:
    std::mutex mu_;
    std::unordered_map<std::string,IWsClientSession*> clients_;
    std::unordered_map<std::string,std::string> clientJobSubscription_;
    void sendSafe_(IWsClientSession* client,const std::string& payload);
};
struct WsClientConfig{
    std::string url="wss://backend.keeply.app.br/ws/agent";
    std::string agentId;
    std::string deviceName="keeply-agent";
    std::string hostName="keeply-host";
    std::string osName="linux";
    std::vector<std::string> localIps;
    std::string cpuModel;
    std::string cpuArchitecture;
    std::string kernelVersion;
    std::uint64_t totalMemoryBytes=0;
    unsigned cpuCores=0;
    std::string pairingCode;
    fs::path identityDir=defaultKeeplyDataDir()/"agent_identity";
    bool allowInsecureTls=false;
    int pairingPollIntervalMs=3000;
};
struct AgentIdentity{
    std::string deviceId;
    std::string userId;
    std::string fingerprintSha256;
    std::string pairingCode;
    fs::path certPemPath;
    fs::path keyPemPath;
    fs::path metaPath;
};
struct WsCommand{
    std::string type;
    std::string requestId;
    std::string path;
    std::string sourcePath;
    std::string scopeId;
    std::string label;
    std::string storage;
    std::string snapshot;
    std::string relPath;
    std::string outRoot;
    std::string ticketId;
    std::string downloadPathBase;
    std::string backupId;
    std::string backupRef;
    std::string bundleId;
    std::string archiveFile;
    std::string indexFile;
    std::string packFile;
    std::string blobFiles;
    std::string sourceRoot;
    std::string entryType;
    std::string raw;
};
class KeeplyAgentBootstrap final{
public:
    static AgentIdentity ensureRegistered(const WsClientConfig& config);
    static AgentIdentity loadPersistedIdentity(const WsClientConfig& config);
};
class KeeplyAgentWsClient final{
public:
    explicit KeeplyAgentWsClient(std::shared_ptr<KeeplyApi> api,AgentIdentity identity={});
    ~KeeplyAgentWsClient();
    void connect(const WsClientConfig& config);
    void run();
    void close();
    bool isConnected() const;
    void sendText(const std::string& payload);
private:
    struct UrlParts{std::string scheme;std::string host;int port=80;std::string target="/";};
    struct TlsState;
    std::shared_ptr<KeeplyApi> api_;
    AgentIdentity identity_;
    WsClientConfig config_;
    mutable std::mutex mu_;
    mutable std::mutex ioMu_;
    int sockfd_=-1;
    bool connected_=false;
    bool closeSent_=false;
    std::string recvBuffer_;
    std::shared_ptr<TlsState> tls_;
    void connectSocket_(const UrlParts& url);
    void performHandshake_(const UrlParts& url);
    void handleServerMessage_(const std::string& payload);
    void executeCommand_(const WsCommand& cmd);
    void runBackupCommand_(const std::string& label,const std::string& storage);
    void runBackupUpload_(const std::string& label,const ws_internal::BackupStoragePolicy& storagePolicy);
    void runRestoreFileCommand_(const std::string& snapshot,const std::string& relPath,const std::string& outRootRaw);
    void runRestoreSnapshotCommand_(const std::string& snapshot,const std::string& outRootRaw);
    void runRestoreCloudSnapshotCommand_(const WsCommand& cmd);
    void sendBackupProgress_(const std::string& label,const BackupProgress& progress);
    void sendBackupFinished_(const std::string& label,const BackupStats& stats);
    void sendBackupFailed_(const std::string& label,const BackupProgress& latestProgress,const std::string& message);
    void sendHello_();
    void sendJson_(const std::string& payload);
    void sendPong_(const std::string& payload);
    void sendClose_(std::uint16_t code,const std::string& reason);
    void sendFrame_(unsigned char opcode,const std::string& payload);
    std::string readHttpHeader_();
    bool readFrame_(unsigned char& opcode,std::string& payload);
    std::string buildDeviceDetailsJson_() const;
    std::string buildScanScopeJson_() const;
    std::string buildStateJson_() const;
    std::string buildSnapshotsJson_();
    std::string buildFsDisksJson_(const std::string& requestId) const;
    std::string buildFsListJson_(const std::string& requestId,const std::string& requestedPath) const;
    std::size_t writeRaw_(const void* data,std::size_t size);
    std::size_t readRaw_(void* data,std::size_t size);
    void configureTls_(const UrlParts& url);
    void ensureConnected_() const;
};}

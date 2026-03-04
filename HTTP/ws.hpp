#pragma once
#include "api-rest.hpp"
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace keeply {

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
    std::string url="ws://127.0.0.1:8081/ws/agent";
    std::string agentId;
    std::string deviceName="keeply-agent";
    std::string hostName="keeply-host";
    std::string osName="linux";
    std::string pairingCode;
    fs::path identityDir="build/agent_identity";
    bool allowInsecureTls=true;
    int pairingPollIntervalMs=3000;
};

struct AgentIdentity{
    std::string deviceId;
    std::string fingerprintSha256;
    std::string pairingCode;
    fs::path certPemPath;
    fs::path keyPemPath;
    fs::path metaPath;
};

class KeeplyAgentBootstrap final{
public:
    static AgentIdentity ensureRegistered(const WsClientConfig& config);
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
    int sockfd_=-1;
    bool connected_=false;
    bool closeSent_=false;
    std::string recvBuffer_;
    std::unique_ptr<TlsState> tls_;
    static UrlParts parseUrl_(const std::string& url);
    static std::string escapeJson_(const std::string& value);
    static std::string urlEncode_(const std::string& value);
    static std::string httpUrlFromWsUrl_(const std::string& wsUrl,const std::string& path);
    void connectSocket_(const UrlParts& url);
    void performHandshake_(const UrlParts& url);
    void handleServerMessage_(const std::string& payload);
    void sendHello_();
    void sendJson_(const std::string& payload);
    void sendPong_(const std::string& payload);
    void sendClose_(std::uint16_t code,const std::string& reason);
    void sendFrame_(unsigned char opcode,const std::string& payload);
    std::string readHttpHeader_();
    bool readFrame_(unsigned char& opcode,std::string& payload);
    std::string buildStateJson_() const;
    std::string buildSnapshotsJson_();
    std::size_t writeRaw_(const void* data,std::size_t size);
    std::size_t readRaw_(void* data,std::size_t size);
    void configureTls_(const UrlParts& url);
    void ensureConnected_() const;
};

} // namespace keeply
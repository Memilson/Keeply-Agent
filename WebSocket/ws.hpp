#pragma once

#include "api-rest.hpp"
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace keeply {

// "Sessão" abstrata de WS (adapter de lib real implementa sendText)
class IWsClientSession {
public:
    virtual ~IWsClientSession() = default;
    virtual std::string id() const = 0;
    virtual void sendText(const std::string& payload) = 0;
};

class KeeplyWsHub final : public IWsNotifier {
public:
    void addClient(IWsClientSession* client);
    void removeClient(IWsClientSession* client);

    // IWsNotifier
    void broadcastJson(const std::string& json) override;
    void publishJobEvent(const std::string& jobId, const std::string& json) override;

    // Hook para receber mensagem do cliente (ex.: subscribe job)
    void onClientMessage(IWsClientSession* client, const std::string& payload);

private:
    std::mutex mu_;
    std::unordered_map<std::string, IWsClientSession*> clients_;
    std::unordered_map<std::string, std::string> clientJobSubscription_; // clientId -> jobId

    void sendSafe_(IWsClientSession* client, const std::string& payload);
};

} // namespace keeply
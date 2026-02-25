#include "ws.hpp"

#include <iostream>

namespace keeply {

void KeeplyWsHub::addClient(IWsClientSession* client) {
    if (!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_[client->id()] = client;
}

void KeeplyWsHub::removeClient(IWsClientSession* client) {
    if (!client) return;
    std::lock_guard<std::mutex> lock(mu_);
    clients_.erase(client->id());
    clientJobSubscription_.erase(client->id());
}

void KeeplyWsHub::broadcastJson(const std::string& json) {
    std::vector<IWsClientSession*> snapshot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        snapshot.reserve(clients_.size());
        for (auto& [_, c] : clients_) snapshot.push_back(c);
    }

    for (auto* c : snapshot) {
        sendSafe_(c, json);
    }
}

void KeeplyWsHub::publishJobEvent(const std::string& jobId, const std::string& json) {
    std::vector<IWsClientSession*> targets;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& [cid, c] : clients_) {
            auto it = clientJobSubscription_.find(cid);
            if (it != clientJobSubscription_.end() && it->second == jobId) {
                targets.push_back(c);
            }
        }
    }

    for (auto* c : targets) {
        sendSafe_(c, json);
    }
}

void KeeplyWsHub::onClientMessage(IWsClientSession* client, const std::string& payload) {
    if (!client) return;

    // MVP super simples:
    // "ping" -> pong
    // "sub:JOB123" -> assina jobId
    if (payload == "ping") {
        sendSafe_(client, R"({"type":"pong"})");
        return;
    }

    const std::string prefix = "sub:";
    if (payload.rfind(prefix, 0) == 0) {
        const std::string jobId = payload.substr(prefix.size());
        {
            std::lock_guard<std::mutex> lock(mu_);
            clientJobSubscription_[client->id()] = jobId;
        }
        sendSafe_(client, std::string("{\"type\":\"subscribed\",\"jobId\":\"") + jobId + "\"}");
        return;
    }

    sendSafe_(client, R"({"type":"error","message":"Mensagem WS nao suportada no MVP"})");
}

void KeeplyWsHub::sendSafe_(IWsClientSession* client, const std::string& payload) {
    if (!client) return;
    try {
        client->sendText(payload);
    } catch (const std::exception& e) {
        // no MVP: loga e segue
        std::cerr << "[ws] sendText falhou para client " << client->id()
                  << ": " << e.what() << "\n";
    } catch (...) {
        std::cerr << "[ws] sendText falhou para client " << client->id()
                  << ": erro desconhecido\n";
    }
}

} // namespace keeply
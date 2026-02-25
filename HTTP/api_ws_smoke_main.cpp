#include "ApiRest/api-rest.hpp"
#include "WebSocket/ws.hpp"

#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace {

class FakeWsClient final : public keeply::IWsClientSession {
public:
    explicit FakeWsClient(std::string id) : id_(std::move(id)) {}

    std::string id() const override { return id_; }

    void sendText(const std::string& payload) override {
        messages.push_back(payload);
    }

    std::vector<std::string> messages;

private:
    std::string id_;
};

void expect(bool cond, const std::string& msg) {
    if (!cond) throw std::runtime_error(msg);
}

bool contains(const std::string& s, const std::string& needle) {
    return s.find(needle) != std::string::npos;
}

keeply::RestRequest makeReq(std::string method,
                            std::string path,
                            std::string body = {},
                            std::unordered_map<std::string, std::string> query = {}) {
    keeply::RestRequest r;
    r.method = std::move(method);
    r.path = std::move(path);
    r.body = std::move(body);
    r.query = std::move(query);
    return r;
}

void printResp(const keeply::RestRequest& req, const keeply::RestResponse& resp) {
    std::cout << req.method << " " << req.path
              << " -> " << resp.status << "\n"
              << resp.body << "\n\n";
}

} // namespace

int main() {
    try {
        namespace fs = std::filesystem;
        auto api = std::make_shared<keeply::KeeplyApi>();
        auto wsHub = std::make_shared<keeply::KeeplyWsHub>();
        keeply::KeeplyRestApi rest(api, wsHub);
        const std::string smokeArchive = (fs::current_path() / "build" / "keeply_api_smoke.db").string();
        const std::string smokeRestoreRoot = (fs::current_path() / "build" / "restore_api_smoke").string();

        FakeWsClient c1("c1");
        FakeWsClient c2("c2");
        wsHub->addClient(&c1);
        wsHub->addClient(&c2);

        // WS smoke
        wsHub->onClientMessage(&c1, "ping");
        expect(!c1.messages.empty(), "WS ping nao gerou resposta.");
        expect(contains(c1.messages.back(), "\"pong\""), "WS ping nao respondeu pong.");

        const std::size_t c1BeforeSub = c1.messages.size();
        const std::size_t c2BeforeSub = c2.messages.size();
        wsHub->onClientMessage(&c1, "sub:JOB42");
        wsHub->publishJobEvent("JOB42", R"({"type":"job.progress","jobId":"JOB42","pct":10})");
        expect(c1.messages.size() >= c1BeforeSub + 2, "WS subscribe/publish nao entregou mensagens ao assinante.");
        expect(c2.messages.size() == c2BeforeSub, "WS publish entregou evento para cliente nao assinante.");

        // REST smoke
        {
            auto req = makeReq("GET", "/api/v1/health");
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "GET /health deveria retornar 200.");
            expect(contains(resp.body, "\"ok\":true"), "GET /health body inesperado.");
        }
        {
            auto req = makeReq("GET", "/api/v1/state");
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "GET /state deveria retornar 200.");
            expect(contains(resp.body, "\"source\""), "GET /state sem campo source.");
        }
        {
            auto req = makeReq("POST", "/api/v1/config/source", "/home");
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "POST /config/source deveria retornar 200.");
        }
        {
            auto req = makeReq("POST", "/api/v1/config/archive", smokeArchive);
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "POST /config/archive deveria retornar 200.");
        }
        {
            auto req = makeReq("POST", "/api/v1/config/restore-root", smokeRestoreRoot);
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "POST /config/restore-root deveria retornar 200.");
        }
        {
            auto req = makeReq("GET", "/api/v1/state");
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "GET /state (apos config) deveria retornar 200.");
            expect(contains(resp.body, "keeply_api_smoke.db"), "GET /state nao refletiu archive configurado.");
        }
        {
            auto req = makeReq("GET", "/api/v1/snapshots");
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 200, "GET /snapshots deveria retornar 200.");
            expect(contains(resp.body, "\"items\""), "GET /snapshots sem campo items.");
        }
        {
            auto req = makeReq("GET", "/api/v1/rota-inexistente");
            auto resp = rest.handle(req);
            printResp(req, resp);
            expect(resp.status == 404, "Rota inexistente deveria retornar 404.");
        }

        std::cout << "SMOKE TEST OK (REST adapter + WS hub + main libs)\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "SMOKE TEST FALHOU: " << e.what() << "\n";
        return 1;
    }
}

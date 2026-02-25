#include "api-rest.hpp"

#include <sstream>
#include <stdexcept>

namespace keeply {

KeeplyRestApi::KeeplyRestApi(std::shared_ptr<KeeplyApi> api,
                             std::shared_ptr<IWsNotifier> wsNotifier)
    : api_(std::move(api)), ws_(std::move(wsNotifier)) {
    if (!api_) throw std::runtime_error("KeeplyRestApi requer KeeplyApi.");
}

RestResponse KeeplyRestApi::handle(const RestRequest& req) {
    try {
        // Rotas básicas (MVP)
        if (req.method == "GET" && req.path == "/api/v1/health") {
            return handleGetHealth();
        }
        if (req.method == "GET" && req.path == "/api/v1/state") {
            return handleGetState();
        }

        if (req.method == "POST" && req.path == "/api/v1/config/source") {
            return handlePostConfigSource(req);
        }
        if (req.method == "POST" && req.path == "/api/v1/config/archive") {
            return handlePostConfigArchive(req);
        }
        if (req.method == "POST" && req.path == "/api/v1/config/restore-root") {
            return handlePostConfigRestoreRoot(req);
        }

        if (req.method == "POST" && req.path == "/api/v1/backup") {
            return handlePostBackup(req);
        }

        if (req.method == "GET" && req.path == "/api/v1/snapshots") {
            return handleGetSnapshots();
        }

        if (req.method == "GET" && req.path == "/api/v1/diff") {
            return handleGetDiff(req);
        }
        if (req.method == "GET" && req.path.rfind("/api/v1/snapshots/", 0) == 0 &&
            req.path.size() > std::string("/api/v1/snapshots/").size()) {
            if (req.path.size() >= 6 && req.path.substr(req.path.size() - 6) == "/paths") {
                return handleGetSnapshotPaths(req);}}
        if (req.method == "POST" && req.path == "/api/v1/restore/file") {
            return handlePostRestoreFile(req);
        }
        if (req.method == "POST" && req.path == "/api/v1/restore/snapshot") {
            return handlePostRestoreSnapshot(req);
        }

        return jsonNotFound("Rota nao encontrada.");
    } catch (const std::exception& e) {
        return jsonError(e.what());
    } catch (...) {
        return jsonError("Erro interno desconhecido.");
    }
}

RestResponse KeeplyRestApi::handleGetHealth() {
    return jsonOk(R"({"ok":true,"service":"keeply","version":"v1"})");
}

RestResponse KeeplyRestApi::handleGetState() {
    std::lock_guard<std::mutex> lock(mu_);
    const auto& s = api_->state();

    std::ostringstream oss;
    oss << "{"
        << "\"source\":\"" << escapeJson(s.source) << "\","
        << "\"archive\":\"" << escapeJson(s.archive) << "\","
        << "\"restoreRoot\":\"" << escapeJson(s.restoreRoot) << "\""
        << "}";
    return jsonOk(oss.str());
}
RestResponse KeeplyRestApi::handlePostConfigSource(const RestRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);
    api_->setSource(req.body);
    if (ws_) ws_->broadcastJson(R"({"type":"config.updated","field":"source"})");
    return jsonOk(R"({"ok":true})");
}
RestResponse KeeplyRestApi::handlePostConfigArchive(const RestRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);
    api_->setArchive(req.body);
    if (ws_) ws_->broadcastJson(R"({"type":"config.updated","field":"archive"})");
    return jsonOk(R"({"ok":true})");
}
RestResponse KeeplyRestApi::handlePostConfigRestoreRoot(const RestRequest& req) {
    std::lock_guard<std::mutex> lock(mu_);
    api_->setRestoreRoot(req.body);
    if (ws_) ws_->broadcastJson(R"({"type":"config.updated","field":"restoreRoot"})");
    return jsonOk(R"({"ok":true})");
}
RestResponse KeeplyRestApi::handlePostBackup(const RestRequest& req) {
    const std::string label = req.body;
    BackupStats stats{};
    {
        std::lock_guard<std::mutex> lock(mu_);
        stats = api_->runBackup(label);
    }
    const std::size_t chunksReused =
        (stats.chunks >= stats.uniqueChunksInserted)
            ? (stats.chunks - stats.uniqueChunksInserted)
            : 0;
    std::ostringstream oss;
    oss << "{"
        << "\"ok\":true,"
        << "\"filesScanned\":" << stats.scanned << ","
        << "\"filesAdded\":" << stats.added << ","
        << "\"filesUnchanged\":" << stats.reused << ","
        << "\"chunksNew\":" << stats.uniqueChunksInserted << ","
        << "\"chunksReused\":" << chunksReused << ","
        << "\"bytesRead\":" << stats.bytesRead << ","
        << "\"bytesStoredCompressed\":0,"
        << "\"warnings\":" << stats.warnings
        << "}";
    if (ws_) ws_->broadcastJson(R"({"type":"backup.finished"})");
    return jsonCreated(oss.str());
}
RestResponse KeeplyRestApi::handleGetSnapshots() {
    std::vector<SnapshotRow> rows;
    {
        std::lock_guard<std::mutex> lock(mu_);
        rows = api_->listSnapshots();
    }
    std::ostringstream oss;
    oss << "{\"items\":[";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const auto& r = rows[i];
        if (i) oss << ",";
        oss << "{"
            << "\"id\":" << r.id << ","
            << "\"createdAt\":\"" << escapeJson(r.createdAt) << "\","
            << "\"sourceRoot\":\"" << escapeJson(r.sourceRoot) << "\","
            << "\"label\":\"" << escapeJson(r.label) << "\","
            << "\"fileCount\":" << r.fileCount
            << "}";
    }
    oss << "]}";
    return jsonOk(oss.str());
}

RestResponse KeeplyRestApi::handleGetDiff(const RestRequest& req) {
    const auto itOlder = req.query.find("older");
    const auto itNewer = req.query.find("newer");

    std::vector<ChangeEntry> rows;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (itOlder == req.query.end() && itNewer == req.query.end()) {
            rows = api_->diffLatestVsPrevious();
        } else if (itOlder != req.query.end() && itNewer != req.query.end()) {
            rows = api_->diffSnapshots(itOlder->second, itNewer->second);
        } else {
            return jsonBadRequest("Use ambos os parametros 'older' e 'newer', ou nenhum.");
        }
    }

    std::ostringstream oss;
    oss << "{\"items\":[";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        const auto& c = rows[i];
        if (i) oss << ",";
        oss << "{"
            << "\"path\":\"" << escapeJson(c.path) << "\","
            << "\"status\":\"" << escapeJson(c.status) << "\","
            << "\"oldSize\":" << c.oldSize << ","
            << "\"newSize\":" << c.newSize << ","
            << "\"oldMtime\":" << c.oldMtime << ","
            << "\"newMtime\":" << c.newMtime
            << "}";
    }
    oss << "]}";
    return jsonOk(oss.str());
}

RestResponse KeeplyRestApi::handleGetSnapshotPaths(const RestRequest& req) {
    const std::string prefix = "/api/v1/snapshots/";
    if (req.path.size() <= prefix.size()) {
        return jsonBadRequest("Path de snapshot invalido.");
    }

    // extrai "{id}" de /api/v1/snapshots/{id}/paths
    const auto mid = req.path.substr(prefix.size());
    const auto slash = mid.find('/');
    if (slash == std::string::npos) {
        return jsonBadRequest("Path de snapshot invalido.");
    }
    const std::string snapshotId = mid.substr(0, slash);

    std::vector<std::string> paths;
    {
        std::lock_guard<std::mutex> lock(mu_);
        paths = api_->listSnapshotPaths(snapshotId);
    }

    std::ostringstream oss;
    oss << "{\"snapshot\":\"" << escapeJson(snapshotId) << "\",\"items\":[";
    for (std::size_t i = 0; i < paths.size(); ++i) {
        if (i) oss << ",";
        oss << "\"" << escapeJson(paths[i]) << "\"";
    }
    oss << "]}";
    return jsonOk(oss.str());
}

RestResponse KeeplyRestApi::handlePostRestoreFile(const RestRequest& req) {
    std::istringstream iss(req.body);
    std::string snapshot, relPath, outRoot;
    if (!std::getline(iss, snapshot, '|') || !std::getline(iss, relPath, '|')) {
        return jsonBadRequest("Formato invalido. Use: snapshot|relpath|outRoot(opcional)");
    }
    std::getline(iss, outRoot);

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (outRoot.empty()) api_->restoreFile(snapshot, relPath, std::nullopt);
        else api_->restoreFile(snapshot, relPath, fs::path(outRoot));
    }

    if (ws_) ws_->broadcastJson(R"({"type":"restore.file.finished"})");
    return jsonOk(R"({"ok":true})");
}

RestResponse KeeplyRestApi::handlePostRestoreSnapshot(const RestRequest& req) {
    // MVP (temporário): body = "snapshot|outRootOpcional"
    std::istringstream iss(req.body);
    std::string snapshot, outRoot;
    if (!std::getline(iss, snapshot, '|')) {
        return jsonBadRequest("Formato invalido. Use: snapshot|outRoot(opcional)");
    }
    std::getline(iss, outRoot);

    {
        std::lock_guard<std::mutex> lock(mu_);
        if (outRoot.empty()) api_->restoreSnapshot(snapshot, std::nullopt);
        else api_->restoreSnapshot(snapshot, fs::path(outRoot));
    }

    if (ws_) ws_->broadcastJson(R"({"type":"restore.snapshot.finished"})");
    return jsonOk(R"({"ok":true})");
}

RestResponse KeeplyRestApi::jsonOk(const std::string& json) {
    RestResponse r;
    r.status = 200;
    r.body = json;
    return r;
}

RestResponse KeeplyRestApi::jsonCreated(const std::string& json) {
    RestResponse r;
    r.status = 201;
    r.body = json;
    return r;
}

RestResponse KeeplyRestApi::jsonBadRequest(const std::string& message) {
    RestResponse r;
    r.status = 400;
    r.body = std::string("{\"ok\":false,\"error\":\"") + escapeJson(message) + "\"}";
    return r;
}

RestResponse KeeplyRestApi::jsonNotFound(const std::string& message) {
    RestResponse r;
    r.status = 404;
    r.body = std::string("{\"ok\":false,\"error\":\"") + escapeJson(message) + "\"}";
    return r;
}

RestResponse KeeplyRestApi::jsonError(const std::string& message) {
    RestResponse r;
    r.status = 500;
    r.body = std::string("{\"ok\":false,\"error\":\"") + escapeJson(message) + "\"}";
    return r;
}

std::string KeeplyRestApi::escapeJson(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);

    for (char c : s) {
        switch (c) {
            case '\"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) out += "?";
                else out += c;
        }
    }
    return out;
}

std::string KeeplyRestApi::getPathParamAfterPrefix(const std::string& fullPath, const std::string& prefix) {
    if (fullPath.rfind(prefix, 0) != 0) return "";
    return fullPath.substr(prefix.size());
}

} // namespace keeply

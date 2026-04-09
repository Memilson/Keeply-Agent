#include "../WebSocket/websocket_interno.hpp"
#include "../Cloud/fila_upload.hpp"
#include "../Core/tls_context.hpp"
#include "../Storage/backend_armazenamento.hpp"
#include "http_util.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#ifdef __linux__
#include <sys/stat.h>
#endif
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
namespace keeply::ws_internal {
namespace {
struct HttpTls {
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;
    HttpTls() = default;
    HttpTls(const HttpTls&) = delete;
    HttpTls& operator=(const HttpTls&) = delete;
    HttpTls(HttpTls&& o) noexcept : ctx(o.ctx), ssl(o.ssl) { o.ctx = nullptr; o.ssl = nullptr; }
    HttpTls& operator=(HttpTls&& o) noexcept {
        if (this != &o) {
            cleanup();
            ctx = o.ctx; ssl = o.ssl;
            o.ctx = nullptr; o.ssl = nullptr;}
        return *this;}
    void cleanup() noexcept {
        if (ssl) { SSL_free(ssl); ssl = nullptr; }
        if (ctx) { SSL_CTX_free(ctx); ctx = nullptr; }}
    ~HttpTls() { cleanup(); }
};
inline std::string trimAscii(const std::string& value) { return keeply::trim(value); }
std::string decodeChunkedBody(const std::string& body) {
    std::string decoded;
    std::size_t cursor = 0;
    for (;;) {
        const std::size_t lineEnd = body.find("\r\n", cursor);
        if (lineEnd == std::string::npos) throw std::runtime_error("Resposta HTTP chunked invalida.");
        const std::string sizeToken = trimAscii(body.substr(cursor, lineEnd - cursor));
        const std::size_t semicolon = sizeToken.find(';');
        const std::string rawSize = sizeToken.substr(0, semicolon);
        std::size_t chunkSize = 0;
        try {
            chunkSize = static_cast<std::size_t>(std::stoull(rawSize, nullptr, 16));
        } catch (...) {
            throw std::runtime_error("Tamanho de chunk HTTP invalido.");}
        cursor = lineEnd + 2;
        if (chunkSize == 0) {
            const std::size_t trailerEnd = body.find("\r\n\r\n", cursor);
            if (trailerEnd == std::string::npos && body.find("\r\n", cursor) == std::string::npos) {
                throw std::runtime_error("Trailer HTTP chunked invalido.");}
            return decoded;}
        if (body.size() < cursor + chunkSize + 2) throw std::runtime_error("Chunk HTTP truncado.");
        decoded.append(body, cursor, chunkSize);
        cursor += chunkSize;
        if (body.compare(cursor, 2, "\r\n") != 0) throw std::runtime_error("Separador de chunk HTTP invalido.");
        cursor += 2;}}
HttpResponse parseHttpResponse(const std::string& raw, const std::string& contextLabel) {
    const std::size_t headerEnd = raw.find("\r\n\r\n");
    if (headerEnd == std::string::npos) throw std::runtime_error("Resposta HTTP invalida no " + contextLabel + ".");
    std::istringstream hs(raw.substr(0, headerEnd));
    std::string statusLine;
    std::getline(hs, statusLine);
    std::istringstream sl(statusLine);
    std::string httpVersion;
    HttpResponse resp;
    sl >> httpVersion >> resp.status;
    if (resp.status <= 0) throw std::runtime_error("Status HTTP invalido no " + contextLabel + ".");
    std::string line;
    while (std::getline(hs, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        resp.headers[toLower(trimAscii(line.substr(0, colon)))] = trimAscii(line.substr(colon + 1));}
    resp.body = raw.substr(headerEnd + 4);
    const auto transferEncodingIt = resp.headers.find("transfer-encoding");
    if (transferEncodingIt != resp.headers.end() && toLower(transferEncodingIt->second).find("chunked") != std::string::npos) {
        resp.body = decodeChunkedBody(resp.body);}
    return resp;}
HttpTls connectTls(int fd,
                   const ParsedUrl& url,
                   const std::optional<fs::path>& certPemPath,
                   const std::optional<fs::path>& keyPemPath,
                   bool allowInsecureTls) {
    keeply::TlsContextConfig tlsCfg;
    tlsCfg.host = url.host;
    tlsCfg.allowInsecure = allowInsecureTls;
    tlsCfg.certPemPath = certPemPath;
    tlsCfg.keyPemPath = keyPemPath;
    HttpTls tls;
    tls.ssl = keeply::createConnectedTlsSession(fd, tlsCfg, tls.ctx);
    return tls;}
std::string readHttpResponseBody(int fd, SSL* ssl) {
    std::string data;
    char buf[4096];
    for (;;) {
        const std::size_t n = ssl ? readSomeSsl(ssl, buf, sizeof(buf)) : readSomeFd(fd, buf, sizeof(buf));
        if (n == 0) break;
        data.append(buf, n);}
    return data;}}
std::size_t computeUploadWorkerCount(std::size_t blobPartCount) {
    if (blobPartCount <= 1) return blobPartCount;
    const std::size_t hw = std::max<std::size_t>(std::thread::hardware_concurrency(), 2);
    const std::size_t cpuBased = hw >= 8 ? hw * 2 : hw;
    const std::size_t target = std::clamp<std::size_t>(cpuBased, 2, 20);
    return std::min<std::size_t>(blobPartCount, target);
}
HttpResponse httpPostJson(const std::string& url,
                          const std::string& body,
                          const std::optional<fs::path>& certPemPath,
                          const std::optional<fs::path>& keyPemPath,
                          bool allowInsecureTls) {
    const ParsedUrl parsed = parseUrlCommon(url);
    if (parsed.scheme != "http" && parsed.scheme != "https") {
        throw std::runtime_error("Enroll HTTP suporta apenas http:// ou https://");}
    int fd = openTcpSocket(parsed.host, parsed.port);
    HttpTls tls;
    SSL* ssl = nullptr;
    try {
        if (parsed.scheme == "https") {
            tls = connectTls(fd, parsed, certPemPath, keyPemPath, allowInsecureTls);
            ssl = tls.ssl;}
        std::ostringstream req;
        req << "POST " << parsed.target << " HTTP/1.1\r\n";
        req << "Host: " << parsed.host << ":" << parsed.port << "\r\n";
        req << "Content-Type: application/json\r\n";
        req << "Accept: application/json\r\n";
        req << "Connection: close\r\n";
        req << "Content-Length: " << body.size() << "\r\n\r\n";
        req << body;
        const std::string request = req.str();
        if (ssl) static_cast<void>(writeAllSsl(ssl, request.data(), request.size()));
        else static_cast<void>(writeAllFd(fd, request.data(), request.size()));
        std::string raw = readHttpResponseBody(fd, ssl);
        tls.cleanup();
        http_internal::closeSocketFd(fd);
        fd = -1;
        return parseHttpResponse(raw, "enroll");
    } catch (...) {
        tls.cleanup();
        if (fd >= 0) http_internal::closeSocketFd(fd);
        throw;}}
HttpResponse httpGet(const std::string& url,
                     const std::optional<fs::path>& certPemPath,
                     const std::optional<fs::path>& keyPemPath,
                     bool allowInsecureTls) {
    const ParsedUrl parsed = parseUrlCommon(url);
    if (parsed.scheme != "http" && parsed.scheme != "https") {
        throw std::runtime_error("Download HTTP suporta apenas http:// ou https://");}
    int fd = openTcpSocket(parsed.host, parsed.port);
    HttpTls tls;
    SSL* ssl = nullptr;
    try {
        if (parsed.scheme == "https") {
            tls = connectTls(fd, parsed, certPemPath, keyPemPath, allowInsecureTls);
            ssl = tls.ssl;}
        std::ostringstream req;
        req << "GET " << parsed.target << " HTTP/1.1\r\n";
        req << "Host: " << parsed.host << ":" << parsed.port << "\r\n";
        req << "Accept: application/octet-stream\r\n";
        req << "Connection: close\r\n\r\n";
        const std::string request = req.str();
        if (ssl) static_cast<void>(writeAllSsl(ssl, request.data(), request.size()));
        else static_cast<void>(writeAllFd(fd, request.data(), request.size()));
        std::string raw = readHttpResponseBody(fd, ssl);
        tls.cleanup();
        http_internal::closeSocketFd(fd);
        fd = -1;
        return parseHttpResponse(raw, "download");
    } catch (...) {
        tls.cleanup();
        if (fd >= 0) http_internal::closeSocketFd(fd);
        throw;}}
namespace {
HttpResponse httpPostMultipartFile(const std::string& url,
                                   const std::vector<MultipartField>& fields,
                                   const std::string& fileFieldName,
                                   const fs::path& filePath,
                                   const std::string& fileName,
                                   const std::string& fileContentType,
                                   const std::optional<fs::path>& certPemPath,
                                   const std::optional<fs::path>& keyPemPath,
                                   bool allowInsecureTls) {
    const ParsedUrl parsed = parseUrlCommon(url);
    if (parsed.scheme != "http" && parsed.scheme != "https") {
        throw std::runtime_error("Upload HTTP suporta apenas http:// ou https://");}
    std::error_code fileEc;
    const std::uintmax_t fileSize = fs::file_size(filePath, fileEc);
    if (fileEc) throw std::runtime_error("Falha lendo tamanho do arquivo para upload: " + fileEc.message());
    const std::string boundary = "----KeeplyBoundary" + randomDigits(24);
    std::vector<std::string> fieldParts;
    fieldParts.reserve(fields.size());
    std::uint64_t contentLength = 0;
    for (const auto& field : fields) {
        std::ostringstream part;
        part << "--" << boundary << "\r\n";
        part << "Content-Disposition: form-data; name=\"" << field.name << "\"\r\n\r\n";
        part << field.value << "\r\n";
        fieldParts.push_back(part.str());
        contentLength += fieldParts.back().size();}
    std::ostringstream fileHeader;
    fileHeader << "--" << boundary << "\r\n";
    fileHeader << "Content-Disposition: form-data; name=\"" << fileFieldName
               << "\"; filename=\"" << fileName << "\"\r\n";
    fileHeader << "Content-Type: " << fileContentType << "\r\n\r\n";
    const std::string fileHeaderStr = fileHeader.str();
    const std::string fileFooterStr = "\r\n--" + boundary + "--\r\n";
    contentLength += fileHeaderStr.size();
    contentLength += fileSize;
    contentLength += fileFooterStr.size();
    int fd = openTcpSocket(parsed.host, parsed.port);
    HttpTls tls;
    SSL* ssl = nullptr;
    try {
        if (parsed.scheme == "https") {
            tls = connectTls(fd, parsed, certPemPath, keyPemPath, allowInsecureTls);
            ssl = tls.ssl;}
        std::ostringstream req;
        req << "POST " << parsed.target << " HTTP/1.1\r\n";
        req << "Host: " << parsed.host << ":" << parsed.port << "\r\n";
        req << "Content-Type: multipart/form-data; boundary=" << boundary << "\r\n";
        req << "Accept: application/json\r\n";
        req << "Connection: close\r\n";
        req << "Content-Length: " << contentLength << "\r\n\r\n";
        const std::string headers = req.str();
        if (ssl) static_cast<void>(writeAllSsl(ssl, headers.data(), headers.size()));
        else static_cast<void>(writeAllFd(fd, headers.data(), headers.size()));
        for (const std::string& part : fieldParts) {
            if (ssl) static_cast<void>(writeAllSsl(ssl, part.data(), part.size()));
            else static_cast<void>(writeAllFd(fd, part.data(), part.size()));}
        if (ssl) static_cast<void>(writeAllSsl(ssl, fileHeaderStr.data(), fileHeaderStr.size()));
        else static_cast<void>(writeAllFd(fd, fileHeaderStr.data(), fileHeaderStr.size()));
        std::ifstream in(filePath, std::ios::binary);
        if (!in) throw std::runtime_error("Falha abrindo arquivo para upload: " + filePath.string());
        std::array<char, 64 * 1024> buf{};
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const std::streamsize got = in.gcount();
            if (got <= 0) break;
            if (ssl) static_cast<void>(writeAllSsl(ssl, buf.data(), static_cast<std::size_t>(got)));
            else static_cast<void>(writeAllFd(fd, buf.data(), static_cast<std::size_t>(got)));}
        if (!in.eof() && in.fail()) throw std::runtime_error("Falha lendo arquivo durante upload.");
        if (ssl) static_cast<void>(writeAllSsl(ssl, fileFooterStr.data(), fileFooterStr.size()));
        else static_cast<void>(writeAllFd(fd, fileFooterStr.data(), fileFooterStr.size()));
        std::string raw = readHttpResponseBody(fd, ssl);
        tls.cleanup();
        http_internal::closeSocketFd(fd);
        fd = -1;
        return parseHttpResponse(raw, "upload");
    } catch (...) {
        tls.cleanup();
        if (fd >= 0) http_internal::closeSocketFd(fd);
        throw;}}}
std::string hexEncode(const unsigned char* data, std::size_t size) {
    return keeply::hexEncode(data, size);}
std::string base64Encode(const unsigned char* data, std::size_t size) {
    std::string out;
    out.resize(((size + 2) / 3) * 4);
    const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&out[0]), data, static_cast<int>(size));
    if (written < 0) throw std::runtime_error("Falha ao codificar base64.");
    out.resize(static_cast<std::size_t>(written));
    return out;}
std::string randomBase64(std::size_t bytes) {
    std::vector<unsigned char> raw(bytes);
    std::random_device rd;
    for (std::size_t i = 0; i < bytes; ++i) raw[i] = static_cast<unsigned char>(rd());
    return base64Encode(raw.data(), raw.size());}
std::string randomDigits(std::size_t digits) {
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 9);
    std::string out;
    out.reserve(digits);
    for (std::size_t i = 0; i < digits; ++i) out.push_back(static_cast<char>('0' + dist(rd)));
    return out;}
std::size_t writeAllSsl(ssl_st* sslHandle, const void* data, std::size_t size) {
    SSL* ssl = static_cast<SSL*>(sslHandle);
    if (!ssl || SSL_get_fd(ssl) < 0)
        throw std::runtime_error("SSL_write: conexao invalida.");
    const unsigned char* ptr = static_cast<const unsigned char*>(data);
    std::size_t offset = 0;
    while (offset < size) {
        ERR_clear_error();
        const int n = SSL_write(ssl, ptr + static_cast<std::ptrdiff_t>(offset), static_cast<int>(size - offset));
        if (n <= 0) {
            const int err = SSL_get_error(ssl, n);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
            std::string detail;
            if (err == SSL_ERROR_SYSCALL) {
                int e = errno;
                detail = "syscall: " + std::string(e ? std::strerror(e) : "EOF inesperado");
            } else if (err == SSL_ERROR_ZERO_RETURN) {
                detail = "conexao fechada pelo servidor";
            } else {
                unsigned long ecode = ERR_peek_last_error();
                char buf[256]; ERR_error_string_n(ecode, buf, sizeof(buf));
                detail = buf;}
            throw std::runtime_error("SSL_write falhou (ssl_error=" + std::to_string(err) + "): " + detail);}
        offset += static_cast<std::size_t>(n);}
    return offset;}
std::size_t readSomeSsl(ssl_st* sslHandle, void* data, std::size_t size) {
    SSL* ssl = static_cast<SSL*>(sslHandle);
    for (;;) {
        const int n = SSL_read(ssl, data, static_cast<int>(size));
        if (n > 0) return static_cast<std::size_t>(n);
        const int err = SSL_get_error(ssl, n);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
        if (err == SSL_ERROR_ZERO_RETURN) return 0;
        std::string detail;
        if (err == SSL_ERROR_SYSCALL) {
            int e = errno;
            detail = "syscall: " + std::string(e ? std::strerror(e) : "EOF inesperado");
        } else {
            unsigned long ecode = ERR_peek_last_error();
            char buf[256]; ERR_error_string_n(ecode, buf, sizeof(buf));
            detail = buf;}
        throw std::runtime_error("SSL_read falhou (ssl_error=" + std::to_string(err) + "): " + detail);}}
std::map<std::string, std::string> loadIdentityMeta(const fs::path& metaPath) {
    std::map<std::string, std::string> out;
    std::ifstream in(metaPath);
    if (!in) return out;
    std::string line;
    while (std::getline(in, line)) {
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        out[line.substr(0, eq)] = line.substr(eq + 1);}
    return out;}
std::string extractJsonStringField(const std::string& json, const std::string& field) {
    const std::string needle = "\"" + field + "\"";
    std::size_t searchFrom = 0;
    while (true) {
        const std::size_t keyPos = json.find(needle, searchFrom);
        if (keyPos == std::string::npos) return {};
        const bool escapedKey = keyPos > 0 && json[keyPos - 1] == '\\';
        if (escapedKey) {
            searchFrom = keyPos + needle.size();
            continue;}
        std::size_t colonPos = keyPos + needle.size();
        while (colonPos < json.size() && std::isspace(static_cast<unsigned char>(json[colonPos]))) ++colonPos;
        if (colonPos >= json.size() || json[colonPos] != ':') {
            searchFrom = keyPos + needle.size();
            continue;}
        ++colonPos;
        while (colonPos < json.size() && std::isspace(static_cast<unsigned char>(json[colonPos]))) ++colonPos;
        if (colonPos >= json.size() || json[colonPos] != '"') return {};
        ++colonPos;
        std::string out;
        out.reserve(32);
        bool escaped = false;
        for (std::size_t i = colonPos; i < json.size(); ++i) {
            const char c = json[i];
            if (escaped) {
                switch (c) {
                    case '"': out.push_back('"'); break;
                    case '\\': out.push_back('\\'); break;
                    case '/': out.push_back('/'); break;
                    case 'b': out.push_back('\b'); break;
                    case 'f': out.push_back('\f'); break;
                    case 'n': out.push_back('\n'); break;
                    case 'r': out.push_back('\r'); break;
                    case 't': out.push_back('\t'); break;
                    case 'u':
                        if (i + 4 >= json.size()) return {};
                        out.push_back('?');
                        i += 4;
                        break;
                    default:
                        out.push_back(c);
                        break;}
                escaped = false;
                continue;}
            if (c == '\\') {
                escaped = true;
                continue;}
            if (c == '"') return out;
            out.push_back(c);}
        return {};}}
void saveIdentityMeta(const AgentIdentity& identity) {
    std::ofstream out(identity.metaPath, std::ios::trunc);
    if (!out) throw std::runtime_error("Falha ao salvar identity.meta do agente.");
    out << "device_id=" << identity.deviceId << "\n";
    out << "user_id=" << identity.userId << "\n";
    out << "fingerprint_sha256=" << identity.fingerprintSha256 << "\n";
    out << "pairing_code=" << identity.pairingCode << "\n";
    out << "cert_pem=" << pathToUtf8(identity.certPemPath) << "\n";
    out << "key_pem=" << pathToUtf8(identity.keyPemPath) << "\n";
    out.flush();
    if (!out) throw std::runtime_error("Falha ao finalizar identity.meta do agente.");
#ifdef __linux__
    if (::chmod(identity.metaPath.c_str(), static_cast<mode_t>(0600)) != 0) {
        throw std::runtime_error("Falha ao ajustar permissoes de identity.meta.");}
#endif
}
std::vector<std::string> splitPipe(const std::string& value) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= value.size()) {
        const std::size_t pos = value.find('|', start);
        out.push_back(value.substr(start, pos == std::string::npos ? std::string::npos : pos - start));
        if (pos == std::string::npos) break;
        start = pos + 1;}
    return out;}
BackupStoragePolicy parseBackupStoragePolicy(const std::string& raw) {
    BackupStoragePolicy policy;
    const std::string normalized = toLower(trim(raw));
    if (normalized.empty()) return policy;
    const auto contains = [&](const char* needle) {
        return normalized.find(needle) != std::string::npos;
    };
    const bool localOnly = contains("local_only") || contains("local-only") ||
                           contains("no_cloud")   || contains("no-cloud")   || contains("nocloud");
    const bool keepLocal = localOnly || contains("keep_local") || contains("keep-local") || contains("keeplocal");
    if (localOnly) policy.uploadCloud = false;
    if (keepLocal) {
        policy.keepLocal = true;
        policy.deleteLocalAfterUpload = false;}
    return policy;}
void deleteLocalArchiveArtifacts(const fs::path& archivePath) {
    const ArchiveStoragePaths paths = describeArchiveStorage(archivePath);
    std::error_code ec;
    fs::remove(paths.archivePath, ec); ec.clear();
    fs::remove(paths.packPath,    ec); ec.clear();
    fs::remove(paths.indexPath,   ec);}
std::string httpUrlFromWsUrl(const std::string& wsUrl, const std::string& path) {
    ParsedUrl parsed = parseUrlCommon(wsUrl);
    if (parsed.scheme == "ws") parsed.scheme = "http";
    else if (parsed.scheme == "wss") parsed.scheme = "https";
    else throw std::runtime_error("URL websocket invalida para derivar URL HTTP.");
    parsed.target = path;
    std::ostringstream oss;
    oss << parsed.scheme << "://" << parsed.host << ":" << parsed.port << parsed.target;
    return oss.str();}
UploadBundleResult uploadArchiveBackup(const WsClientConfig& config,
                                       const AgentIdentity& identity,
                                       const AppState& state,
                                       const std::string& label,
                                       const std::function<void(const UploadProgressSnapshot&)>& onProgress) {
    if (identity.userId.empty()) throw std::runtime_error("Agente sem userId persistido. Refaça o pareamento.");
    const fs::path archivePath = pathFromUtf8(state.archive);
    if (!fs::exists(archivePath)) throw std::runtime_error("Arquivo de backup local nao encontrado para upload.");
    StorageArchive archive(archivePath);
    const auto snapshots = archive.listSnapshots();
    std::string backupType = snapshots.empty() ? "full" : (snapshots.back().backupType == "incremental" ? "incremental" : "full");
    const auto bundle = archive.exportCloudBundle(defaultCloudBundleExportRoot(), 16ull * 1024ull * 1024ull);
    const std::string url = httpUrlFromWsUrl(config.url, "/api/agent/backups/upload");
    UploadBundleResult result;
    result.bundleId = bundle.bundleId;
    const std::size_t filesTotal = bundle.files.size();
    std::atomic<std::size_t> uploadedCount{0};
    std::atomic<std::size_t> uploadedBlobCount{0};
    try {
        auto uploadOne = [&](const StorageArchive::CloudBundleFile& item, std::size_t attempt) {
            const std::string role = item.manifest ? "manifest" : (item.blobPart ? "blob" : "metadata");
            std::size_t blobPartIndex = 0;
            if (item.blobPart) {
                const std::size_t marker = item.uploadName.find_last_of('-');
                const std::size_t dot = item.uploadName.rfind('.');
                if (marker != std::string::npos && dot != std::string::npos && dot > marker + 1) {
                    blobPartIndex = static_cast<std::size_t>(
                        std::strtoull(item.uploadName.substr(marker + 1, dot - marker - 1).c_str(), nullptr, 10));}}
            if (onProgress) {
                onProgress(UploadProgressSnapshot{
                    uploadedCount.load(), filesTotal, blobPartIndex, bundle.blobPartCount, item.uploadName, role
                });}
            std::vector<MultipartField> fields;
            fields.push_back({"userId", identity.userId});
            fields.push_back({"agentId", identity.deviceId});
            fields.push_back({"folderName", label});
            fields.push_back({"mode", "manual"});
            fields.push_back({"backupType", backupType});
            fields.push_back({"sourcePath", state.source});
            fields.push_back({"bundleId", bundle.bundleId});
            fields.push_back({"bundleFileName", item.uploadName});
            fields.push_back({"bundleRole", role});
            const HttpResponse response = httpPostMultipartFile(
                url, fields, "file", item.path, item.uploadName, item.contentType, std::nullopt, std::nullopt, config.allowInsecureTls);
            if (response.status < 200 || response.status >= 300) {
                throw std::runtime_error(
                    "HTTP " + std::to_string(response.status) +
                    " | tentativa=" + std::to_string(attempt) +
                    " | body=" + response.body);}
            const std::size_t completed = uploadedCount.fetch_add(1) + 1;
            if (item.blobPart) uploadedBlobCount.fetch_add(1);
            if (item.manifest) result.manifestResponse = response;
            if (onProgress) {
                onProgress(UploadProgressSnapshot{
                    completed, filesTotal, blobPartIndex, bundle.blobPartCount, item.uploadName, role
                });}
        };
        const StorageArchive::CloudBundleFile* manifestFile = nullptr;
        for (const auto& item : bundle.files) {
            if (item.manifest) {
                manifestFile = &item;
                continue;}
            if (item.blobPart) continue;
            uploadOne(item, 1);
            if (!item.path.empty()) {
                std::error_code cleanupEc;
                fs::remove(item.path, cleanupEc);}}
        const std::size_t uploadWorkers = computeUploadWorkerCount(bundle.blobPartCount);
        std::cout << "[keeply][upload] workers=" << uploadWorkers
                  << " | blob_parts=" << bundle.blobPartCount
                  << " | files_total=" << filesTotal << "\n";
        std::cout.flush();
        if (onProgress && bundle.blobPartCount > 0) {
            onProgress(UploadProgressSnapshot{
                uploadedCount.load(), filesTotal, 0, bundle.blobPartCount,
                "workers=" + std::to_string(uploadWorkers), "scheduler"
            });}
        runParallelUploadQueue(
            bundle.blobPartCount,
            ParallelUploadOptions{uploadWorkers, 3, std::chrono::milliseconds(750)},
            [&](std::size_t partIndex, std::size_t attempt) {
                auto blobFile = archive.materializeCloudBundleBlob(bundle, partIndex);
                try {
                    uploadOne(blobFile, attempt);
                } catch (...) {
                    std::error_code cleanupEc;
                    if (!blobFile.path.empty()) fs::remove(blobFile.path, cleanupEc);
                    throw;}
                std::error_code cleanupEc;
                if (!blobFile.path.empty()) fs::remove(blobFile.path, cleanupEc);}
        );
        if (manifestFile) uploadOne(*manifestFile, 1);
    } catch (...) {
        std::error_code cleanupEc;
        fs::remove_all(bundle.rootDir, cleanupEc);
        throw;}
    std::error_code cleanupEc;
    fs::remove_all(bundle.rootDir, cleanupEc);
    result.filesUploaded = uploadedCount.load();
    result.blobPartCount = uploadedBlobCount.load();
    if (result.manifestResponse.status == 0) throw std::runtime_error("Manifest do bundle cloud nao foi enviado.");
    return result;}
}

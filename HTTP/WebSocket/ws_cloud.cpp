#include "ws_internal.hpp"
#include "../multithread.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>

namespace keeply::ws_internal {

namespace {

struct HttpTls {
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;
    ~HttpTls() {
        if (ssl) {
            SSL_shutdown(ssl);
            SSL_free(ssl);
        }
        if (ctx) SSL_CTX_free(ctx);
    }
};

HttpTls connectTls(int fd,
                   const ParsedUrl& url,
                   const std::optional<fs::path>& certPemPath,
                   const std::optional<fs::path>& keyPemPath,
                   bool allowInsecureTls) {
    HttpTls tls;
    tls.ctx = SSL_CTX_new(TLS_client_method());
    if (!tls.ctx) throw std::runtime_error("Falha ao criar SSL_CTX.");
    if (allowInsecureTls) {
        SSL_CTX_set_verify(tls.ctx, SSL_VERIFY_NONE, nullptr);
    } else {
        SSL_CTX_set_verify(tls.ctx, SSL_VERIFY_PEER, nullptr);
        if (SSL_CTX_set_default_verify_paths(tls.ctx) != 1) {
            throw std::runtime_error("Falha ao configurar trust store do TLS.");
        }
    }
    if (certPemPath && keyPemPath) {
        if (SSL_CTX_use_certificate_file(tls.ctx, certPemPath->c_str(), SSL_FILETYPE_PEM) != 1) {
            throw std::runtime_error("Falha ao carregar certificado do agente.");
        }
        if (SSL_CTX_use_PrivateKey_file(tls.ctx, keyPemPath->c_str(), SSL_FILETYPE_PEM) != 1) {
            throw std::runtime_error("Falha ao carregar chave privada do agente.");
        }
    }
    tls.ssl = SSL_new(tls.ctx);
    if (!tls.ssl) throw std::runtime_error("Falha ao criar SSL.");
    SSL_set_fd(tls.ssl, fd);
    SSL_set_tlsext_host_name(tls.ssl, url.host.c_str());
    if (SSL_connect(tls.ssl) != 1) throw std::runtime_error("Falha no handshake TLS com o backend.");
    return tls;
}

std::string readHttpResponseBody(int fd, SSL* ssl) {
    std::string data;
    char buf[4096];
    for (;;) {
        const std::size_t n = ssl ? readSomeSsl(ssl, buf, sizeof(buf)) : readSomeFd(fd, buf, sizeof(buf));
        if (n == 0) break;
        data.append(buf, n);
    }
    return data;
}

} // namespace

HttpResponse httpPostJson(const std::string& url,
                          const std::string& body,
                          const std::optional<fs::path>& certPemPath,
                          const std::optional<fs::path>& keyPemPath,
                          bool allowInsecureTls) {
    const ParsedUrl parsed = parseUrlCommon(url);
    if (parsed.scheme != "http" && parsed.scheme != "https") {
        throw std::runtime_error("Enroll HTTP suporta apenas http:// ou https://");
    }
    const int fd = openTcpSocket(parsed.host, parsed.port);
    HttpTls tls;
    SSL* ssl = nullptr;
    try {
        if (parsed.scheme == "https") {
            tls = connectTls(fd, parsed, certPemPath, keyPemPath, allowInsecureTls);
            ssl = tls.ssl;
        }
        std::ostringstream req;
        req << "POST " << parsed.target << " HTTP/1.1\r\n";
        req << "Host: " << parsed.host << ":" << parsed.port << "\r\n";
        req << "Content-Type: application/json\r\n";
        req << "Accept: application/json\r\n";
        req << "Connection: close\r\n";
        req << "Content-Length: " << body.size() << "\r\n\r\n";
        req << body;
        const std::string request = req.str();
        if (ssl) writeAllSsl(ssl, request.data(), request.size());
        else writeAllFd(fd, request.data(), request.size());
        std::string raw = readHttpResponseBody(fd, ssl);
        ::close(fd);
        const std::size_t headerEnd = raw.find("\r\n\r\n");
        if (headerEnd == std::string::npos) throw std::runtime_error("Resposta HTTP invalida no enroll.");
        std::istringstream hs(raw.substr(0, headerEnd));
        std::string statusLine;
        std::getline(hs, statusLine);
        std::istringstream sl(statusLine);
        std::string httpVersion;
        HttpResponse resp;
        sl >> httpVersion >> resp.status;
        resp.body = raw.substr(headerEnd + 4);
        return resp;
    } catch (...) {
        ::close(fd);
        throw;
    }
}

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
        throw std::runtime_error("Upload HTTP suporta apenas http:// ou https://");
    }
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
        contentLength += fieldParts.back().size();
    }

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

    const int fd = openTcpSocket(parsed.host, parsed.port);
    HttpTls tls;
    SSL* ssl = nullptr;
    try {
        if (parsed.scheme == "https") {
            tls = connectTls(fd, parsed, certPemPath, keyPemPath, allowInsecureTls);
            ssl = tls.ssl;
        }

        std::ostringstream req;
        req << "POST " << parsed.target << " HTTP/1.1\r\n";
        req << "Host: " << parsed.host << ":" << parsed.port << "\r\n";
        req << "Content-Type: multipart/form-data; boundary=" << boundary << "\r\n";
        req << "Accept: application/json\r\n";
        req << "Connection: close\r\n";
        req << "Content-Length: " << contentLength << "\r\n\r\n";
        const std::string headers = req.str();
        if (ssl) writeAllSsl(ssl, headers.data(), headers.size());
        else writeAllFd(fd, headers.data(), headers.size());
        for (const std::string& part : fieldParts) {
            if (ssl) writeAllSsl(ssl, part.data(), part.size());
            else writeAllFd(fd, part.data(), part.size());
        }
        if (ssl) writeAllSsl(ssl, fileHeaderStr.data(), fileHeaderStr.size());
        else writeAllFd(fd, fileHeaderStr.data(), fileHeaderStr.size());

        std::ifstream in(filePath, std::ios::binary);
        if (!in) throw std::runtime_error("Falha abrindo arquivo para upload: " + filePath.string());
        std::array<char, 64 * 1024> buf{};
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const std::streamsize got = in.gcount();
            if (got <= 0) break;
            if (ssl) writeAllSsl(ssl, buf.data(), static_cast<std::size_t>(got));
            else writeAllFd(fd, buf.data(), static_cast<std::size_t>(got));
        }
        if (!in.eof() && in.fail()) throw std::runtime_error("Falha lendo arquivo durante upload.");

        if (ssl) writeAllSsl(ssl, fileFooterStr.data(), fileFooterStr.size());
        else writeAllFd(fd, fileFooterStr.data(), fileFooterStr.size());
        std::string raw = readHttpResponseBody(fd, ssl);
        ::close(fd);
        const std::size_t headerEnd = raw.find("\r\n\r\n");
        if (headerEnd == std::string::npos) throw std::runtime_error("Resposta HTTP invalida no upload.");
        std::istringstream hs(raw.substr(0, headerEnd));
        std::string statusLine;
        std::getline(hs, statusLine);
        std::istringstream sl(statusLine);
        std::string httpVersion;
        HttpResponse resp;
        sl >> httpVersion >> resp.status;
        resp.body = raw.substr(headerEnd + 4);
        return resp;
    } catch (...) {
        ::close(fd);
        throw;
    }
}

} // namespace

std::string toLower(std::string value) {
    for (char& c : value) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return value;
}

std::string hexEncode(const unsigned char* data, std::size_t size) {
    static const char* h = "0123456789abcdef";
    std::string out;
    out.resize(size * 2);
    for (std::size_t i = 0; i < size; ++i) {
        out[i * 2] = h[(data[i] >> 4) & 0x0F];
        out[i * 2 + 1] = h[data[i] & 0x0F];
    }
    return out;
}

std::string base64Encode(const unsigned char* data, std::size_t size) {
    std::string out;
    out.resize(((size + 2) / 3) * 4);
    const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(&out[0]), data, static_cast<int>(size));
    if (written < 0) throw std::runtime_error("Falha ao codificar base64.");
    out.resize(static_cast<std::size_t>(written));
    return out;
}

std::string randomBase64(std::size_t bytes) {
    std::vector<unsigned char> raw(bytes);
    std::random_device rd;
    for (std::size_t i = 0; i < bytes; ++i) raw[i] = static_cast<unsigned char>(rd());
    return base64Encode(raw.data(), raw.size());
}

std::string randomDigits(std::size_t digits) {
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 9);
    std::string out;
    out.reserve(digits);
    for (std::size_t i = 0; i < digits; ++i) out.push_back(static_cast<char>('0' + dist(rd)));
    return out;
}

ParsedUrl parseUrlCommon(const std::string& url) {
    const std::size_t schemeSep = url.find("://");
    if (schemeSep == std::string::npos) throw std::runtime_error("URL invalida: esquema ausente.");
    ParsedUrl parsed;
    parsed.scheme = toLower(url.substr(0, schemeSep));
    const std::string rest = url.substr(schemeSep + 3);
    const std::size_t slash = rest.find('/');
    const std::string hostPort = slash == std::string::npos ? rest : rest.substr(0, slash);
    parsed.target = slash == std::string::npos ? "/" : rest.substr(slash);
    if (hostPort.empty()) throw std::runtime_error("URL invalida: host ausente.");
    const std::size_t colon = hostPort.rfind(':');
    if (colon != std::string::npos && hostPort.find(':') == colon) {
        parsed.host = hostPort.substr(0, colon);
        parsed.port = std::stoi(hostPort.substr(colon + 1));
    } else {
        parsed.host = hostPort;
        if (parsed.scheme == "ws" || parsed.scheme == "http") parsed.port = 80;
        else if (parsed.scheme == "wss" || parsed.scheme == "https") parsed.port = 443;
        else throw std::runtime_error("Esquema de URL nao suportado: " + parsed.scheme);
    }
    if (parsed.host.empty() || parsed.port <= 0 || parsed.port > 65535) throw std::runtime_error("URL invalida.");
    return parsed;
}

std::string urlEncode(const std::string& value) {
    static const char* kHex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char c : value) {
        const bool safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
                          c == '-' || c == '_' || c == '.' || c == '~';
        if (safe) out.push_back(static_cast<char>(c));
        else {
            out.push_back('%');
            out.push_back(kHex[(c >> 4) & 0x0F]);
            out.push_back(kHex[c & 0x0F]);
        }
    }
    return out;
}

std::string escapeJson(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 8);
    for (char c : value) {
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

int openTcpSocket(const std::string& host, int port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    const std::string portStr = std::to_string(port);
    const int rc = ::getaddrinfo(host.c_str(), portStr.c_str(), &hints, &result);
    if (rc != 0) throw std::runtime_error(std::string("getaddrinfo falhou: ") + gai_strerror(rc));
    int fd = -1;
    for (addrinfo* p = result; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(result);
    if (fd < 0) throw std::runtime_error("Nao foi possivel conectar ao backend.");
    return fd;
}

std::size_t writeAllFd(int fd, const void* data, std::size_t size) {
    const unsigned char* ptr = static_cast<const unsigned char*>(data);
    std::size_t offset = 0;
    while (offset < size) {
        int flags = 0;
#ifdef MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
#endif
        const ssize_t n = ::send(fd, ptr + static_cast<std::ptrdiff_t>(offset), size - offset, flags);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("send falhou: ") + std::strerror(errno));
        }
        offset += static_cast<std::size_t>(n);
    }
    return offset;
}

std::size_t writeAllSsl(void* sslHandle, const void* data, std::size_t size) {
    SSL* ssl = static_cast<SSL*>(sslHandle);
    const unsigned char* ptr = static_cast<const unsigned char*>(data);
    std::size_t offset = 0;
    while (offset < size) {
        const int n = SSL_write(ssl, ptr + static_cast<std::ptrdiff_t>(offset), static_cast<int>(size - offset));
        if (n <= 0) {
            const int err = SSL_get_error(ssl, n);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
            throw std::runtime_error("SSL_write falhou.");
        }
        offset += static_cast<std::size_t>(n);
    }
    return offset;
}

std::size_t readSomeFd(int fd, void* data, std::size_t size) {
    for (;;) {
        const ssize_t n = ::recv(fd, data, size, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("recv falhou: ") + std::strerror(errno));
        }
        return static_cast<std::size_t>(n);
    }
}

std::size_t readSomeSsl(void* sslHandle, void* data, std::size_t size) {
    SSL* ssl = static_cast<SSL*>(sslHandle);
    for (;;) {
        const int n = SSL_read(ssl, data, static_cast<int>(size));
        if (n > 0) return static_cast<std::size_t>(n);
        const int err = SSL_get_error(ssl, n);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) continue;
        if (err == SSL_ERROR_ZERO_RETURN) return 0;
        throw std::runtime_error("SSL_read falhou.");
    }
}

std::map<std::string, std::string> loadIdentityMeta(const fs::path& metaPath) {
    std::map<std::string, std::string> out;
    std::ifstream in(metaPath);
    if (!in) return out;
    std::string line;
    while (std::getline(in, line)) {
        const std::size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        out[line.substr(0, eq)] = line.substr(eq + 1);
    }
    return out;
}

void saveIdentityMeta(const AgentIdentity& identity) {
    std::ofstream out(identity.metaPath, std::ios::trunc);
    if (!out) throw std::runtime_error("Falha ao persistir metadata da identidade do agente.");
    out << "device_id=" << identity.deviceId << "\n";
    out << "user_id=" << identity.userId << "\n";
    out << "fingerprint_sha256=" << identity.fingerprintSha256 << "\n";
    out << "pairing_code=" << identity.pairingCode << "\n";
    out << "cert_pem=" << identity.certPemPath.string() << "\n";
    out << "key_pem=" << identity.keyPemPath.string() << "\n";
}

std::string extractJsonStringField(const std::string& json, const std::string& field) {
    const std::string key = "\"" + field + "\"";
    const std::size_t keyPos = json.find(key);
    if (keyPos == std::string::npos) return "";
    const std::size_t colon = json.find(':', keyPos + key.size());
    if (colon == std::string::npos) return "";
    const std::size_t q1 = json.find('\"', colon + 1);
    if (q1 == std::string::npos) return "";
    const std::size_t q2 = json.find('\"', q1 + 1);
    if (q2 == std::string::npos) return "";
    return json.substr(q1 + 1, q2 - q1 - 1);
}

std::vector<std::string> splitPipe(const std::string& value) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= value.size()) {
        const std::size_t pos = value.find('|', start);
        out.push_back(value.substr(start, pos == std::string::npos ? std::string::npos : pos - start));
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

BackupStoragePolicy parseBackupStoragePolicy(const std::string& raw) {
    BackupStoragePolicy policy;
    const std::string normalized = toLower(trim(raw));
    if (normalized == "cloud_only") {
        policy.mode = "cloud_only";
        policy.keepLocal = false;
        policy.uploadCloud = true;
        policy.deleteLocalAfterUpload = true;
        return policy;
    }
    if (normalized == "local_then_cloud") {
        policy.mode = "local_then_cloud";
        policy.keepLocal = true;
        policy.uploadCloud = true;
        policy.deleteLocalAfterUpload = false;
        return policy;
    }
    return policy;
}

void deleteLocalArchiveArtifacts(const fs::path& archivePath) {
    std::error_code ec;
    fs::remove(archivePath, ec);
    ec.clear();
    fs::path packPath = archivePath;
    packPath.replace_extension(".klyp");
    fs::remove(packPath, ec);
    ec.clear();
    fs::remove(fs::path(packPath.string() + ".idx"), ec);
}

std::string httpUrlFromWsUrl(const std::string& wsUrl, const std::string& path) {
    ParsedUrl parsed = parseUrlCommon(wsUrl);
    if (parsed.scheme == "ws") parsed.scheme = "http";
    else if (parsed.scheme == "wss") parsed.scheme = "https";
    else throw std::runtime_error("URL websocket invalida para derivar URL HTTP.");
    parsed.target = path;
    std::ostringstream oss;
    oss << parsed.scheme << "://" << parsed.host << ":" << parsed.port << parsed.target;
    return oss.str();
}

UploadBundleResult uploadArchiveBackup(const WsClientConfig& config,
                                       const AgentIdentity& identity,
                                       const AppState& state,
                                       const std::string& label,
                                       const std::function<void(const UploadProgressSnapshot&)>& onProgress) {
    if (identity.userId.empty()) throw std::runtime_error("Agente sem userId persistido. Refaça o pareamento.");
    const fs::path archivePath = fs::path(state.archive);
    if (!fs::exists(archivePath)) throw std::runtime_error("Arquivo de backup local nao encontrado para upload.");

    StorageArchive archive(archivePath);
    const auto bundle = archive.exportCloudBundle("/tmp/keeply/cloud_bundle_export", 16ull * 1024ull * 1024ull);
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
                        std::strtoull(item.uploadName.substr(marker + 1, dot - marker - 1).c_str(), nullptr, 10));
                }
            }
            if (onProgress) {
                onProgress(UploadProgressSnapshot{
                    uploadedCount.load(), filesTotal, blobPartIndex, bundle.blobPartCount, item.uploadName, role
                });
            }
            std::vector<MultipartField> fields;
            fields.push_back({"userId", identity.userId});
            fields.push_back({"agentId", identity.deviceId});
            fields.push_back({"folderName", label});
            fields.push_back({"mode", "manual"});
            fields.push_back({"sourcePath", state.source});
            fields.push_back({"bundleId", bundle.bundleId});
            fields.push_back({"bundleFileName", item.uploadName});
            fields.push_back({"objectKey", item.objectKey});
            fields.push_back({"bundleRole", role});
            const HttpResponse response = httpPostMultipartFile(
                url, fields, "file", item.path, item.uploadName, item.contentType, std::nullopt, std::nullopt, config.allowInsecureTls);
            if (response.status < 200 || response.status >= 300) {
                throw std::runtime_error(
                    "HTTP " + std::to_string(response.status) +
                    " | tentativa=" + std::to_string(attempt) +
                    " | body=" + response.body);
            }
            const std::size_t completed = uploadedCount.fetch_add(1) + 1;
            if (item.blobPart) uploadedBlobCount.fetch_add(1);
            if (item.manifest) result.manifestResponse = response;
            if (onProgress) {
                onProgress(UploadProgressSnapshot{
                    completed, filesTotal, blobPartIndex, bundle.blobPartCount, item.uploadName, role
                });
            }
        };

        const StorageArchive::CloudBundleFile* manifestFile = nullptr;
        for (const auto& item : bundle.files) {
            if (item.manifest) {
                manifestFile = &item;
                continue;
            }
            if (item.blobPart) continue;
            uploadOne(item, 1);
            if (!item.path.empty()) {
                std::error_code cleanupEc;
                fs::remove(item.path, cleanupEc);
            }
        }

        runParallelUploadQueue(
            bundle.blobPartCount,
            ParallelUploadOptions{2, 3, std::chrono::milliseconds(750)},
            [&](std::size_t partIndex, std::size_t attempt) {
                auto blobFile = archive.materializeCloudBundleBlob(bundle, partIndex);
                try {
                    uploadOne(blobFile, attempt);
                } catch (...) {
                    std::error_code cleanupEc;
                    if (!blobFile.path.empty()) fs::remove(blobFile.path, cleanupEc);
                    throw;
                }
                std::error_code cleanupEc;
                if (!blobFile.path.empty()) fs::remove(blobFile.path, cleanupEc);
            }
        );

        if (manifestFile) uploadOne(*manifestFile, 1);
    } catch (...) {
        std::error_code cleanupEc;
        fs::remove_all(bundle.rootDir, cleanupEc);
        throw;
    }
    std::error_code cleanupEc;
    fs::remove_all(bundle.rootDir, cleanupEc);
    result.filesUploaded = uploadedCount.load();
    result.blobPartCount = uploadedBlobCount.load();
    if (result.manifestResponse.status == 0) throw std::runtime_error("Manifest do bundle cloud nao foi enviado.");
    return result;
}

} // namespace keeply::ws_internal

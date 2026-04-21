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
#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
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
std::vector<std::string> extractJsonStringArrayField(const std::string& json,const std::string& field){
    const std::string needle="\""+field+"\"";
    std::size_t keyPos=json.find(needle);
    if(keyPos==std::string::npos) return {};
    std::size_t cursor=keyPos+needle.size();
    while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
    if(cursor>=json.size()||json[cursor]!=':') return {};
    ++cursor;
    while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
    if(cursor>=json.size()||json[cursor]!='[') return {};
    ++cursor;
    std::vector<std::string> out;
    std::string current;
    bool inString=false;
    bool escaped=false;
    while(cursor<json.size()){
        const char c=json[cursor++];
        if(!inString){
            if(c==']') return out;
            if(c=='"'){
                inString=true;
                current.clear();}
            continue;}
        if(escaped){
            switch(c){
                case '"': current.push_back('"'); break;
                case '\\': current.push_back('\\'); break;
                case '/': current.push_back('/'); break;
                case 'b': current.push_back('\b'); break;
                case 'f': current.push_back('\f'); break;
                case 'n': current.push_back('\n'); break;
                case 'r': current.push_back('\r'); break;
                case 't': current.push_back('\t'); break;
                case 'u': current.push_back('?'); for(int i=0;i<4&&cursor<json.size();++i,++cursor){} break;
                default: current.push_back(c); break;}
            escaped=false;
            continue;}
        if(c=='\\'){
            escaped=true;
            continue;}
        if(c=='"'){
            inString=false;
            out.push_back(current);
            continue;}
        current.push_back(c);}
    return {};}
std::map<std::string,std::string> extractJsonStringMapField(const std::string& json,const std::string& field){
    const std::string needle="\""+field+"\"";
    std::size_t keyPos=json.find(needle);
    if(keyPos==std::string::npos) return {};
    std::size_t cursor=keyPos+needle.size();
    while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
    if(cursor>=json.size()||json[cursor]!=':') return {};
    ++cursor;
    while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
    if(cursor>=json.size()||json[cursor]!='{') return {};
    ++cursor;
    std::map<std::string,std::string> out;
    auto readJsonString=[&](std::string& value)->bool{
        while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
        if(cursor>=json.size()||json[cursor]!='"') return false;
        ++cursor;
        value.clear();
        bool escaped=false;
        while(cursor<json.size()){
            const char c=json[cursor++];
            if(escaped){
                switch(c){
                    case '"': value.push_back('"'); break;
                    case '\\': value.push_back('\\'); break;
                    case '/': value.push_back('/'); break;
                    case 'b': value.push_back('\b'); break;
                    case 'f': value.push_back('\f'); break;
                    case 'n': value.push_back('\n'); break;
                    case 'r': value.push_back('\r'); break;
                    case 't': value.push_back('\t'); break;
                    case 'u': value.push_back('?'); for(int i=0;i<4&&cursor<json.size();++i,++cursor){} break;
                    default: value.push_back(c); break;}
                escaped=false;
                continue;}
            if(c=='\\'){
                escaped=true;
                continue;}
            if(c=='"') return true;
            value.push_back(c);}
        return false;};
    while(cursor<json.size()){
        while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
        if(cursor<json.size()&&json[cursor]=='}') return out;
        std::string key;
        if(!readJsonString(key)) return {};
        while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
        if(cursor>=json.size()||json[cursor]!=':') return {};
        ++cursor;
        std::string value;
        if(!readJsonString(value)) return {};
        out[key]=value;
        while(cursor<json.size()&&std::isspace(static_cast<unsigned char>(json[cursor]))) ++cursor;
        if(cursor<json.size()&&json[cursor]==','){++cursor;continue;}
        if(cursor<json.size()&&json[cursor]=='}') return out;}
    return {};}
fs::path writeTempUploadFile(const std::string& prefix,const std::string& suffix,const unsigned char* data,std::size_t size){
    const fs::path dir=defaultKeeplyTempDir()/"cloud_upload";
    std::error_code ec;
    fs::create_directories(dir,ec);
    if(ec) throw std::runtime_error("Falha criando diretorio temporario de upload: "+ec.message());
    const fs::path path=dir/(prefix+"-"+randomDigits(12)+suffix);
    std::ofstream out(path,std::ios::binary|std::ios::trunc);
    if(!out) throw std::runtime_error("Falha criando arquivo temporario de upload: "+path.string());
    if(size>0) out.write(reinterpret_cast<const char*>(data),static_cast<std::streamsize>(size));
    out.flush();
    if(!out) throw std::runtime_error("Falha escrevendo arquivo temporario de upload: "+path.string());
    return path;}
fs::path writeTempUploadFile(const std::string& prefix,const std::string& suffix,const std::string& content){
    return writeTempUploadFile(prefix,suffix,reinterpret_cast<const unsigned char*>(content.data()),content.size());}
std::string jsonStringArray(const std::vector<std::string>& values){
    std::ostringstream oss;
    oss<<"[";
    for(std::size_t i=0;i<values.size();++i){
        if(i>0) oss<<",";
        oss<<"\""<<escapeJson(values[i])<<"\"";}
    oss<<"]";
    return oss.str();}
std::vector<std::string> requestMissingHashes(const std::string& url,
                                              const AgentIdentity& identity,
                                              const AppState& state,
                                              const std::string& label,
                                              const std::string& bundleId,
                                              sqlite3_int64 latestSnapshotId,
                                              const std::vector<std::string>& hashes,
                                              bool allowInsecureTls){
    if(hashes.empty()) return {};
    constexpr std::size_t kBatchSize=1000;
    std::vector<std::string> missingAll;
    for(std::size_t offset=0;offset<hashes.size();offset+=kBatchSize){
        const std::size_t end=std::min(offset+kBatchSize,hashes.size());
        const std::vector<std::string> batch(hashes.begin()+offset,hashes.begin()+end);
        std::ostringstream body;
        body<<"{"<<"\"userId\":\""<<escapeJson(identity.userId)<<"\","<<"\"agentId\":\""<<escapeJson(identity.deviceId)<<"\","<<"\"folderName\":\""<<escapeJson(label)<<"\","<<"\"sourcePath\":\""<<escapeJson(state.source)<<"\","<<"\"bundleId\":\""<<escapeJson(bundleId)<<"\","<<"\"snapshotId\":"<<latestSnapshotId<<","<<"\"hashes\":"<<jsonStringArray(batch)<<"}";
        const HttpResponse response=httpPostJson(url,body.str(),std::nullopt,std::nullopt,allowInsecureTls);
        if(response.status==404){
            missingAll.insert(missingAll.end(),batch.begin(),batch.end());
            continue;}
        if(response.status<200||response.status>=300) throw std::runtime_error("HTTP "+std::to_string(response.status)+" | body="+response.body);
        auto missing=extractJsonStringArrayField(response.body,"missingHashes");
        if(missing.empty()) missing=extractJsonStringArrayField(response.body,"missing");
        if(missing.empty()) missing=extractJsonStringArrayField(response.body,"hashes");
        missingAll.insert(missingAll.end(),missing.begin(),missing.end());}
    return missingAll;}
std::map<std::string,std::string> requestChunkKeys(const std::string& url,
                                                   const AgentIdentity& identity,
                                                   const AppState& state,
                                                   const std::string& label,
                                                   const std::string& bundleId,
                                                   sqlite3_int64 latestSnapshotId,
                                                   const std::vector<std::string>& hashes,
                                                   bool allowInsecureTls){
    if(hashes.empty()) return {};
    constexpr std::size_t kBatchSize=1000;
    std::map<std::string,std::string> outAll;
    for(std::size_t offset=0;offset<hashes.size();offset+=kBatchSize){
        const std::size_t end=std::min(offset+kBatchSize,hashes.size());
        const std::vector<std::string> batch(hashes.begin()+offset,hashes.begin()+end);
        std::ostringstream body;
        body<<"{"<<"\"userId\":\""<<escapeJson(identity.userId)<<"\","<<"\"agentId\":\""<<escapeJson(identity.deviceId)<<"\","<<"\"folderName\":\""<<escapeJson(label)<<"\","<<"\"sourcePath\":\""<<escapeJson(state.source)<<"\","<<"\"bundleId\":\""<<escapeJson(bundleId)<<"\","<<"\"snapshotId\":"<<latestSnapshotId<<","<<"\"hashes\":"<<jsonStringArray(batch)<<"}";
        const HttpResponse response=httpPostJson(url,body.str(),std::nullopt,std::nullopt,allowInsecureTls);
        if(response.status==404) continue;
        if(response.status<200||response.status>=300) throw std::runtime_error("HTTP "+std::to_string(response.status)+" | body="+response.body);
        auto out=extractJsonStringMapField(response.body,"keys");
        if(out.empty()) out=extractJsonStringMapField(response.body,"chunkKeys");
        if(out.empty()) out=extractJsonStringMapField(response.body,"s3Keys");
        for(auto& kv:out) outAll.insert(std::move(kv));}
    return outAll;}
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
    const auto plan=archive.prepareCloudUpload(identity.deviceId);
    const auto snapshots = archive.listSnapshots();
    std::string backupType = snapshots.empty() ? "full" : (snapshots.back().backupType == "incremental" ? "incremental" : "full");
    const std::string url = httpUrlFromWsUrl(config.url, "/api/agent/backups/upload");
    const std::string missingHashesUrl=httpUrlFromWsUrl(config.url,"/api/agent/backups/missing-hashes");
    const std::string chunkKeysUrl=httpUrlFromWsUrl(config.url,"/api/agent/backups/chunk-keys");
    UploadBundleResult result;
    result.bundleId = plan.bundleId;
    std::vector<std::string> allHashes;
    allHashes.reserve(plan.chunks.size());
    for(const auto& chunk:plan.chunks) allHashes.push_back(hexEncode(chunk.hash.data(),chunk.hash.size()));
    const std::vector<std::string> missingHashes=requestMissingHashes(missingHashesUrl,identity,state,label,plan.bundleId,plan.latestSnapshotId,allHashes,config.allowInsecureTls);
    const std::set<std::string> missingSet(missingHashes.begin(),missingHashes.end());
    const auto chunkKeys=requestChunkKeys(chunkKeysUrl,identity,state,label,plan.bundleId,plan.latestSnapshotId,missingHashes,config.allowInsecureTls);
    std::vector<StorageArchive::CloudChunkRef> missingChunks;
    missingChunks.reserve(missingHashes.size());
    for(const auto& chunk:plan.chunks){
        const std::string hashHex=hexEncode(chunk.hash.data(),chunk.hash.size());
        if(missingSet.find(hashHex)!=missingSet.end()) missingChunks.push_back(chunk);}
    const std::size_t filesTotal=missingChunks.size()+2;
    std::atomic<std::size_t> uploadedCount{0};
    std::atomic<std::size_t> uploadedBlobCount{0};
    try {
        auto uploadFile=[&](const fs::path& filePath,
                            const std::string& uploadName,
                            const std::string& role,
                            std::size_t blobPartIndex,
                            const std::vector<MultipartField>& extraFields,
                            std::size_t attempt,
                            const std::string& contentType){
            if(onProgress){
                onProgress(UploadProgressSnapshot{
                    uploadedCount.load(),filesTotal,blobPartIndex,missingChunks.size(),uploadName,role
                });}
            std::vector<MultipartField> fields;
            fields.push_back({"userId",identity.userId});
            fields.push_back({"agentId",identity.deviceId});
            fields.push_back({"folderName",label});
            fields.push_back({"mode","manual"});
            fields.push_back({"backupType",backupType});
            fields.push_back({"sourcePath",state.source});
            fields.push_back({"bundleId",plan.bundleId});
            fields.push_back({"bundleFileName",uploadName});
            fields.push_back({"bundleRole",role});
            for(const auto& field:extraFields) fields.push_back(field);
            const HttpResponse response=httpPostMultipartFile(url,fields,"file",filePath,uploadName,contentType,std::nullopt,std::nullopt,config.allowInsecureTls);
            if(response.status<200||response.status>=300){
                throw std::runtime_error("HTTP "+std::to_string(response.status)+" | tentativa="+std::to_string(attempt)+" | body="+response.body);}
            const std::size_t completed=uploadedCount.fetch_add(1)+1;
            if(role=="chunk") uploadedBlobCount.fetch_add(1);
            if(role=="manifest") result.manifestResponse=response;
            if(onProgress){
                onProgress(UploadProgressSnapshot{
                    completed,filesTotal,blobPartIndex,missingChunks.size(),uploadName,role
                });}};
        uploadFile(plan.archiveDbPath,plan.archiveDbPath.filename().string(),"archive",0,{{"snapshotId",std::to_string(plan.latestSnapshotId)}},1,"application/octet-stream");
        const std::size_t uploadWorkers=computeUploadWorkerCount(missingChunks.size());
        std::cout << "[keeply][upload] workers=" << uploadWorkers
                  << " | missing_chunks=" << missingChunks.size()
                  << " | total_chunks=" << plan.chunks.size()
                  << " | files_total=" << filesTotal << "\n";
        std::cout.flush();
        if(onProgress&&missingChunks.size()>0){
            onProgress(UploadProgressSnapshot{
                uploadedCount.load(),filesTotal,0,missingChunks.size(),
                "workers=" + std::to_string(uploadWorkers), "scheduler"
            });}
        runParallelUploadQueue(
            missingChunks.size(),
            ParallelUploadOptions{uploadWorkers==0?1:uploadWorkers,3,std::chrono::milliseconds(750)},
            [&](std::size_t partIndex,std::size_t attempt){
                const auto& chunk=missingChunks[partIndex];
                const std::string hashHex=hexEncode(chunk.hash.data(),chunk.hash.size());
                const auto data=archive.readPackRecord(chunk);
                const fs::path chunkPath=writeTempUploadFile("chunk-"+hashHex,".bin",data.data(),data.size());
                try {
                    std::vector<MultipartField> extraFields;
                    extraFields.push_back({"chunkHash",hashHex});
                    extraFields.push_back({"chunkAlgo","pack-record"});
                    extraFields.push_back({"snapshotId",std::to_string(plan.latestSnapshotId)});
                    const auto keyIt=chunkKeys.find(hashHex);
                    if(keyIt!=chunkKeys.end()&&!keyIt->second.empty()) extraFields.push_back({"objectKey",keyIt->second});
                    uploadFile(chunkPath,hashHex+".bin","chunk",partIndex+1,extraFields,attempt,"application/octet-stream");
                } catch (...) {
                    std::error_code cleanupEc;
                    fs::remove(chunkPath,cleanupEc);
                    throw;}
                std::error_code cleanupEc;
                fs::remove(chunkPath,cleanupEc);}
        );
        const std::size_t latestSnapshotChunkCount=plan.latestSnapshotId>0?archive.snapshotChunkHashes(plan.latestSnapshotId).size():0;
        std::ostringstream manifestJson;
        manifestJson<<"{"<<"\"bundleId\":\""<<escapeJson(plan.bundleId)<<"\","<<"\"snapshotId\":"<<plan.latestSnapshotId<<","<<"\"sourcePath\":\""<<escapeJson(state.source)<<"\","<<"\"backupType\":\""<<escapeJson(backupType)<<"\","<<"\"archiveFile\":\""<<escapeJson(plan.archiveDbPath.filename().string())<<"\","<<"\"totalChunks\":"<<plan.chunks.size()<<","<<"\"latestSnapshotChunkCount\":"<<latestSnapshotChunkCount<<","<<"\"missingChunksUploaded\":"<<missingChunks.size()<<"}";
        const fs::path manifestPath=writeTempUploadFile("manifest-"+plan.bundleId,".json",manifestJson.str());
        try{
            uploadFile(manifestPath,"manifest.json","manifest",0,{{"snapshotId",std::to_string(plan.latestSnapshotId)}},1,"application/json");
        }catch(...){
            std::error_code cleanupEc;
            fs::remove(manifestPath,cleanupEc);
            throw;}
        std::error_code manifestCleanupEc;
        fs::remove(manifestPath,manifestCleanupEc);
    } catch (...) {
        throw;}
    result.filesUploaded = uploadedCount.load();
    result.blobPartCount = uploadedBlobCount.load();
    if (result.manifestResponse.status == 0) throw std::runtime_error("Manifest do bundle cloud nao foi enviado.");
    return result;}
}

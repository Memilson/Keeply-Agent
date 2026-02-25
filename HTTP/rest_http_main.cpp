#include "ApiRest/api-rest.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace {

using keeply::KeeplyApi;
using keeply::KeeplyRestApi;
using keeply::RestRequest;
using keeply::RestResponse;

std::string trim(const std::string& s) {
    std::size_t b = 0;
    while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
    std::size_t e = s.size();
    while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
    return s.substr(b, e - b);
}

std::string toLower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

int fromHex(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

std::string urlDecode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (std::size_t i = 0; i < s.size(); ++i) {
        const char c = s[i];
        if (c == '+') {
            out.push_back(' ');
            continue;
        }
        if (c == '%' && i + 2 < s.size()) {
            const int hi = fromHex(s[i + 1]);
            const int lo = fromHex(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
                continue;
            }
        }
        out.push_back(c);
    }
    return out;
}

void parseQuery(const std::string& q, std::unordered_map<std::string, std::string>& out) {
    std::size_t pos = 0;
    while (pos <= q.size()) {
        const std::size_t amp = q.find('&', pos);
        const std::string part = q.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
        if (!part.empty()) {
            const std::size_t eq = part.find('=');
            if (eq == std::string::npos) {
                out[urlDecode(part)] = "";
            } else {
                out[urlDecode(part.substr(0, eq))] = urlDecode(part.substr(eq + 1));
            }
        }
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
}

bool recvHttpRequest(int fd, RestRequest& req) {
    std::string buf;
    buf.reserve(8192);
    char tmp[4096];

    std::size_t headerEnd = std::string::npos;
    while (true) {
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n == 0) return false;
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("recv falhou: ") + std::strerror(errno));
        }
        buf.append(tmp, static_cast<std::size_t>(n));
        headerEnd = buf.find("\r\n\r\n");
        if (headerEnd != std::string::npos) break;
        if (buf.size() > (1u << 20)) {
            throw std::runtime_error("Cabecalho HTTP muito grande.");
        }
    }

    const std::string headerBlock = buf.substr(0, headerEnd);
    std::istringstream hs(headerBlock);

    std::string requestLine;
    if (!std::getline(hs, requestLine)) {
        throw std::runtime_error("Request line ausente.");
    }
    if (!requestLine.empty() && requestLine.back() == '\r') requestLine.pop_back();

    std::istringstream rl(requestLine);
    std::string target;
    std::string httpVersion;
    if (!(rl >> req.method >> target >> httpVersion)) {
        throw std::runtime_error("Request line invalida.");
    }

    std::string line;
    req.headers.clear();
    while (std::getline(hs, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = toLower(trim(line.substr(0, colon)));
        std::string value = trim(line.substr(colon + 1));
        req.headers[key] = value;
    }

    req.query.clear();
    const std::size_t qpos = target.find('?');
    if (qpos == std::string::npos) {
        req.path = target;
    } else {
        req.path = target.substr(0, qpos);
        parseQuery(target.substr(qpos + 1), req.query);
    }

    std::size_t contentLen = 0;
    auto itCl = req.headers.find("content-length");
    if (itCl != req.headers.end()) {
        try {
            contentLen = static_cast<std::size_t>(std::stoull(itCl->second));
        } catch (...) {
            throw std::runtime_error("Content-Length invalido.");
        }
    }

    const std::size_t bodyStart = headerEnd + 4;
    while (buf.size() < bodyStart + contentLen) {
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n == 0) break;
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("recv body falhou: ") + std::strerror(errno));
        }
        buf.append(tmp, static_cast<std::size_t>(n));
    }
    if (buf.size() < bodyStart + contentLen) {
        throw std::runtime_error("Corpo HTTP truncado.");
    }
    req.body.assign(buf.data() + static_cast<std::ptrdiff_t>(bodyStart), contentLen);

    return true;
}

std::string reasonPhrase(int status) {
    switch (status) {
        case 200: return "OK";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 500: return "Internal Server Error";
        default: return "OK";
    }
}

void sendAll(int fd, const std::string& data) {
    std::size_t off = 0;
    while (off < data.size()) {
        const ssize_t n = ::send(fd, data.data() + static_cast<std::ptrdiff_t>(off), data.size() - off, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("send falhou: ") + std::strerror(errno));
        }
        off += static_cast<std::size_t>(n);
    }
}

void sendHttpResponse(int fd, const RestResponse& resp) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << resp.status << " " << reasonPhrase(resp.status) << "\r\n";
    oss << "Content-Type: " << resp.contentType << "\r\n";
    oss << "Content-Length: " << resp.body.size() << "\r\n";
    oss << "Connection: close\r\n";
    oss << "\r\n";
    oss << resp.body;
    sendAll(fd, oss.str());
}

void sendHttpError(int fd, int status, const std::string& msg) {
    RestResponse r;
    r.status = status;
    r.body = std::string("{\"ok\":false,\"error\":\"") + msg + "\"}";
    sendHttpResponse(fd, r);
}

int createListenSocket(int port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error(std::string("socket falhou: ") + std::strerror(errno));

    int one = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        const std::string err = std::strerror(errno);
        ::close(fd);
        throw std::runtime_error("bind falhou: " + err);
    }
    if (::listen(fd, 32) < 0) {
        const std::string err = std::strerror(errno);
        ::close(fd);
        throw std::runtime_error("listen falhou: " + err);
    }
    return fd;
}

} // namespace

int main(int argc, char** argv) {
    try {
        int port = 8080;
        if (argc >= 2) {
            port = std::atoi(argv[1]);
            if (port <= 0 || port > 65535) throw std::runtime_error("Porta invalida.");
        }

        auto api = std::make_shared<KeeplyApi>();
        namespace fs = std::filesystem;
        const fs::path buildDir = fs::current_path() / "build";
        fs::create_directories(buildDir);
        api->setArchive((buildDir / "keeply_http_api.db").string());
        api->setRestoreRoot((buildDir / "restore_http_api").string());
        KeeplyRestApi rest(api);
        const int listenFd = createListenSocket(port);

        std::cout << "Keeply REST HTTP listening on http://127.0.0.1:" << port << "\n";
        std::cout << "Archive: " << api->state().archive << "\n";
        std::cout << "RestoreRoot: " << api->state().restoreRoot << "\n";
        std::cout.flush();

        for (;;) {
            sockaddr_in cli{};
            socklen_t cliLen = sizeof(cli);
            const int cfd = ::accept(listenFd, reinterpret_cast<sockaddr*>(&cli), &cliLen);
            if (cfd < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("accept falhou: ") + std::strerror(errno));
            }

            try {
                RestRequest req;
                if (recvHttpRequest(cfd, req)) {
                    std::cout << req.method << " " << req.path << "\n";
                    const RestResponse resp = rest.handle(req);
                    sendHttpResponse(cfd, resp);
                }
            } catch (const std::exception& e) {
                try { sendHttpError(cfd, 500, e.what()); } catch (...) {}
            } catch (...) {
                try { sendHttpError(cfd, 500, "erro interno"); } catch (...) {}
            }

            ::close(cfd);
        }
    } catch (const std::exception& e) {
        std::cerr << "REST HTTP server falhou: " << e.what() << "\n";
        return 1;
    }
}

#include "http_util.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <cctype>
#include <stdexcept>

namespace keeply::http_internal {

namespace {

#ifdef _WIN32
struct WinsockInit {
    WinsockInit() {
        WSADATA data{};
        if (WSAStartup(MAKEWORD(2, 2), &data) != 0) {
            throw std::runtime_error("WSAStartup falhou.");
        }
    }
    ~WinsockInit() {
        WSACleanup();
    }
};
#endif

}

std::string toLower(std::string value) {
    for (char& c : value) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return value;
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

void ensureSocketRuntime() {
#ifdef _WIN32
    static WinsockInit init;
    (void)init;
#endif
}

int closeSocketFd(int fd) {
#ifdef _WIN32
    return closesocket(static_cast<SOCKET>(fd));
#else
    return ::close(fd);
#endif
}

int lastSocketError() {
#ifdef _WIN32
    return WSAGetLastError();
#else
    return errno;
#endif
}

bool socketInterrupted(int err) {
#ifdef _WIN32
    return err == WSAEINTR;
#else
    return err == EINTR;
#endif
}

bool socketTimeoutOrWouldBlock(int err) {
#ifdef _WIN32
    return err == WSAETIMEDOUT || err == WSAEWOULDBLOCK;
#else
    return err == EAGAIN || err == EWOULDBLOCK;
#endif
}

std::string socketErrorMessage(int err) {
#ifdef _WIN32
    char* buffer = nullptr;
    const DWORD flags = FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
    const DWORD len = FormatMessageA(flags, nullptr, static_cast<DWORD>(err),
                                     MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                                     reinterpret_cast<LPSTR>(&buffer), 0, nullptr);
    std::string out = (len && buffer) ? std::string(buffer, len) : ("WSA error " + std::to_string(err));
    if (buffer) LocalFree(buffer);
    while (!out.empty() && (out.back() == '\r' || out.back() == '\n' || out.back() == ' ')) out.pop_back();
    return out;
#else
    return std::strerror(err);
#endif
}

int openTcpSocket(const std::string& host, int port) {
    ensureSocketRuntime();
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
        if (::connect(fd, p->ai_addr, static_cast<int>(p->ai_addrlen)) == 0) break;
        closeSocketFd(fd);
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
        const auto n = ::send(fd,
                              reinterpret_cast<const char*>(ptr + static_cast<std::ptrdiff_t>(offset)),
                              static_cast<int>(size - offset),
                              flags);
        if (n < 0) {
            const int err = lastSocketError();
            if (socketInterrupted(err)) continue;
            throw std::runtime_error(std::string("send falhou: ") + socketErrorMessage(err));
        }
        offset += static_cast<std::size_t>(n);
    }
    return offset;
}

std::size_t readSomeFd(int fd, void* data, std::size_t size) {
    for (;;) {
        const auto n = ::recv(fd, reinterpret_cast<char*>(data), static_cast<int>(size), 0);
        if (n < 0) {
            const int err = lastSocketError();
            if (socketInterrupted(err)) continue;
            throw std::runtime_error(std::string("recv falhou: ") + socketErrorMessage(err));
        }
        return static_cast<std::size_t>(n);
    }
}

}

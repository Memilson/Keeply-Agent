#pragma once

#include <cstddef>
#include <map>
#include <string>

namespace keeply::http_internal {

struct ParsedUrl {
    std::string scheme;
    std::string host;
    int port = 0;
    std::string target = "/";
};

struct HttpResponse {
    int status = 0;
    std::string body;
    std::map<std::string, std::string> headers;
};

struct MultipartField {
    std::string name;
    std::string value;
};

[[nodiscard]] std::string toLower(std::string value);
[[nodiscard]] std::string escapeJson(const std::string& value);
[[nodiscard]] ParsedUrl parseUrlCommon(const std::string& url);
[[nodiscard]] std::string urlEncode(const std::string& value);

void ensureSocketRuntime();
int closeSocketFd(int fd);
[[nodiscard]] int lastSocketError();
[[nodiscard]] bool socketInterrupted(int err);
[[nodiscard]] bool socketTimeoutOrWouldBlock(int err);
[[nodiscard]] std::string socketErrorMessage(int err);
[[nodiscard]] int openTcpSocket(const std::string& host, int port);
[[nodiscard]] std::size_t writeAllFd(int fd, const void* data, std::size_t size);
[[nodiscard]] std::size_t readSomeFd(int fd, void* data, std::size_t size);

} 

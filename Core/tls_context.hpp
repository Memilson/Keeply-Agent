#pragma once
#include <openssl/ssl.h>
#include <filesystem>
#include <optional>
#include <string>
namespace keeply {
namespace fs = std::filesystem;
struct TlsContextConfig {
    std::string host;
    bool allowInsecure = false;
    std::optional<fs::path> certPemPath;
    std::optional<fs::path> keyPemPath;};
SSL* createConnectedTlsSession(
    int fd,
    const TlsContextConfig& config,
    SSL_CTX*& ctxOut);
}

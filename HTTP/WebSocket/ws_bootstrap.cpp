#include "ws_internal.hpp"

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#include <openssl/x509.h>

#include <cstdio>
#include <iostream>
#include <system_error>
#include <thread>

#if defined(__linux__) || defined(__APPLE__)
#include <sys/stat.h>
#endif

namespace keeply {

namespace {

using namespace ws_internal;

struct PairingStatusResponse {
    std::string status;
    std::string deviceId;
    std::string code;
    std::string userId;
};

void tightenIdentityPathPermissions(const fs::path& path, bool executable) {
#if defined(__linux__) || defined(__APPLE__)
    const mode_t mode = executable ? static_cast<mode_t>(0700) : static_cast<mode_t>(0600);
    if (::chmod(path.c_str(), mode) != 0) {
        throw std::runtime_error("Falha ao ajustar permissoes de " + path.string());
    }
#else
    (void)path;
    (void)executable;
#endif
}

std::string fingerprintFromX509(X509* cert) {
    int derLen = i2d_X509(cert, nullptr);
    if (derLen <= 0) throw std::runtime_error("Falha ao serializar certificado DER.");
    std::vector<unsigned char> der(static_cast<std::size_t>(derLen));
    unsigned char* ptr = der.data();
    if (i2d_X509(cert, &ptr) != derLen) throw std::runtime_error("Falha ao serializar certificado DER.");
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(der.data(), der.size(), digest);
    return hexEncode(digest, sizeof(digest));
}

std::string fingerprintFromPemFile(const fs::path& certPemPath) {
    FILE* fp = fopenPath(certPemPath, "rb");
    if (!fp) throw std::runtime_error("Falha ao abrir certificado do agente.");
    X509* cert = PEM_read_X509(fp, nullptr, nullptr, nullptr);
    std::fclose(fp);
    if (!cert) throw std::runtime_error("Falha ao ler certificado PEM do agente.");
    const std::string fpHex = fingerprintFromX509(cert);
    X509_free(cert);
    return fpHex;
}

AgentIdentity generateSelfSignedIdentity(const WsClientConfig& config) {
    AgentIdentity identity;
    identity.certPemPath = config.identityDir / "agent-cert.pem";
    identity.keyPemPath = config.identityDir / "agent-key.pem";
    identity.metaPath = config.identityDir / "identity.meta";
    std::error_code dirEc;
    fs::create_directories(config.identityDir, dirEc);
    if (dirEc) throw std::runtime_error("Falha ao criar diretorio de identidade: " + dirEc.message());
    tightenIdentityPathPermissions(config.identityDir, true);
    EVP_PKEY_CTX* pkeyCtx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
    if (!pkeyCtx) throw std::runtime_error("Falha ao criar contexto da chave.");
    EVP_PKEY* pkey = nullptr;
    X509* cert = nullptr;
    try {
        if (EVP_PKEY_keygen_init(pkeyCtx) <= 0) throw std::runtime_error("Falha ao inicializar keygen.");
        if (EVP_PKEY_CTX_set_rsa_keygen_bits(pkeyCtx, 2048) <= 0) throw std::runtime_error("Falha ao configurar bits da chave.");
        if (EVP_PKEY_keygen(pkeyCtx, &pkey) <= 0) throw std::runtime_error("Falha ao gerar chave privada.");
        EVP_PKEY_CTX_free(pkeyCtx);
        pkeyCtx = nullptr;
        cert = X509_new();
        if (!cert) throw std::runtime_error("Falha ao criar certificado X509.");
        X509_set_version(cert, 2);
        ASN1_INTEGER_set(X509_get_serialNumber(cert), static_cast<long>(std::time(nullptr)));
        X509_gmtime_adj(X509_get_notBefore(cert), 0);
        X509_gmtime_adj(X509_get_notAfter(cert), 60L * 60L * 24L * 365L * 5L);
        X509_set_pubkey(cert, pkey);
        X509_NAME* name = X509_get_subject_name(cert);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char*>(config.deviceName.c_str()),
                                   -1, -1, 0);
        X509_set_issuer_name(cert, name);
        if (X509_sign(cert, pkey, EVP_sha256()) <= 0) throw std::runtime_error("Falha ao assinar certificado do agente.");
        FILE* keyFp = fopenPath(identity.keyPemPath, "wb");
        if (!keyFp) throw std::runtime_error("Falha ao criar chave PEM do agente.");
        if (PEM_write_PrivateKey(keyFp, pkey, nullptr, nullptr, 0, nullptr, nullptr) != 1) {
            std::fclose(keyFp);
            throw std::runtime_error("Falha ao salvar chave PEM do agente.");
        }
        std::fclose(keyFp);
        tightenIdentityPathPermissions(identity.keyPemPath, false);
        FILE* certFp = fopenPath(identity.certPemPath, "wb");
        if (!certFp) throw std::runtime_error("Falha ao criar cert PEM do agente.");
        if (PEM_write_X509(certFp, cert) != 1) {
            std::fclose(certFp);
            throw std::runtime_error("Falha ao salvar cert PEM do agente.");
        }
        std::fclose(certFp);
        tightenIdentityPathPermissions(identity.certPemPath, false);
        identity.fingerprintSha256 = fingerprintFromX509(cert);
        X509_free(cert);
        EVP_PKEY_free(pkey);
        return identity;
    } catch (...) {
        if (cert) X509_free(cert);
        if (pkey) EVP_PKEY_free(pkey);
        if (pkeyCtx) EVP_PKEY_CTX_free(pkeyCtx);
        throw;
    }
}

PairingStatusResponse parsePairingStatusResponse(const ws_internal::HttpResponse& response) {
    PairingStatusResponse out;
    out.status = trim(extractJsonStringField(response.body, "status"));
    out.deviceId = trim(extractJsonStringField(response.body, "deviceId"));
    out.code = trim(extractJsonStringField(response.body, "code"));
    out.userId = trim(extractJsonStringField(response.body, "userId"));
    return out;
}

PairingStatusResponse startPairing(const WsClientConfig& config, const AgentIdentity& identity, const std::string& code) {
    const std::string url = httpUrlFromWsUrl(config.url, "/api/devices/pairing/start");
    const std::string body =
        std::string("{\"code\":\"") + escapeJson(code) +
        "\",\"deviceName\":\"" + escapeJson(config.deviceName) +
        "\",\"hostName\":\"" + escapeJson(config.hostName) +
        "\",\"os\":\"" + escapeJson(config.osName) +
        "\",\"certFingerprintSha256\":\"" + escapeJson(identity.fingerprintSha256) + "\"}";
    const auto response = httpPostJson(url, body, std::nullopt, std::nullopt, config.allowInsecureTls);
    if (response.status == 409) return PairingStatusResponse{"code_conflict", "", "", ""};
    if (response.status < 200 || response.status >= 300) {
        throw std::runtime_error("Falha ao iniciar pareamento do agente. HTTP " +
                                 std::to_string(response.status) + " | body=" + response.body);
    }
    return parsePairingStatusResponse(response);
}

PairingStatusResponse pollPairingStatus(const WsClientConfig& config, const AgentIdentity& identity, const std::string& code) {
    const std::string url = httpUrlFromWsUrl(config.url, "/api/devices/pairing/status");
    const std::string body =
        std::string("{\"code\":\"") + escapeJson(code) +
        "\",\"certFingerprintSha256\":\"" + escapeJson(identity.fingerprintSha256) + "\"}";
    const auto response = httpPostJson(url, body, std::nullopt, std::nullopt, config.allowInsecureTls);
    if (response.status < 200 || response.status >= 300) {
        throw std::runtime_error("Falha ao consultar status do pareamento. HTTP " +
                                 std::to_string(response.status) + " | body=" + response.body);
    }
    return parsePairingStatusResponse(response);
}

} // namespace

AgentIdentity KeeplyAgentBootstrap::ensureRegistered(const WsClientConfig& config) {
    if (trim(config.url).empty()) throw std::runtime_error("URL do backend websocket nao pode ser vazia.");
    fs::create_directories(config.identityDir);
    tightenIdentityPathPermissions(config.identityDir, true);
    AgentIdentity identity;
    identity.metaPath = config.identityDir / "identity.meta";
    const auto meta = ws_internal::loadIdentityMeta(identity.metaPath);
    auto itDeviceId = meta.find("device_id");
    if (itDeviceId != meta.end()) identity.deviceId = trim(itDeviceId->second);
    auto itUserId = meta.find("user_id");
    if (itUserId != meta.end()) identity.userId = trim(itUserId->second);
    auto itFp = meta.find("fingerprint_sha256");
    if (itFp != meta.end()) identity.fingerprintSha256 = trim(itFp->second);
    auto itPairingCode = meta.find("pairing_code");
    if (itPairingCode != meta.end()) identity.pairingCode = trim(itPairingCode->second);
    auto itCert = meta.find("cert_pem");
    auto itKey = meta.find("key_pem");
    identity.certPemPath = (itCert != meta.end() && !trim(itCert->second).empty())
        ? pathFromUtf8(trim(itCert->second))
        : (config.identityDir / "agent-cert.pem");
    identity.keyPemPath = (itKey != meta.end() && !trim(itKey->second).empty())
        ? pathFromUtf8(trim(itKey->second))
        : (config.identityDir / "agent-key.pem");
    if (!fs::exists(identity.certPemPath) || !fs::exists(identity.keyPemPath)) {
        identity = generateSelfSignedIdentity(config);
    } else if (identity.fingerprintSha256.empty()) {
        identity.fingerprintSha256 = fingerprintFromPemFile(identity.certPemPath);
    }
    if (fs::exists(identity.certPemPath)) tightenIdentityPathPermissions(identity.certPemPath, false);
    if (fs::exists(identity.keyPemPath)) tightenIdentityPathPermissions(identity.keyPemPath, false);
    if (fs::exists(identity.metaPath)) tightenIdentityPathPermissions(identity.metaPath, false);
    for (;;) {
        if (identity.pairingCode.empty()) identity.pairingCode = trim(config.pairingCode);
        if (identity.pairingCode.empty()) identity.pairingCode = ws_internal::randomDigits(8);
        PairingStatusResponse started = startPairing(config, identity, identity.pairingCode);
        if (started.status == "code_conflict") {
            identity.pairingCode.clear();
            continue;
        }
        if (!started.code.empty()) identity.pairingCode = started.code;
        if (started.status == "active") {
            identity.deviceId = started.deviceId;
            identity.userId = started.userId;
            identity.pairingCode.clear();
            break;
        }
        ws_internal::saveIdentityMeta(identity);
        std::cout << "Codigo de ativacao Keeply: " << identity.pairingCode
                  << " | dispositivo: " << config.deviceName
                  << " | host: " << config.hostName << "\n";
        for (;;) {
            std::this_thread::sleep_for(std::chrono::milliseconds(std::max(1000, config.pairingPollIntervalMs)));
            PairingStatusResponse status = pollPairingStatus(config, identity, identity.pairingCode);
            if (status.status == "active") {
                identity.deviceId = status.deviceId;
                identity.userId = status.userId;
                identity.pairingCode.clear();
                break;
            }
            if (status.status == "expired" || status.status == "missing") {
                identity.pairingCode.clear();
                break;
            }
        }
        if (!identity.deviceId.empty()) break;
    }
    ws_internal::saveIdentityMeta(identity);
    tightenIdentityPathPermissions(identity.metaPath, false);
    return identity;
}

} // namespace keeply

#include "tls_context.hpp"
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <stdexcept>
#include <string>
namespace keeply {
SSL* createConnectedTlsSession(
    int fd,
    const TlsContextConfig& config,
    SSL_CTX*& ctxOut) {
    ctxOut = SSL_CTX_new(TLS_client_method());
    if (!ctxOut) throw std::runtime_error("Falha ao criar SSL_CTX.");
    if (config.allowInsecure) {
        SSL_CTX_set_verify(ctxOut, SSL_VERIFY_NONE, nullptr);
    } else {
        SSL_CTX_set_verify(ctxOut, SSL_VERIFY_PEER, nullptr);
        if (SSL_CTX_set_default_verify_paths(ctxOut) != 1) {
            throw std::runtime_error("Falha ao configurar trust store do TLS.");}}
    if (config.certPemPath && config.keyPemPath) {
        if (SSL_CTX_use_certificate_file(ctxOut, config.certPemPath->string().c_str(), SSL_FILETYPE_PEM) != 1) {
            throw std::runtime_error("Falha ao carregar certificado do agente.");}
        if (SSL_CTX_use_PrivateKey_file(ctxOut, config.keyPemPath->string().c_str(), SSL_FILETYPE_PEM) != 1) {
            throw std::runtime_error("Falha ao carregar chave privada do agente.");}}
    SSL* ssl = SSL_new(ctxOut);
    if (!ssl) throw std::runtime_error("Falha ao criar SSL.");
    SSL_set_mode(ssl, SSL_MODE_AUTO_RETRY | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    SSL_set_fd(ssl, fd);
    SSL_set_tlsext_host_name(ssl, config.host.c_str());
    if (!config.allowInsecure) {
#if OPENSSL_VERSION_NUMBER >= 0x10002000L
        if (SSL_set1_host(ssl, config.host.c_str()) != 1) {
            throw std::runtime_error("Falha ao configurar validacao de hostname TLS.");}
#endif
    }
    if (SSL_connect(ssl) != 1) throw std::runtime_error("Falha no handshake TLS.");
    if (!config.allowInsecure && SSL_get_verify_result(ssl) != X509_V_OK) {
        throw std::runtime_error("Validacao do certificado TLS falhou.");}
    return ssl;}
}

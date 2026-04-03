#include "tipos.hpp"
#include <blake3.h>
#include <zlib.h>
#include <stdexcept>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <cstring>
#if defined(__has_include)
#  if __has_include(<zstd.h>)
#    include <zstd.h>
#    define KEEPLY_HAVE_ZSTD_HEADER 1
#  endif
#endif
#ifndef KEEPLY_HAVE_ZSTD_HEADER
extern "C" {
std::size_t ZSTD_compressBound(std::size_t srcSize);
std::size_t ZSTD_compress(void* dst,
                          std::size_t dstCapacity,
                          const void* src,
                          std::size_t srcSize,
                          int compressionLevel);
std::size_t ZSTD_decompress(void* dst,
                            std::size_t dstCapacity,
                            const void* src,
                            std::size_t compressedSize);
unsigned ZSTD_isError(std::size_t code);
const char* ZSTD_getErrorName(std::size_t code);}
#endif
namespace keeply {
Blob Compactador::aesGcmEncrypt(
    const std::array<unsigned char, 32>& key,
    const std::array<unsigned char, 12>& iv,
    const unsigned char* data, std::size_t len)
{
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) throw std::runtime_error("EVP_CIPHER_CTX_new falhou (encrypt).");
    // Saida: [16-byte tag][ciphertext]; GCM nao altera tamanho do texto
    Blob out(16 + len);
    int outLen = 0;
    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) != 1 ||
        EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.data(), iv.data()) != 1 ||
        EVP_EncryptUpdate(ctx, out.data() + 16, &outLen, data, static_cast<int>(len)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptUpdate falhou (AES-256-GCM).");}
    int finalLen = 0;
    if (EVP_EncryptFinal_ex(ctx, out.data() + 16 + outLen, &finalLen) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptFinal_ex falhou (AES-256-GCM).");}
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, 16, out.data()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("Falha extraindo GCM tag.");}
    EVP_CIPHER_CTX_free(ctx);
    out.resize(16 + outLen + finalLen);
    return out;}
bool Compactador::aesGcmDecrypt(
    const std::array<unsigned char, 32>& key,
    const std::array<unsigned char, 12>& iv,
    const unsigned char* data, std::size_t len,
    Blob& plainOut)
{
    if (len < 16) return false;  // precisa de pelo menos 16 bytes de tag
    const unsigned char* tag        = data;
    const unsigned char* ciphertext = data + 16;
    const std::size_t    cipherLen  = len - 16;
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) return false;
    plainOut.resize(cipherLen);
    int outLen = 0;
    const bool ok =
        EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr, nullptr, nullptr) == 1 &&
        EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.data(), iv.data())       == 1 &&
        EVP_DecryptUpdate(ctx, plainOut.data(), &outLen, ciphertext, static_cast<int>(cipherLen)) == 1 &&
        EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, 16, const_cast<unsigned char*>(tag)) == 1;
    int finalLen = 0;
    const bool authOk = ok && (EVP_DecryptFinal_ex(ctx, plainOut.data() + outLen, &finalLen) == 1);
    EVP_CIPHER_CTX_free(ctx);
    if (!authOk) return false;
    plainOut.resize(outLen + finalLen);
    return true;}
std::array<unsigned char, 12> Compactador::generateIv() {
    std::array<unsigned char, 12> iv{};
    if (RAND_bytes(iv.data(), static_cast<int>(iv.size())) != 1)
        throw std::runtime_error("RAND_bytes falhou ao gerar IV.");
    return iv;}
bool Compactador::deriveKeyFromEnv(std::array<unsigned char, 32>& keyOut) {
    const char* raw = std::getenv("KEEPLY_BACKUP_KEY");
    if (!raw || raw[0] == '\0') return false;
    const std::string s(raw);
    // 64 chars hex -> 32 bytes direto
    if (s.size() == 64) {
        bool validHex = true;
        for (std::size_t i = 0; i < 32 && validHex; ++i) {
            auto nibble = [](char c) -> int {
                if (c >= '0' && c <= '9') return c - '0';
                if (c >= 'a' && c <= 'f') return c - 'a' + 10;
                if (c >= 'A' && c <= 'F') return c - 'A' + 10;
                return -1;};
            const int hi = nibble(s[i * 2]), lo = nibble(s[i * 2 + 1]);
            if (hi < 0 || lo < 0) { validHex = false; break; }
            keyOut[i] = static_cast<unsigned char>((hi << 4) | lo);}
        if (validHex) return true;}
    // Qualquer outra string: SHA-256 como chave
    SHA256(reinterpret_cast<const unsigned char*>(s.data()), s.size(), keyOut.data());
    return true;}

std::string Compactador::blake3Hex(const void* data, std::size_t len) {
    unsigned char digest[32]{};
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, digest, sizeof(digest));
    return hexOfBytes(digest, sizeof(digest));}
void Compactador::zlibCompress(
    const unsigned char* data,
    std::size_t len,
    int level,
    std::vector<unsigned char>& out
) {
    uLongf bound = compressBound(static_cast<uLong>(len));
    if (out.capacity() < bound) out.reserve(bound);
    out.resize(bound);
    int rc = compress2(out.data(), &bound, data, static_cast<uLong>(len), level);
    if (rc != Z_OK) {
        throw std::runtime_error("Falha ao comprimir chunk (zlib)");}
    out.resize(bound);}
void Compactador::zlibDecompress(
    const void* compData,
    std::size_t compLen,
    std::size_t rawSize,
    std::vector<unsigned char>& out
) {
    if (out.capacity() < rawSize) out.reserve(rawSize);
    out.resize(rawSize);
    uLongf destLen = static_cast<uLongf>(rawSize);
    int rc = uncompress(out.data(), &destLen,
                        static_cast<const Bytef*>(compData),
                        static_cast<uLong>(compLen));
    if (rc != Z_OK) {
        throw std::runtime_error("Falha ao descomprimir chunk (zlib)");}
    if (destLen != rawSize) {
        throw std::runtime_error("Tamanho descomprimido inesperado (zlib)");}}
void Compactador::zstdCompress(
    const unsigned char* data,
    std::size_t len,
    int level,
    std::vector<unsigned char>& out
) {
    const std::size_t bound = ZSTD_compressBound(len);
    if (out.capacity() < bound) out.reserve(bound);
    out.resize(bound);
    const std::size_t rc = ZSTD_compress(out.data(), out.size(), data, len, level);
    if (ZSTD_isError(rc)) {
        throw std::runtime_error(std::string("Falha ao comprimir chunk (zstd): ") + ZSTD_getErrorName(rc));}
    out.resize(rc);}
void Compactador::zstdDecompress(
    const void* compData,
    std::size_t compLen,
    std::size_t rawSize,
    std::vector<unsigned char>& out
) {
    if (out.capacity() < rawSize) out.reserve(rawSize);
    out.resize(rawSize);
    const std::size_t rc = ZSTD_decompress(out.data(), out.size(), compData, compLen);
    if (ZSTD_isError(rc)) {
        throw std::runtime_error(std::string("Falha ao descomprimir chunk (zstd): ") + ZSTD_getErrorName(rc));}
    if (rc != rawSize) {
        throw std::runtime_error("Tamanho descomprimido inesperado (zstd)");}}}

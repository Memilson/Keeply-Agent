#include "keeply.hpp"

#include <openssl/sha.h>
#include <zlib.h>

#include <stdexcept>

namespace keeply {

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
const char* ZSTD_getErrorName(std::size_t code);
}

std::string Compactador::sha256Hex(const void* data, std::size_t len) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(digest, &ctx);
    return hexOfBytes(digest, SHA256_DIGEST_LENGTH);
}

std::vector<unsigned char> Compactador::zlibCompress(
    const unsigned char* data,
    std::size_t len,
    int level
) {
    uLongf bound = compressBound(static_cast<uLong>(len));
    std::vector<unsigned char> out(bound);

    int rc = compress2(out.data(), &bound, data, static_cast<uLong>(len), level);
    if (rc != Z_OK) {
        throw std::runtime_error("Falha ao comprimir chunk (zlib)");
    }

    out.resize(bound);
    return out;
}

std::vector<unsigned char> Compactador::zlibDecompress(
    const void* compData,
    std::size_t compLen,
    std::size_t rawSize
) {
    std::vector<unsigned char> out(rawSize);
    uLongf destLen = static_cast<uLongf>(rawSize);

    int rc = uncompress(out.data(), &destLen,
                        static_cast<const Bytef*>(compData),
                        static_cast<uLong>(compLen));

    if (rc != Z_OK) {
        throw std::runtime_error("Falha ao descomprimir chunk (zlib)");
    }
    if (destLen != rawSize) {
        throw std::runtime_error("Tamanho descomprimido inesperado");
    }

    return out;
}

std::vector<unsigned char> Compactador::zstdCompress(
    const unsigned char* data,
    std::size_t len,
    int level
) {
    const std::size_t bound = ZSTD_compressBound(len);
    std::vector<unsigned char> out(bound);
    const std::size_t rc = ZSTD_compress(out.data(), out.size(), data, len, level);
    if (ZSTD_isError(rc)) {
        throw std::runtime_error(std::string("Falha ao comprimir chunk (zstd): ") + ZSTD_getErrorName(rc));
    }
    out.resize(rc);
    return out;
}

std::vector<unsigned char> Compactador::zstdDecompress(
    const void* compData,
    std::size_t compLen,
    std::size_t rawSize
) {
    std::vector<unsigned char> out(rawSize);
    const std::size_t rc = ZSTD_decompress(out.data(), out.size(), compData, compLen);
    if (ZSTD_isError(rc)) {
        throw std::runtime_error(std::string("Falha ao descomprimir chunk (zstd): ") + ZSTD_getErrorName(rc));
    }
    if (rc != rawSize) {
        throw std::runtime_error("Tamanho descomprimido inesperado (zstd)");
    }
    return out;
}

}

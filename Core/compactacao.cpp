#include "tipos.hpp"

#include <blake3.h>
#include <zlib.h>
#include <stdexcept>

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
const char* ZSTD_getErrorName(std::size_t code);
}
#endif

namespace keeply {

std::string Compactador::blake3Hex(const void* data, std::size_t len) {
    unsigned char digest[32]{};
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, digest, sizeof(digest));
    return hexOfBytes(digest, sizeof(digest));
}

void Compactador::zlibCompress(
    const unsigned char* data,
    std::size_t len,
    int level,
    std::vector<unsigned char>& out // Buffer reaproveitável passado por referência
) {
    uLongf bound = compressBound(static_cast<uLong>(len));

    // Garante capacidade sem realocar se o buffer já for grande o suficiente
    if (out.capacity() < bound) out.reserve(bound);
    out.resize(bound);

    int rc = compress2(out.data(), &bound, data, static_cast<uLong>(len), level);
    if (rc != Z_OK) {
        throw std::runtime_error("Falha ao comprimir chunk (zlib)");
    }

    out.resize(bound); // Ajusta para o tamanho real comprimido
}

void Compactador::zlibDecompress(
    const void* compData,
    std::size_t compLen,
    std::size_t rawSize,
    std::vector<unsigned char>& out // Buffer reaproveitável
) {
    if (out.capacity() < rawSize) out.reserve(rawSize);
    out.resize(rawSize);

    uLongf destLen = static_cast<uLongf>(rawSize);

    int rc = uncompress(out.data(), &destLen,
                        static_cast<const Bytef*>(compData),
                        static_cast<uLong>(compLen));

    if (rc != Z_OK) {
        throw std::runtime_error("Falha ao descomprimir chunk (zlib)");
    }
    if (destLen != rawSize) {
        throw std::runtime_error("Tamanho descomprimido inesperado (zlib)");
    }
}

void Compactador::zstdCompress(
    const unsigned char* data,
    std::size_t len,
    int level,
    std::vector<unsigned char>& out // Buffer reaproveitável
) {
    const std::size_t bound = ZSTD_compressBound(len);

    if (out.capacity() < bound) out.reserve(bound);
    out.resize(bound);

    const std::size_t rc = ZSTD_compress(out.data(), out.size(), data, len, level);
    if (ZSTD_isError(rc)) {
        throw std::runtime_error(std::string("Falha ao comprimir chunk (zstd): ") + ZSTD_getErrorName(rc));
    }

    out.resize(rc);
}

void Compactador::zstdDecompress(
    const void* compData,
    std::size_t compLen,
    std::size_t rawSize,
    std::vector<unsigned char>& out // Buffer reaproveitável
) {
    if (out.capacity() < rawSize) out.reserve(rawSize);
    out.resize(rawSize);

    const std::size_t rc = ZSTD_decompress(out.data(), out.size(), compData, compLen);
    if (ZSTD_isError(rc)) {
        throw std::runtime_error(std::string("Falha ao descomprimir chunk (zstd): ") + ZSTD_getErrorName(rc));
    }
    if (rc != rawSize) {
        throw std::runtime_error("Tamanho descomprimido inesperado (zstd)");
    }
}

} // namespace keeply

#include "delta_rsync.hpp"
#include <librsync.h>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#  include <unistd.h>
namespace keeply {
namespace {
static FILE* openTempFile(char outPath[256]) {
    std::strcpy(outPath, "/tmp/keeply_rsync_XXXXXX");
    const int fd = ::mkstemp(outPath);
    FILE* f = (fd >= 0) ? ::fdopen(fd, "w+b") : nullptr;
    if (!f) throw std::runtime_error("Falha criando arquivo temporario (rsync).");
    return f;}
static FILE* blobToTempReadFile(const Blob& blob, char tmpPath[256]) {
    FILE* f = openTempFile(tmpPath);
    if (!blob.empty()) std::fwrite(blob.data(), 1, blob.size(), f);
    std::rewind(f);
    return f;}
static Blob readFile(FILE* f) {
    std::fseek(f, 0, SEEK_END);
    const long sz = std::ftell(f);
    std::rewind(f);
    Blob out(sz > 0 ? static_cast<std::size_t>(sz) : 0);
    if (!out.empty()) {
        const std::size_t got = std::fread(out.data(), 1, out.size(), f);
        if (got != out.size()) throw std::runtime_error("Leitura incompleta do arquivo temporario rsync.");}
    return out;}
static void rsCheck(rs_result r, const char* ctx) {
    if (r != RS_DONE)
        throw std::runtime_error(std::string("librsync (") + ctx + "): " + rs_strerror(r));}}
Blob rsyncGenerateSignature(const fs::path& baseFile) {
    FILE* inFile = fopenPath(baseFile, "rb");
    if (!inFile)
        throw std::runtime_error("Falha abrindo arquivo para assinatura rsync: " + baseFile.string());
    char sigPath[256];
    FILE* sigOut = openTempFile(sigPath);
    const rs_result r = rs_sig_file(inFile, sigOut,
                                    RS_DEFAULT_BLOCK_LEN, RS_DEFAULT_STRONG_LEN,
                                    RS_MD4_SIG_MAGIC, nullptr);
    std::fclose(inFile);
    if (r != RS_DONE) { std::fclose(sigOut); std::remove(sigPath); rsCheck(r, "rs_sig_file"); }
    std::rewind(sigOut);
    Blob sig = readFile(sigOut);
    std::fclose(sigOut);
    std::remove(sigPath);
    return sig;}
std::optional<Blob> rsyncGenerateDelta(const Blob& prevSignature, const fs::path& newFile) {
    if (prevSignature.empty()) return std::nullopt;
    char sigPath[256];
    FILE* sigIn = blobToTempReadFile(prevSignature, sigPath);
    rs_signature_t* sig = nullptr;
    const rs_result loadR = rs_loadsig_file(sigIn, &sig, nullptr);
    std::fclose(sigIn);
    std::remove(sigPath);
    if (loadR != RS_DONE || !sig) return std::nullopt;
    rs_build_hash_table(sig);
    FILE* newIn = fopenPath(newFile, "rb");
    if (!newIn) { rs_free_sumset(sig); return std::nullopt; }
    char deltaPath[256];
    FILE* deltaOut = openTempFile(deltaPath);
    const rs_result deltaR = rs_delta_file(sig, newIn, deltaOut, nullptr);
    rs_free_sumset(sig);
    std::fclose(newIn);
    if (deltaR != RS_DONE) {
        std::fclose(deltaOut); std::remove(deltaPath);
        return std::nullopt;}
    std::rewind(deltaOut);
    Blob delta = readFile(deltaOut);
    std::fclose(deltaOut);
    std::remove(deltaPath);
    return delta;}
void rsyncApplyDelta(const fs::path& baseFile, const Blob& deltaBlob, const fs::path& outputFile) {
    FILE* base = fopenPath(baseFile, "rb");
    if (!base)
        throw std::runtime_error("Falha abrindo arquivo base para patch rsync: " + baseFile.string());
    char deltaPath[256];
    FILE* deltaIn = blobToTempReadFile(deltaBlob, deltaPath);
    FILE* outFile = fopenPath(outputFile, "wb");
    if (!outFile) {
        std::fclose(base); std::fclose(deltaIn); std::remove(deltaPath);
        throw std::runtime_error("Falha criando arquivo de saida do patch rsync: " + outputFile.string());}
    const rs_result r = rs_patch_file(base, deltaIn, outFile, nullptr);
    std::fclose(base);
    std::fclose(deltaIn); std::remove(deltaPath);
    std::fclose(outFile);
    rsCheck(r, "rs_patch_file");}}

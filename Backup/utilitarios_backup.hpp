#pragma once

// =============================================================================
// utilitarios_backup.hpp
// Utilitários compartilhados — elimina duplicações entre módulos do agente.
// Todos os módulos devem usar estas funções em vez de implementações locais.
// NUNCA inclua headers de plataforma aqui.
// =============================================================================

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace keeply {

// -----------------------------------------------------------------------------
// String utilities
// -----------------------------------------------------------------------------

/// Converte ASCII para minúsculas sem depender de locale.
inline std::string lowerAscii(std::string s) {
    for (char& c : s)
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return s;
}

/// Remove espaços/tabs/newlines do início e fim.
inline std::string trim(const std::string& s) {
    const auto front = s.find_first_not_of(" \t\r\n");
    if (front == std::string::npos) return {};
    const auto back = s.find_last_not_of(" \t\r\n");
    return s.substr(front, back - front + 1);
}

/// Normaliza separadores de caminho para '/' e remove trailing slashes.
inline std::string normalizeSeparators(std::string p) {
    for (char& c : p)
        if (c == '\\') c = '/';
    while (p.size() > 1 && p.back() == '/')
        p.pop_back();
    return p;
}

/// Interpreta verdadeiro/falso a partir de strings de configuração.
/// Aceita "1", "true", "yes", "on" como true.
inline bool parseTruthyValue(const std::string& raw) {
    std::string v = lowerAscii(trim(raw));
    return v == "1" || v == "true" || v == "yes" || v == "on";
}

// -----------------------------------------------------------------------------
// Hex encoding / decoding  (substitui hexOfBytes, hexEncode, hexNibble, etc.)
// Todas as implementações locais de hex devem ser substituídas por estas.
// -----------------------------------------------------------------------------

/// Converte um nibble hex ('0'-'9','a'-'f','A'-'F') em valor 0..15.
inline unsigned hexNibble(char c) {
    if (c >= '0' && c <= '9') return static_cast<unsigned>(c - '0');
    if (c >= 'a' && c <= 'f') return static_cast<unsigned>(10 + c - 'a');
    if (c >= 'A' && c <= 'F') return static_cast<unsigned>(10 + c - 'A');
    throw std::runtime_error("Hex invalido.");
}

/// Codifica bytes para hex lowercase.
inline std::string hexEncode(const unsigned char* data, std::size_t size) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(size * 2);
    for (std::size_t i = 0; i < size; ++i) {
        out.push_back(kHex[(data[i] >> 4) & 0x0F]);
        out.push_back(kHex[data[i] & 0x0F]);
    }
    return out;
}

/// Decodifica hex string para vetor de bytes.
inline std::vector<unsigned char> hexDecode(const std::string& hex) {
    if (hex.size() % 2 != 0) throw std::runtime_error("Hex com tamanho invalido.");
    std::vector<unsigned char> out(hex.size() / 2);
    for (std::size_t i = 0; i < out.size(); ++i) {
        out[i] = static_cast<unsigned char>(
            (hexNibble(hex[i * 2]) << 4) | hexNibble(hex[i * 2 + 1]));
    }
    return out;
}

// -----------------------------------------------------------------------------
// Typed exceptions — substituem classificação de erros por string matching.
// Use estas exceções em throw sites novos. No REST handler, o catch pode
// distinguir por tipo em vez de parsear o texto da mensagem.
// -----------------------------------------------------------------------------

/// Recurso não encontrado (mapeia para HTTP 404).
class KeeplyNotFoundError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/// Requisição inválida / parâmetro incorreto (mapeia para HTTP 400).
class KeeplyValidationError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// -----------------------------------------------------------------------------
// Hash algorithm name constants
// Centraliza os nomes dos algoritmos para evitar strings mágicas espalhadas.
// -----------------------------------------------------------------------------
inline constexpr const char* kAlgoBlake3 = "blake3";   ///< Hash primário de chunks
inline constexpr const char* kAlgoZstd   = "zstd";     ///< Compressão padrão
inline constexpr const char* kAlgoZlib   = "zlib";     ///< Compressão legada
inline constexpr const char* kAlgoRaw    = "raw";      ///< Sem compressão

// -----------------------------------------------------------------------------
// Protocol versioning  — incluso em todas as mensagens JSON do agente.
// Incrementar a cada mudança incompatível no formato de mensagens.
// -----------------------------------------------------------------------------
inline constexpr int kProtocolVersion = 1;

// -----------------------------------------------------------------------------
// Schema versioning
// Incrementar kCurrentSchemaVersion a cada mudança estrutural no banco.
// O mecanismo de migração em DB::initSchema() aplica as etapas necessárias.
// -----------------------------------------------------------------------------
inline constexpr int kCurrentSchemaVersion = 6;
//  v1 — schema inicial
//  v2 — ADD COLUMN cbt_token em snapshots
//  v3 — rename hash_sha256 → chunk_hash (BLAKE3, não SHA-256)
//  v4 — ADD COLUMN pack_id em chunks
//  v5 — ADD COLUMN storage_state em chunks
//  v6 — recreate files table para reparar FKs/migração

} // namespace keeply

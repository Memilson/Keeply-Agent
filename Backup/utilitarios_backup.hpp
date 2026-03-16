#pragma once

// =============================================================================
// utilitarios_backup.hpp
// Utilitários compartilhados — elimina duplicações entre arquivo_armazenamento.cpp,
// varredura_backup.cpp
// e outros módulos. NUNCA inclua headers de plataforma aqui.
// =============================================================================

#include <algorithm>
#include <string>

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

// -----------------------------------------------------------------------------
// Hash algorithm name constants
// Centraliza os nomes dos algoritmos para evitar strings mágicas espalhadas.
// -----------------------------------------------------------------------------
inline constexpr const char* kAlgoBlake3 = "blake3";   ///< Hash primário de chunks
inline constexpr const char* kAlgoZstd   = "zstd";     ///< Compressão padrão
inline constexpr const char* kAlgoZlib   = "zlib";     ///< Compressão legada
inline constexpr const char* kAlgoRaw    = "raw";      ///< Sem compressão

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

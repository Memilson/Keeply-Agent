#pragma once
#include "../Core/tipos.hpp"
#include <filesystem>
#include <optional>

namespace keeply {

// Geracao de assinatura rsync a partir de um arquivo base.
// A assinatura e um blob pequeno (~1/700 do arquivo) que representa
// o conteudo sem precisar do arquivo original.
Blob rsyncGenerateSignature(const fs::path& baseFile);

// Gera um delta binario comparando a assinatura do backup anterior
// com o arquivo atual. O delta contem apenas as diferencas.
// Retorna nullopt se a sig nao puder ser usada (arquivo muito diferente).
std::optional<Blob> rsyncGenerateDelta(const Blob& prevSignature, const fs::path& newFile);

// Aplica um delta sobre um arquivo base para reconstruir o arquivo final.
// baseFile: arquivo completo do backup anterior (restaurado ou local)
// deltaBlob: delta gerado por rsyncGenerateDelta
// outputFile: destino do arquivo reconstruido
void rsyncApplyDelta(const fs::path& baseFile, const Blob& deltaBlob, const fs::path& outputFile);

} // namespace keeply

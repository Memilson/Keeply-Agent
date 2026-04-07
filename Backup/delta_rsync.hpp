#pragma once
#include "../Core/tipos.hpp"
#include <filesystem>
#include <optional>
namespace keeply {
Blob rsyncGenerateSignature(const fs::path& baseFile);
std::optional<Blob> rsyncGenerateDelta(const Blob& prevSignature, const fs::path& newFile);
void rsyncApplyDelta(const fs::path& baseFile, const Blob& deltaBlob, const fs::path& outputFile);}

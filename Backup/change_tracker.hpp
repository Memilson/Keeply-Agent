#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace keeply {

struct ChangedFile {
    std::string relPath;
    bool isDeleted = false;
};

class ChangeTracker {
public:
    virtual ~ChangeTracker() = default;
    virtual bool isAvailable() const = 0;
    virtual const char* backendName() const = 0;
    virtual void startTracking(const std::filesystem::path& rootPath) = 0;
    virtual std::vector<ChangedFile> getChanges(std::uint64_t lastToken, std::uint64_t& newToken) = 0;
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker();

} // namespace keeply

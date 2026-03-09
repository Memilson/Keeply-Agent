#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace keeply {

namespace fs = std::filesystem;

struct ChangedFile {
    std::string relPath;
    bool isDeleted = false;
};

class EventStore {
public:
    explicit EventStore(const fs::path& dbPath);
    ~EventStore();

    EventStore(const EventStore&) = delete;
    EventStore& operator=(const EventStore&) = delete;

    void ensureRoot(const fs::path& rootPath);
    void appendEvent(const std::string& type, const std::string& relPath, bool isDir);
    std::optional<std::uint64_t> latestToken(const fs::path& rootPath) const;
    std::vector<ChangedFile> loadChangesSince(const fs::path& rootPath,
                                              std::uint64_t lastToken,
                                              std::uint64_t& newToken) const;

private:
    class Db;

    static std::string normalizePath(const fs::path& path);
    static std::int64_t nowUnixSeconds();

    Db* db_ = nullptr;
    std::int64_t rootId_ = 0;
    std::int64_t nextSeq_ = 1;
};

class ChangeTracker {
public:
    virtual ~ChangeTracker() = default;
    virtual bool isAvailable() const = 0;
    virtual const char* backendName() const = 0;
    virtual void startTracking(const fs::path& rootPath) = 0;
    virtual std::vector<ChangedFile> getChanges(std::uint64_t lastToken,
                                                std::uint64_t& newToken) = 0;
};

std::unique_ptr<ChangeTracker> createPlatformChangeTracker();
fs::path defaultEventStorePath();
fs::path defaultEventStorePidPath();

} // namespace keeply

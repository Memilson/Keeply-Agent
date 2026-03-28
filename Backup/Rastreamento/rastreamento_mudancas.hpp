#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

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
    void prepareStatements();

    static std::string normalizePath(const fs::path& path);
    static std::int64_t nowUnixSeconds();

    std::unique_ptr<Db> db_;
    sqlite3_stmt* appendStmt_ = nullptr;
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
fs::path defaultEventStoreRootPath();

class BackgroundCbtWatcher {
public:
    BackgroundCbtWatcher();
    ~BackgroundCbtWatcher();

    BackgroundCbtWatcher(const BackgroundCbtWatcher&) = delete;
    BackgroundCbtWatcher& operator=(const BackgroundCbtWatcher&) = delete;

    void start(const fs::path& rootPath);
    void stop() noexcept;
    bool running() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

namespace rastreamento_eventos_base {

enum class TipoEventoMonitorado {
    Upsert,
    Modify,
    Delete
};

class MotorMonitor {
public:
    virtual ~MotorMonitor() = default;
    virtual void requestStop() noexcept = 0;
    virtual void run(const std::function<bool()>& shouldStop) = 0;
};

class MotorMonitorBase : public MotorMonitor {
public:
    MotorMonitorBase(const fs::path& rootPath, bool respectSystemExclusionPolicy);

protected:
    const fs::path& rootPath() const noexcept;
    bool respectSystemExclusionPolicy() const noexcept;
    bool shouldIgnorePath(const fs::path& path) const;
    std::optional<std::string> buildRelativePath(const fs::path& fullPath) const;
    void appendEvent(TipoEventoMonitorado eventType, const std::string& relPath, bool isDir);
    EventStore& eventStore() noexcept;

private:
    fs::path rootPath_;
    bool respectSystemExclusionPolicy_ = false;
    std::unique_ptr<EventStore> store_;
};

struct OpcoesDaemonMonitor {
    fs::path rootPath;
    bool foreground = false;
    bool installService = false;
    bool uninstallService = false;
    bool serviceMode = false;
};

class MonitorRunner {
public:
    MonitorRunner(const fs::path& rootPath, bool respectSystemExclusionPolicy);

    void requestStop() noexcept;
    void run(const std::function<bool()>& shouldStop);
    MotorMonitor* engine() noexcept;

private:
    std::unique_ptr<MotorMonitor> engine_;
};

std::unique_ptr<MotorMonitor> createLinuxMotorMonitor(const fs::path& rootPath,
                                                      bool respectSystemExclusionPolicy);
std::unique_ptr<MotorMonitor> createWindowsMotorMonitor(const fs::path& rootPath,
                                                        bool respectSystemExclusionPolicy);
std::unique_ptr<MotorMonitor> createPlatformMotorMonitor(const fs::path& rootPath,
                                                         bool respectSystemExclusionPolicy);
OpcoesDaemonMonitor parseDaemonMonitorArgs(int argc, char** argv, bool allowServiceOptions);
fs::path resolveDaemonMonitorRootOrThrow(const fs::path& rootFromArgs);
void prepareDaemonMonitorState(const fs::path& rootPath, const std::string& pidValue);
fs::path resolveAndPrepareDaemonMonitorRootOrThrow(const fs::path& rootFromArgs,
                                                   const std::string& pidValue);
void runMonitorDaemonOrThrow(const fs::path& rootPath,
                             const std::string& pidValue,
                             const std::function<bool()>& shouldStop);
std::string envOrEmpty(const char* key);
std::optional<std::string> buildRelativeEventPath(const fs::path& rootPath, const fs::path& fullPath);
void appendMonitoredEvent(EventStore& store,
                          TipoEventoMonitorado eventType,
                          const std::string& relPath,
                          bool isDir);
void writeMonitorPidFile(const fs::path& pidPath, const std::string& pidValue);
void writeMonitorRootFile(const fs::path& metadataPath, const fs::path& rootPath);

}

}

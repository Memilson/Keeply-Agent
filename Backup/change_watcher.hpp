#pragma once

#include <filesystem>
#include <memory>

namespace keeply {

namespace fs = std::filesystem;

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

} // namespace keeply

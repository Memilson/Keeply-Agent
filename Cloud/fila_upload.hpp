#pragma once
#include <chrono>
#include <cstddef>
#include <functional>
namespace keeply {
struct ParallelUploadOptions{
    std::size_t workerCount=2;
    std::size_t maxRetries=3;
    std::chrono::milliseconds retryBackoff{750};};
void runParallelUploadQueue(
    std::size_t taskCount,
    const ParallelUploadOptions& options,
    const std::function<void(std::size_t taskIndex,std::size_t attempt)>& worker);}

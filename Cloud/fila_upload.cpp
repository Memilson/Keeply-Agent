#include "fila_upload.hpp"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
namespace keeply {
namespace {
struct QueueItem{
    std::size_t taskIndex=0;
    std::size_t attempt=1;};}
void runParallelUploadQueue(
    std::size_t taskCount,
    const ParallelUploadOptions& options,
    const std::function<void(std::size_t taskIndex,std::size_t attempt)>& worker){
    if(taskCount==0) return;
    if(!worker) throw std::runtime_error("Worker paralelo invalido.");
    const std::size_t workerCount=options.workerCount==0?1:options.workerCount;
    const std::size_t maxRetries=options.maxRetries==0?1:options.maxRetries;
    std::deque<QueueItem> queue;
    for(std::size_t i=0;i<taskCount;++i) queue.push_back(QueueItem{i,1});
    std::mutex mu;
    std::condition_variable cv;
    std::atomic<bool> abortUpload{false};
    std::size_t activeWorkers=0;
    std::exception_ptr fatalError;
    auto workerLoop=[&](){
        for(;;){
            QueueItem item;{
                std::unique_lock<std::mutex> lock(mu);
                cv.wait(lock,[&](){return abortUpload.load()||!queue.empty()||activeWorkers==0;});
                if(abortUpload.load()) return;
                if(queue.empty()){
                    if(activeWorkers==0) return;
                    continue;}
                item=queue.front();
                queue.pop_front();
                ++activeWorkers;}
            bool shouldRetry=false;
            try{
                worker(item.taskIndex,item.attempt);
            }catch(...){
                if(item.attempt<maxRetries&&!abortUpload.load()){
                    shouldRetry=true;
                }else{{
                        std::lock_guard<std::mutex> lock(mu);
                        if(!fatalError) fatalError=std::current_exception();}
                    abortUpload.store(true);}}{
                std::lock_guard<std::mutex> lock(mu);
                if(activeWorkers>0) --activeWorkers;
                if(shouldRetry&&!abortUpload.load()){
                    queue.push_front(QueueItem{item.taskIndex,item.attempt+1});}}
            if(shouldRetry&&!abortUpload.load()&&options.retryBackoff.count()>0){
                std::this_thread::sleep_for(options.retryBackoff*static_cast<int>(item.attempt));}
            cv.notify_all();}};
    std::vector<std::thread> threads;
    threads.reserve(workerCount);
    for(std::size_t i=0;i<workerCount;++i){
        threads.emplace_back(workerLoop);}
    for(auto& t:threads){
        if(t.joinable()) t.join();}
    if(fatalError) std::rethrow_exception(fatalError);}}

#pragma once
#include <string>
#include <memory>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <arrow/api.h>
#include "green_gate/arrow_connector.hpp"

namespace greengate {

class S3Writer {
public:
    S3Writer(const std::string& endpoint, const std::string& bucket,
             const std::string& access_key, const std::string& secret_key);
    ~S3Writer();

    // Queues a batch to be transposed and written to S3 asynchronously
    void WriteAsync(const std::string& s3_key, 
                    const std::shared_ptr<arrow::RecordBatch>& batch);

    // Wait for all queued writes to complete
    void Flush();

private:
    struct WriteRequest {
        std::string s3_key;
        std::shared_ptr<arrow::RecordBatch> batch;
    };

    void WorkerLoop();
    bool UploadToS3(const std::string& s3_key, const uint8_t* data, size_t size);

    std::string endpoint_;
    std::string bucket_;
    std::string access_key_;
    std::string secret_key_;
    
    std::queue<WriteRequest> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable flush_cv_;
    std::thread worker_;
    bool stop_ = false;
    uint64_t active_tasks_ = 0;
};

} // namespace greengate

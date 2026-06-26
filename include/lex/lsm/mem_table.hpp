#pragma once
#include <arrow/api.h>
#include "lex/jit/query_planner.hpp"
#include <vector>
#include <string>
#include <memory>
#include <mutex>

namespace greengate {

class MemTable {
public:
    MemTable() = default;
    ~MemTable() = default;

    void Append(const std::shared_ptr<arrow::RecordBatch>& batch);
    void Clear();

    std::vector<std::shared_ptr<arrow::RecordBatch>> GetBatches() const;
    size_t GetMemorySize() const;
    size_t GetRowCount() const;

    uint64_t ExecuteQuery(const std::vector<Predicate>& predicates, 
                          const std::string& agg_col_name, 
                          uint64_t& out_agg_sum) const;

private:
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    size_t row_count_ = 0;
    size_t memory_size_ = 0;
};

} // namespace greengate

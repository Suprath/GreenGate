#include "lex/lsm/mem_table.hpp"
#include <cstring>
#include <iostream>
#include <string_view>

namespace greengate {

static bool MatchString(std::string_view val, PredicateOp op, std::string_view clean_pat) {
    if (op == PredicateOp::EQ) {
        return val == clean_pat;
    } else if (op == PredicateOp::LIKE_PREFIX) {
        return val.size() >= clean_pat.size() && val.compare(0, clean_pat.size(), clean_pat) == 0;
    } else if (op == PredicateOp::LIKE_CONTAINS) {
        return val.find(clean_pat) != std::string_view::npos;
    }
    return false;
}

static bool MatchNumeric(int64_t val, PredicateOp op, uint64_t target) {
    int64_t target_signed = static_cast<int64_t>(target);
    if (op == PredicateOp::EQ) {
        return val == target_signed;
    } else if (op == PredicateOp::GT) {
        return val > target_signed;
    } else if (op == PredicateOp::LT) {
        return val < target_signed;
    }
    return false;
}

void MemTable::Append(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) return;
    std::lock_guard<std::mutex> lock(mutex_);
    batches_.push_back(batch);
    row_count_ += batch->num_rows();

    size_t batch_size = 0;
    for (int i = 0; i < batch->num_columns(); ++i) {
        auto arr = batch->column(i);
        if (arr && arr->data()) {
            for (const auto& buf : arr->data()->buffers) {
                if (buf) {
                    batch_size += buf->size();
                }
            }
        }
    }
    memory_size_ += batch_size;
}

void MemTable::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    batches_.clear();
    row_count_ = 0;
    memory_size_ = 0;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> MemTable::GetBatches() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return batches_;
}

size_t MemTable::GetMemorySize() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return memory_size_;
}

size_t MemTable::GetRowCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return row_count_;
}

uint64_t MemTable::ExecuteQuery(const std::vector<Predicate>& predicates, 
                              const std::string& agg_col_name, 
                              uint64_t& out_agg_sum) const {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t total_matches = 0;
    out_agg_sum = 0;

    for (const auto& batch : batches_) {
        size_t num_rows = batch->num_rows();
        if (num_rows == 0) continue;

        std::vector<bool> matches(num_rows, true);

        for (const auto& pred : predicates) {
            auto col = batch->GetColumnByName(pred.column_name);
            if (!col) {
                // Column not present in this batch, all rows fail
                std::fill(matches.begin(), matches.end(), false);
                break;
            }

            if (pred.is_string) {
                auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);
                std::string clean_pat = pred.value;
                if (pred.op == PredicateOp::LIKE_PREFIX) {
                    if (!clean_pat.empty() && clean_pat.back() == '%') clean_pat.pop_back();
                } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                    if (!clean_pat.empty() && clean_pat.front() == '%') clean_pat.erase(0, 1);
                    if (!clean_pat.empty() && clean_pat.back() == '%') clean_pat.pop_back();
                }

                for (size_t r = 0; r < num_rows; ++r) {
                    if (!matches[r]) continue;
                    if (str_arr->IsNull(r)) {
                        matches[r] = false;
                    } else {
                        matches[r] = MatchString(str_arr->GetView(r), pred.op, clean_pat);
                    }
                }
            } else {
                auto int_arr = std::static_pointer_cast<arrow::Int64Array>(col);
                for (size_t r = 0; r < num_rows; ++r) {
                    if (!matches[r]) continue;
                    if (int_arr->IsNull(r)) {
                        matches[r] = false;
                    } else {
                        matches[r] = MatchNumeric(int_arr->Value(r), pred.op, pred.numeric_value);
                    }
                }
            }
        }

        // Count matches and aggregate
        uint64_t batch_matches = 0;
        for (size_t r = 0; r < num_rows; ++r) {
            if (matches[r]) {
                batch_matches++;
            }
        }
        total_matches += batch_matches;

        if (batch_matches > 0 && !agg_col_name.empty()) {
            auto agg_col = batch->GetColumnByName(agg_col_name);
            if (agg_col) {
                auto int_arr = std::static_pointer_cast<arrow::Int64Array>(agg_col);
                for (size_t r = 0; r < num_rows; ++r) {
                    if (matches[r] && !int_arr->IsNull(r)) {
                        out_agg_sum += int_arr->Value(r);
                    }
                }
            }
        }
    }

    return total_matches;
}

} // namespace greengate

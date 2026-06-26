#include "lex/lsm/mem_table.hpp"
#include <cstring>
#include <iostream>
#include <string_view>
#include <unordered_set>
#include <algorithm>

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

QueryResult MemTable::ExecuteQueryResult(const std::vector<Predicate>& predicates, 
                                         const std::string& agg_col_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    QueryResult res;
    res.count = 0;
    res.sum = 0;
    
    std::vector<int64_t> matched_numerics;
    std::vector<std::string> matched_strings;
    
    bool is_agg_str = false;

    for (const auto& batch : batches_) {
        size_t num_rows = batch->num_rows();
        if (num_rows == 0) continue;

        std::vector<bool> matches(num_rows, true);

        for (const auto& pred : predicates) {
            auto col = batch->GetColumnByName(pred.column_name);
            if (!col) {
                std::fill(matches.begin(), matches.end(), false);
                break;
            }

            if (pred.is_string) {
                auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);
                std::string clean_pat = pred.value;
                if (pred.op == PredicateOp::LIKE_PREFIX && !clean_pat.empty() && clean_pat.back() == '%') {
                    clean_pat.pop_back();
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

        uint64_t batch_matches = 0;
        for (size_t r = 0; r < num_rows; ++r) {
            if (matches[r]) {
                batch_matches++;
            }
        }
        res.count += batch_matches;

        if (batch_matches > 0 && !agg_col_name.empty()) {
            auto agg_col = batch->GetColumnByName(agg_col_name);
            if (agg_col) {
                if (agg_col->type_id() == arrow::Type::STRING || agg_col->type_id() == arrow::Type::BINARY) {
                    is_agg_str = true;
                    auto str_arr = std::static_pointer_cast<arrow::StringArray>(agg_col);
                    for (size_t r = 0; r < num_rows; ++r) {
                        if (matches[r] && !str_arr->IsNull(r)) {
                            matched_strings.push_back(std::string(str_arr->GetView(r)));
                        }
                    }
                } else {
                    auto int_arr = std::static_pointer_cast<arrow::Int64Array>(agg_col);
                    for (size_t r = 0; r < num_rows; ++r) {
                        if (matches[r] && !int_arr->IsNull(r)) {
                            int64_t val = int_arr->Value(r);
                            matched_numerics.push_back(val);
                            res.sum += val;
                        }
                    }
                }
            }
        }
    }

    if (res.count > 0 && !agg_col_name.empty()) {
        if (is_agg_str) {
            if (!matched_strings.empty()) {
                std::string min_s = matched_strings[0];
                std::string max_s = matched_strings[0];
                for (const auto& s : matched_strings) {
                    if (s < min_s) min_s = s;
                    if (s > max_s) max_s = s;
                }
                res.min_val = 0;
                res.max_val = 0;
                std::unordered_set<std::string> dist_set(matched_strings.begin(), matched_strings.end());
                res.distinct_count = dist_set.size();
                res.distinct_strings.assign(dist_set.begin(), dist_set.end());
            }
        } else {
            if (!matched_numerics.empty()) {
                res.min_val = matched_numerics[0];
                res.max_val = matched_numerics[0];
                for (auto val : matched_numerics) {
                    if (val < res.min_val) res.min_val = val;
                    if (val > res.max_val) res.max_val = val;
                }
                
                std::unordered_set<int64_t> dist_set(matched_numerics.begin(), matched_numerics.end());
                res.distinct_count = dist_set.size();
                res.distinct_numerics.assign(dist_set.begin(), dist_set.end());
                
                res.avg = static_cast<double>(res.sum) / matched_numerics.size();
                
                std::vector<int64_t> sorted_n = matched_numerics;
                std::sort(sorted_n.begin(), sorted_n.end());
                size_t sz = sorted_n.size();
                if (sz % 2 == 1) {
                    res.median_val = sorted_n[sz / 2];
                } else {
                    res.median_val = (sorted_n[sz / 2 - 1] + sorted_n[sz / 2]) / 2;
                }
            }
        }
    }

    return res;
}

} // namespace greengate

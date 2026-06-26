#include "lex/jit/query_planner.hpp"
#include "lex/lsm/metadata_registry.hpp"
#include "lex/ingest/simd_transposer.hpp"
#include "lex/lsm/promotion_daemon.hpp"
#include <stdexcept>
#include <cstring>
#include <iostream>
#include <string_view>
#include <unordered_set>
#include <algorithm>

namespace greengate {

static uint64_t GenerateKimSignature(std::string_view str) {
    uint64_t hash = 0xcbf29ce484222325ULL; // FNV-1a offset basis
    if (str.empty()) return hash;
    if (str.size() == 1) {
        hash ^= static_cast<uint8_t>(str[0]);
        hash *= 0x100000001b3ULL; // FNV-1a prime
        return hash;
    }
    for (size_t i = 0; i < str.size() - 1; ++i) {
        uint32_t bigram = (static_cast<uint32_t>(static_cast<uint8_t>(str[i])) << 8) | 
                           static_cast<uint32_t>(static_cast<uint8_t>(str[i+1]));
        hash ^= bigram;
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

static uint64_t GetSkeletonConstant(std::string_view str) {
    std::string s(str);
    if (!s.empty() && s.back() == '%') {
        s.pop_back();
    }
    uint64_t val = 0;
    std::memcpy(&val, s.data(), std::min(size_t(4), s.size()));
    return val;
}

uint64_t QueryRunner::Execute(const RowGroup& rg, uint64_t& out_agg_sum) {
    uint64_t total_matches = 0;
    out_agg_sum = 0;
    
    alignas(64) uint64_t materialized_dest[64];
    
    size_t num_blocks = rg.blocks.size();
    for (size_t b = 0; b < num_blocks; ++b) {
        const auto& block = rg.blocks[b];
        size_t rows_in_block = std::min(size_t(64), rg.num_rows - b * 64);
        
        // 1. Run JIT block scan (filters numerical, and checks skeleton + KIM signature)
        uint64_t candidate_mask = scan_func(block.columns.data(), block.tail_payload.data(), materialized_dest, block.delete_mask);
        
        // Mask out rows beyond rows_in_block (for the last block)
        if (rows_in_block < 64) {
            uint64_t valid_rows_mask = (1ULL << rows_in_block) - 1;
            candidate_mask &= valid_rows_mask;
        }
        
        if (candidate_mask == 0) continue;
        
        // 2. Deferred Verification: verify candidate matches against the tail payload using bitmask iteration
        uint64_t final_mask = 0;
        
        const char* tail_ptr = block.tail_payload.data();
        uint64_t num_strings = 0;
        const uint64_t* offsets = nullptr;
        const char* data_start = nullptr;
        if (tail_ptr) {
            std::memcpy(&num_strings, tail_ptr, sizeof(uint64_t));
            offsets = reinterpret_cast<const uint64_t*>(tail_ptr + sizeof(uint64_t));
            data_start = tail_ptr + sizeof(uint64_t) + (num_strings + 1) * sizeof(uint64_t);
        }
        
        if (string_predicates.empty()) {
            final_mask = candidate_mask;
        } else if (string_predicates.size() == 1) {
            const auto& pred = string_predicates[0];
            std::string_view clean_pat = clean_string_patterns[0];
            size_t base_idx = string_col_indices[0] * rows_in_block;
            
            if (pred.op == PredicateOp::EQ) {
                uint64_t temp_mask = candidate_mask;
                const uint64_t* block_offsets = &offsets[base_idx];
                size_t pat_len = clean_pat.size();
                const char* pat_data = clean_pat.data();
                
                uint64_t pat_const = 0;
                if (pat_len < 8) {
                    std::memcpy(&pat_const, pat_data, pat_len);
                }
                uint64_t pat_mask = (pat_len >= 8) ? 0 : (1ULL << (pat_len * 8)) - 1;
                
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = block_offsets[i];
                    uint64_t end_off = block_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (len == pat_len) {
                        if (pat_len < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            if ((val & pat_mask) == pat_const) {
                                final_mask |= (1ULL << i);
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, pat_data, pat_len) == 0) {
                                final_mask |= (1ULL << i);
                            }
                        }
                    }
                }
            } else if (pred.op == PredicateOp::LIKE_PREFIX) {
                uint64_t temp_mask = candidate_mask;
                const uint64_t* block_offsets = &offsets[base_idx];
                size_t pat_len = clean_pat.size();
                const char* pat_data = clean_pat.data();
                
                uint64_t pat_const = 0;
                if (pat_len < 8) {
                    std::memcpy(&pat_const, pat_data, pat_len);
                }
                uint64_t pat_mask = (pat_len >= 8) ? 0 : (1ULL << (pat_len * 8)) - 1;
                
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = block_offsets[i];
                    uint64_t end_off = block_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (len >= pat_len) {
                        if (pat_len < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            if ((val & pat_mask) == pat_const) {
                                final_mask |= (1ULL << i);
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, pat_data, pat_len) == 0) {
                                final_mask |= (1ULL << i);
                            }
                        }
                    }
                }
            } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                uint64_t temp_mask = candidate_mask;
                const uint64_t* block_offsets = &offsets[base_idx];
                size_t pat_len = clean_pat.size();
                
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = block_offsets[i];
                    uint64_t end_off = block_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (len >= pat_len) {
                        std::string_view actual_val(data_start + start_off, len);
                        if (actual_val.find(clean_pat) != std::string_view::npos) {
                            final_mask |= (1ULL << i);
                        }
                    }
                }
            }
        } else {
            uint64_t temp_mask = candidate_mask;
            while (temp_mask > 0) {
                int i = __builtin_ctzll(temp_mask);
                temp_mask &= temp_mask - 1;
                
                bool matched = true;
                for (size_t p = 0; p < string_predicates.size(); ++p) {
                    const auto& pred = string_predicates[p];
                    const auto& clean_pat = clean_string_patterns[p];
                    size_t str_col_idx = string_col_indices[p];
                    
                    const uint64_t* col_offsets = &offsets[str_col_idx * rows_in_block];
                    uint64_t start_off = col_offsets[i];
                    uint64_t end_off = col_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (pred.op == PredicateOp::EQ) {
                        if (len != clean_pat.size()) {
                            matched = false;
                            break;
                        }
                        if (len < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            uint64_t pat_const = 0;
                            std::memcpy(&pat_const, clean_pat.data(), len);
                            uint64_t pat_mask = (1ULL << (len * 8)) - 1;
                            if ((val & pat_mask) != pat_const) {
                                matched = false;
                                break;
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, clean_pat.data(), len) != 0) {
                                matched = false;
                                break;
                            }
                        }
                    } else if (pred.op == PredicateOp::LIKE_PREFIX) {
                        if (len < clean_pat.size()) {
                            matched = false;
                            break;
                        }
                        if (clean_pat.size() < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            uint64_t pat_const = 0;
                            std::memcpy(&pat_const, clean_pat.data(), clean_pat.size());
                            uint64_t pat_mask = (1ULL << (clean_pat.size() * 8)) - 1;
                            if ((val & pat_mask) != pat_const) {
                                matched = false;
                                break;
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, clean_pat.data(), clean_pat.size()) != 0) {
                                matched = false;
                                break;
                            }
                        }
                    } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                        if (len < clean_pat.size() || std::string_view(data_start + start_off, len).find(clean_pat) == std::string_view::npos) {
                            matched = false;
                            break;
                        }
                    }
                }
                if (matched) {
                    final_mask |= (1ULL << i);
                }
            }
        }
        
        // 3. Count matches
        total_matches += __builtin_popcountll(final_mask);
        
        // 4. Perform aggregation if requested using fast bit-plane popcounts
        if (agg_col_idx != -1 && final_mask != 0) {
            uint64_t block_sum = 0;
            int block_max_bit = max_agg_bit;
            while (block_max_bit > 0 && block.columns[agg_col_idx].planes[block_max_bit] == 0) {
                block_max_bit--;
            }
            
            const uint64_t* planes = block.columns[agg_col_idx].planes;
            int bit = 0;
            for (; bit <= block_max_bit - 3; bit += 4) {
                uint64_t m0 = final_mask & planes[bit];
                uint64_t m1 = final_mask & planes[bit + 1];
                uint64_t m2 = final_mask & planes[bit + 2];
                uint64_t m3 = final_mask & planes[bit + 3];
                
                block_sum += static_cast<uint64_t>(__builtin_popcountll(m0)) << bit;
                block_sum += static_cast<uint64_t>(__builtin_popcountll(m1)) << (bit + 1);
                block_sum += static_cast<uint64_t>(__builtin_popcountll(m2)) << (bit + 2);
                block_sum += static_cast<uint64_t>(__builtin_popcountll(m3)) << (bit + 3);
            }
            for (; bit <= block_max_bit; ++bit) {
                uint64_t m = final_mask & planes[bit];
                block_sum += static_cast<uint64_t>(__builtin_popcountll(m)) << bit;
            }
            out_agg_sum += block_sum;
        }
    }
    
    return total_matches;
}

QueryResult QueryRunner::ExecuteQueryResult(const RowGroup& rg) {
    QueryResult res;
    res.count = 0;
    res.sum = 0;
    
    std::vector<int64_t> matched_numerics;
    std::vector<std::string> matched_strings;
    
    alignas(64) uint64_t materialized_dest[64];
    size_t num_blocks = rg.blocks.size();
    
    bool is_agg_str = false;
    size_t agg_str_col_idx = 0;
    
    if (agg_col_idx != -1) {
        if (rg.column_types[agg_col_idx] == 13) {
            is_agg_str = true;
            size_t str_count = 0;
            for (int i = 0; i < agg_col_idx; ++i) {
                if (rg.column_types[i] == 13) {
                    str_count++;
                }
            }
            agg_str_col_idx = str_count;
        }
    }
    
    for (size_t b = 0; b < num_blocks; ++b) {
        const auto& block = rg.blocks[b];
        size_t rows_in_block = std::min(size_t(64), rg.num_rows - b * 64);
        
        uint64_t candidate_mask = scan_func(block.columns.data(), block.tail_payload.data(), materialized_dest, block.delete_mask);
        
        if (rows_in_block < 64) {
            uint64_t valid_rows_mask = (1ULL << rows_in_block) - 1;
            candidate_mask &= valid_rows_mask;
        }
        
        if (candidate_mask == 0) continue;
        
        uint64_t final_mask = 0;
        const char* tail_ptr = block.tail_payload.data();
        uint64_t num_strings = 0;
        const uint64_t* offsets = nullptr;
        const char* data_start = nullptr;
        if (tail_ptr) {
            std::memcpy(&num_strings, tail_ptr, sizeof(uint64_t));
            offsets = reinterpret_cast<const uint64_t*>(tail_ptr + sizeof(uint64_t));
            data_start = tail_ptr + sizeof(uint64_t) + (num_strings + 1) * sizeof(uint64_t);
        }
        
        if (string_predicates.empty()) {
            final_mask = candidate_mask;
        } else if (string_predicates.size() == 1) {
            const auto& pred = string_predicates[0];
            std::string_view clean_pat = clean_string_patterns[0];
            size_t base_idx = string_col_indices[0] * rows_in_block;
            
            if (pred.op == PredicateOp::EQ) {
                uint64_t temp_mask = candidate_mask;
                const uint64_t* block_offsets = &offsets[base_idx];
                size_t pat_len = clean_pat.size();
                const char* pat_data = clean_pat.data();
                
                uint64_t pat_const = 0;
                if (pat_len < 8) {
                    std::memcpy(&pat_const, pat_data, pat_len);
                }
                uint64_t pat_mask = (pat_len >= 8) ? 0 : (1ULL << (pat_len * 8)) - 1;
                
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = block_offsets[i];
                    uint64_t end_off = block_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (len == pat_len) {
                        if (pat_len < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            if ((val & pat_mask) == pat_const) {
                                final_mask |= (1ULL << i);
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, pat_data, pat_len) == 0) {
                                final_mask |= (1ULL << i);
                            }
                        }
                    }
                }
            } else if (pred.op == PredicateOp::LIKE_PREFIX) {
                uint64_t temp_mask = candidate_mask;
                const uint64_t* block_offsets = &offsets[base_idx];
                size_t pat_len = clean_pat.size();
                const char* pat_data = clean_pat.data();
                
                uint64_t pat_const = 0;
                if (pat_len < 8) {
                    std::memcpy(&pat_const, pat_data, pat_len);
                }
                uint64_t pat_mask = (pat_len >= 8) ? 0 : (1ULL << (pat_len * 8)) - 1;
                
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = block_offsets[i];
                    uint64_t end_off = block_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (len >= pat_len) {
                        if (pat_len < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            if ((val & pat_mask) == pat_const) {
                                final_mask |= (1ULL << i);
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, pat_data, pat_len) == 0) {
                                final_mask |= (1ULL << i);
                            }
                        }
                    }
                }
            } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                uint64_t temp_mask = candidate_mask;
                const uint64_t* block_offsets = &offsets[base_idx];
                size_t pat_len = clean_pat.size();
                
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = block_offsets[i];
                    uint64_t end_off = block_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (len >= pat_len) {
                        std::string_view actual_val(data_start + start_off, len);
                        if (actual_val.find(clean_pat) != std::string_view::npos) {
                            final_mask |= (1ULL << i);
                        }
                    }
                }
            }
        } else {
            uint64_t temp_mask = candidate_mask;
            while (temp_mask > 0) {
                int i = __builtin_ctzll(temp_mask);
                temp_mask &= temp_mask - 1;
                
                bool matched = true;
                for (size_t p = 0; p < string_predicates.size(); ++p) {
                    const auto& pred = string_predicates[p];
                    const auto& clean_pat = clean_string_patterns[p];
                    size_t str_col_idx = string_col_indices[p];
                    
                    const uint64_t* col_offsets = &offsets[str_col_idx * rows_in_block];
                    uint64_t start_off = col_offsets[i];
                    uint64_t end_off = col_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    
                    if (pred.op == PredicateOp::EQ) {
                        if (len != clean_pat.size()) {
                            matched = false;
                            break;
                        }
                        if (len < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            uint64_t pat_const = 0;
                            std::memcpy(&pat_const, clean_pat.data(), len);
                            uint64_t pat_mask = (1ULL << (len * 8)) - 1;
                            if ((val & pat_mask) != pat_const) {
                                matched = false;
                                break;
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, clean_pat.data(), len) != 0) {
                                matched = false;
                                break;
                            }
                        }
                    } else if (pred.op == PredicateOp::LIKE_PREFIX) {
                        if (len < clean_pat.size()) {
                            matched = false;
                            break;
                        }
                        if (clean_pat.size() < 8) {
                            uint64_t val = *reinterpret_cast<const uint64_t*>(data_start + start_off);
                            uint64_t pat_const = 0;
                            std::memcpy(&pat_const, clean_pat.data(), clean_pat.size());
                            uint64_t pat_mask = (1ULL << (clean_pat.size() * 8)) - 1;
                            if ((val & pat_mask) != pat_const) {
                                matched = false;
                                break;
                            }
                        } else {
                            if (std::memcmp(data_start + start_off, clean_pat.data(), clean_pat.size()) != 0) {
                                matched = false;
                                break;
                            }
                        }
                    } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                        if (len < clean_pat.size() || std::string_view(data_start + start_off, len).find(clean_pat) == std::string_view::npos) {
                            matched = false;
                            break;
                        }
                    }
                }
                if (matched) {
                    final_mask |= (1ULL << i);
                }
            }
        }
        
        uint64_t pop = __builtin_popcountll(final_mask);
        res.count += pop;
        if (pop == 0) continue;
        
        if (agg_col_idx != -1) {
            if (is_agg_str) {
                const uint64_t* col_offsets = &offsets[agg_str_col_idx * rows_in_block];
                uint64_t temp_mask = final_mask;
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    
                    uint64_t start_off = col_offsets[i];
                    uint64_t end_off = col_offsets[i + 1];
                    uint64_t len = end_off - start_off - 1;
                    matched_strings.push_back(std::string(data_start + start_off, len));
                }
            } else {
                greengate_butterfly_transpose(block.columns[agg_col_idx].planes, materialized_dest);
                uint64_t temp_mask = final_mask;
                while (temp_mask > 0) {
                    int i = __builtin_ctzll(temp_mask);
                    temp_mask &= temp_mask - 1;
                    int64_t val = static_cast<int64_t>(materialized_dest[i]);
                    matched_numerics.push_back(val);
                    res.sum += val;
                }
            }
        }
    }
    
    if (res.count > 0 && agg_col_idx != -1) {
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

QueryResult QueryRunner::ExecuteQueryResult(const TableState& state) {
    // Record queries in the auto-pilot system
    for (const auto& pred : all_predicates) {
        PromotionDaemon::RecordColumnAccess(pred.column_name);
    }
    if (!agg_col_name.empty()) {
        PromotionDaemon::RecordColumnAccess(agg_col_name);
    }

    std::vector<QueryResult> results;
    
    for (const auto& rg : state.persisted_groups) {
        results.push_back(ExecuteQueryResult(*rg));
    }
    
    if (state.active_memtable) {
        results.push_back(state.active_memtable->ExecuteQueryResult(all_predicates, agg_col_name));
    }
    
    if (state.frozen_memtable) {
        results.push_back(state.frozen_memtable->ExecuteQueryResult(all_predicates, agg_col_name));
    }
    
    QueryResult merged;
    if (results.empty()) return merged;
    
    merged.count = 0;
    merged.sum = 0;
    bool first = true;
    bool is_string = false;
    
    if (agg_col_idx != -1) {
        if (!state.persisted_groups.empty() && state.persisted_groups[0]->column_types[agg_col_idx] == 13) {
            is_string = true;
        } else if (state.active_memtable) {
            auto batches = state.active_memtable->GetBatches();
            if (!batches.empty()) {
                auto col = batches[0]->GetColumnByName(agg_col_name);
                if (col && (col->type_id() == arrow::Type::STRING || col->type_id() == arrow::Type::BINARY)) {
                    is_string = true;
                }
            }
        }
    }
    
    std::vector<int64_t> all_numerics;
    std::vector<std::string> all_strings;
    
    for (const auto& r : results) {
        merged.count += r.count;
        merged.sum += r.sum;
        if (r.count > 0) {
            if (first) {
                merged.min_val = r.min_val;
                merged.max_val = r.max_val;
                first = false;
            } else {
                if (r.min_val < merged.min_val) merged.min_val = r.min_val;
                if (r.max_val > merged.max_val) merged.max_val = r.max_val;
            }
            all_numerics.insert(all_numerics.end(), r.distinct_numerics.begin(), r.distinct_numerics.end());
            all_strings.insert(all_strings.end(), r.distinct_strings.begin(), r.distinct_strings.end());
        }
    }
    
    if (merged.count > 0) {
        if (!is_string) {
            if (all_numerics.size() > 0) {
                merged.avg = static_cast<double>(merged.sum) / all_numerics.size();
            }
            
            std::unordered_set<int64_t> dist_set(all_numerics.begin(), all_numerics.end());
            merged.distinct_count = dist_set.size();
            merged.distinct_numerics.assign(dist_set.begin(), dist_set.end());
            
            std::sort(all_numerics.begin(), all_numerics.end());
            size_t sz = all_numerics.size();
            if (sz > 0) {
                if (sz % 2 == 1) {
                    merged.median_val = all_numerics[sz / 2];
                } else {
                    merged.median_val = (all_numerics[sz / 2 - 1] + all_numerics[sz / 2]) / 2;
                }
            }
        } else {
            std::unordered_set<std::string> dist_set(all_strings.begin(), all_strings.end());
            merged.distinct_count = dist_set.size();
            merged.distinct_strings.assign(dist_set.begin(), dist_set.end());
        }
    }
    
    return merged;
}

uint64_t QueryRunner::Execute(const TableState& state, uint64_t& out_agg_sum) {
    uint64_t total_matches = 0;
    out_agg_sum = 0;

    // 1. Scan persisted RowGroups
    for (const auto& rg : state.persisted_groups) {
        uint64_t local_sum = 0;
        total_matches += Execute(*rg, local_sum);
        out_agg_sum += local_sum;
    }

    // 2. Scan active MemTable
    if (state.active_memtable) {
        uint64_t local_sum = 0;
        total_matches += state.active_memtable->ExecuteQuery(all_predicates, agg_col_name, local_sum);
        out_agg_sum += local_sum;
    }

    // 3. Scan frozen MemTable (if exists during concurrent compaction)
    if (state.frozen_memtable) {
        uint64_t local_sum = 0;
        total_matches += state.frozen_memtable->ExecuteQuery(all_predicates, agg_col_name, local_sum);
        out_agg_sum += local_sum;
    }

    return total_matches;
}

QueryRunner QueryPlanner::Plan(const std::vector<Predicate>& predicates, 
                              const RowGroup& rg, 
                              const std::string& agg_column_name,
                              AggregationType agg_type) {
    QueryRunner runner;
    runner.agg_type = agg_type;
    runner.all_predicates = predicates;
    std::vector<MicroOp> ops;
    
    // Identify all string columns in the original schema (to map their relative index)
    std::vector<std::string> logical_col_names;
    std::vector<bool> logical_col_is_str;
    
    for (size_t i = 0; i < rg.column_names.size(); ++i) {
        std::string name = rg.column_names[i];
        if (i + 1 < rg.column_names.size() && rg.column_names[i + 1] == name + "_kim") {
            logical_col_names.push_back(name);
            logical_col_is_str.push_back(true);
            i++; // Skip the _kim column
        } else {
            logical_col_names.push_back(name);
            logical_col_is_str.push_back(false);
        }
    }
    
    auto get_str_col_idx = [&](const std::string& col_name) -> size_t {
        size_t str_count = 0;
        for (size_t i = 0; i < logical_col_names.size(); ++i) {
            if (logical_col_names[i] == col_name) {
                return str_count;
            }
            if (logical_col_is_str[i]) {
                str_count++;
            }
        }
        throw std::runtime_error("Column not found: " + col_name);
    };
    
    auto get_physical_idx = [&](const std::string& col_name) -> size_t {
        for (size_t i = 0; i < rg.column_names.size(); ++i) {
            if (rg.column_names[i] == col_name) {
                return i;
            }
        }
        throw std::runtime_error("Physical column not found: " + col_name);
    };

    std::cout << "[QueryPlanner Debug] Physical columns in RowGroup:" << std::endl;
    for (size_t i = 0; i < rg.column_names.size(); ++i) {
        std::cout << "  " << i << ": " << rg.column_names[i] << " (type " << rg.column_types[i] << ")" << std::endl;
    }
    
    int vreg_counter = 0;
    std::vector<int> predicate_mask_regs;
    
    for (const auto& pred : predicates) {
        std::cout << "[QueryPlanner Debug] Predicate on column " << pred.column_name 
                  << " is_string=" << pred.is_string 
                  << " op=" << static_cast<int>(pred.op) 
                  << " val='" << pred.value << "' num=" << pred.numeric_value << std::endl;
        if (pred.is_string) {
            size_t p_skeleton = get_physical_idx(pred.column_name);
            size_t p_kim = get_physical_idx(pred.column_name + "_kim");
            size_t str_col_idx = get_str_col_idx(pred.column_name);
            
            runner.string_predicates.push_back(pred);
            runner.string_col_indices.push_back(str_col_idx);
            
            std::string clean_pat = pred.value;
            if (pred.op == PredicateOp::LIKE_PREFIX) {
                if (!clean_pat.empty() && clean_pat.back() == '%') {
                    clean_pat.pop_back();
                }
            } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                if (!clean_pat.empty() && clean_pat.front() == '%') {
                    clean_pat.erase(0, 1);
                }
                if (!clean_pat.empty() && clean_pat.back() == '%') {
                    clean_pat.pop_back();
                }
            }
            runner.clean_string_patterns.push_back(clean_pat);
            
            int vreg_skel_tile = vreg_counter++;
            ops.push_back({Opcode::LOAD_TILE, vreg_skel_tile, -1, -1, p_skeleton});
            std::cout << "  EMIT LOAD_TILE dest=" << vreg_skel_tile << " col=" << p_skeleton << std::endl;
            
            if (pred.op == PredicateOp::EQ) {
                int vreg_kim_tile = vreg_counter++;
                ops.push_back({Opcode::LOAD_TILE, vreg_kim_tile, -1, -1, p_kim});
                std::cout << "  EMIT LOAD_TILE dest=" << vreg_kim_tile << " col=" << p_kim << std::endl;
                
                int vreg_skel_match = vreg_counter++;
                uint64_t skel_const = GetSkeletonConstant(pred.value);
                int skel_bits = std::min(size_t(32), (clean_pat.size() + 1) * 8);
                ops.push_back({Opcode::CMP_SKELETON, vreg_skel_match, vreg_skel_tile, skel_bits, skel_const});
                std::cout << "  EMIT CMP_SKELETON dest=" << vreg_skel_match << " src1=" << vreg_skel_tile << " imm=" << skel_const << " bits=" << skel_bits << std::endl;
                
                int vreg_kim_match = vreg_counter++;
                uint64_t kim_const = GenerateKimSignature(pred.value);
                ops.push_back({Opcode::CMP_KIM, vreg_kim_match, vreg_kim_tile, 32, kim_const});
                std::cout << "  EMIT CMP_KIM dest=" << vreg_kim_match << " src1=" << vreg_kim_tile << " imm=" << kim_const << " bits=32" << std::endl;
                
                int vreg_final_match = vreg_counter++;
                ops.push_back({Opcode::BIT_AND, vreg_final_match, vreg_skel_match, vreg_kim_match});
                std::cout << "  EMIT BIT_AND dest=" << vreg_final_match << " src1=" << vreg_skel_match << " src2=" << vreg_kim_match << std::endl;
                
                predicate_mask_regs.push_back(vreg_final_match);
            } else if (pred.op == PredicateOp::LIKE_PREFIX) {
                int vreg_skel_match = vreg_counter++;
                uint64_t skel_const = GetSkeletonConstant(pred.value);
                int skel_bits = std::min(size_t(32), clean_pat.size() * 8);
                ops.push_back({Opcode::CMP_SKELETON, vreg_skel_match, vreg_skel_tile, skel_bits, skel_const});
                std::cout << "  EMIT CMP_SKELETON dest=" << vreg_skel_match << " src1=" << vreg_skel_tile << " imm=" << skel_const << " bits=" << skel_bits << std::endl;
                
                predicate_mask_regs.push_back(vreg_skel_match);
            } else if (pred.op == PredicateOp::LIKE_CONTAINS) {
                int vreg_kim_tile = vreg_counter++;
                ops.push_back({Opcode::LOAD_TILE, vreg_kim_tile, -1, -1, p_kim});
                std::cout << "  EMIT LOAD_TILE dest=" << vreg_kim_tile << " col=" << p_kim << std::endl;
                
                int vreg_kim_match = vreg_counter++;
                std::string sub = pred.value;
                if (!sub.empty() && sub.front() == '%') sub.erase(0, 1);
                if (!sub.empty() && sub.back() == '%') sub.pop_back();
                uint64_t kim_const = GenerateKimSignature(sub);
                
                ops.push_back({Opcode::CMP_KIM, vreg_kim_match, vreg_kim_tile, 32, kim_const});
                std::cout << "  EMIT CMP_KIM dest=" << vreg_kim_match << " src1=" << vreg_kim_tile << " imm=" << kim_const << " bits=32" << std::endl;
                predicate_mask_regs.push_back(vreg_kim_match);
            }
        } else {
            size_t p = get_physical_idx(pred.column_name);
            int vreg_tile = vreg_counter++;
            ops.push_back({Opcode::LOAD_TILE, vreg_tile, -1, -1, p});
            std::cout << "  EMIT LOAD_TILE dest=" << vreg_tile << " col=" << p << std::endl;
            
            int vreg_match = vreg_counter++;
            Opcode cmp_op = Opcode::CMP_EQ;
            if (pred.op == PredicateOp::GT) cmp_op = Opcode::CMP_GT;
            else if (pred.op == PredicateOp::LT) cmp_op = Opcode::CMP_LT;
            
            int threshold_msb = 0;
            if (pred.numeric_value > 0) {
                threshold_msb = 63 - __builtin_clzll(pred.numeric_value);
            }
            int max_bit = 0;
            for (int bit = 63; bit >= 0; --bit) {
                bool plane_active = false;
                for (const auto& block : rg.blocks) {
                    if (block.columns[p].planes[bit] != 0) {
                        plane_active = true;
                        break;
                    }
                }
                if (plane_active) {
                    max_bit = bit;
                    break;
                }
            }
            int num_compare_bits = std::max(1, std::max(max_bit, threshold_msb) + 1);
            std::cout << "  [QueryPlanner Debug] Column " << pred.column_name << " max_bit=" << max_bit 
                      << " threshold_msb=" << threshold_msb << " -> comparing " << num_compare_bits << " bits" << std::endl;

            ops.push_back({cmp_op, vreg_match, vreg_tile, num_compare_bits, pred.numeric_value});
            std::cout << "  EMIT CMP dest=" << vreg_match << " src1=" << vreg_tile << " imm=" << pred.numeric_value << std::endl;
            predicate_mask_regs.push_back(vreg_match);
        }
    }
    
    int vreg_final_mask = -1;
    if (!predicate_mask_regs.empty()) {
        vreg_final_mask = predicate_mask_regs[0];
        for (size_t i = 1; i < predicate_mask_regs.size(); ++i) {
            int new_mask = vreg_counter++;
            ops.push_back({Opcode::BIT_AND, new_mask, vreg_final_mask, predicate_mask_regs[i]});
            std::cout << "  EMIT BIT_AND dest=" << new_mask << " src1=" << vreg_final_mask << " src2=" << predicate_mask_regs[i] << std::endl;
            vreg_final_mask = new_mask;
        }
    } else {
        throw std::runtime_error("No predicates specified in QueryPlanner");
    }
    
    if (!agg_column_name.empty()) {
        size_t p_agg = get_physical_idx(agg_column_name);
        runner.agg_col_idx = static_cast<int>(p_agg);
        runner.agg_col_name = agg_column_name;
        
        int max_agg_bit = 0;
        for (int bit = 63; bit >= 0; --bit) {
            bool plane_active = false;
            for (const auto& block : rg.blocks) {
                if (block.columns[p_agg].planes[bit] != 0) {
                    plane_active = true;
                    break;
                }
            }
            if (plane_active) {
                max_agg_bit = bit;
                break;
            }
        }
        runner.max_agg_bit = max_agg_bit;
    }
    
    runner.scan_func = MicroOpCompiler::Compile(ops);
    return runner;
}

} // namespace greengate

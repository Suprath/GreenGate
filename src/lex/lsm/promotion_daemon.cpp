#include "lex/lsm/promotion_daemon.hpp"
#include "lex/ingest/adaptive_ingester.hpp"
#include "lex/ingest/simd_transposer.hpp"
#include <chrono>
#include <iostream>

extern "C" void greengate_butterfly_transpose(const uint64_t* in_planes, uint64_t* out_scalars) noexcept;

namespace greengate {

PromotionDaemon::PromotionDaemon(const std::string& table_name, size_t memory_threshold_bytes)
    : table_name_(table_name), memory_threshold_(memory_threshold_bytes) {}

PromotionDaemon::~PromotionDaemon() {
    Stop();
}

void PromotionDaemon::Start() {
    running_ = true;
    worker_thread_ = std::thread(&PromotionDaemon::DaemonLoop, this);
    std::cout << "[PromotionDaemon] Started compaction thread for table: " << table_name_ << std::endl;
}

void PromotionDaemon::Stop() {
    if (running_) {
        running_ = false;
        cv_.notify_all();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        std::cout << "[PromotionDaemon] Stopped compaction thread for table: " << table_name_ << std::endl;
    }
}

void PromotionDaemon::DaemonLoop() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, std::chrono::milliseconds(50), [this]() { return !running_; });
        if (!running_) break;

        auto state = MetadataRegistry::Instance().GetTableState(table_name_);
        if (state && state->active_memtable) {
            if (state->active_memtable->GetMemorySize() >= memory_threshold_) {
                std::cout << "[PromotionDaemon] Memory size threshold reached: " 
                          << state->active_memtable->GetMemorySize() << " >= " << memory_threshold_
                          << ". Triggering compaction." << std::endl;
                TriggerCompaction();
            }
        }
    }
}

void PromotionDaemon::TriggerCompaction() {
    auto state = MetadataRegistry::Instance().GetTableState(table_name_);
    if (!state) return;

    // Check if there is anything to compact
    if (state->active_memtable->GetRowCount() == 0 && state->persisted_groups.empty()) {
        return;
    }

    std::cout << "[PromotionDaemon] Freezing MemTable for compaction..." << std::endl;

    // 1. Freeze active MemTable
    auto freeze_state = std::make_shared<TableState>();
    freeze_state->table_name = state->table_name;
    freeze_state->column_names = state->column_names;
    freeze_state->column_types = state->column_types;
    freeze_state->persisted_groups = state->persisted_groups;
    freeze_state->active_memtable = std::make_shared<MemTable>();
    freeze_state->frozen_memtable = state->active_memtable;
    freeze_state->version = state->version + 1;

    MetadataRegistry::Instance().UpdateTableState(table_name_, freeze_state);

    // Refresh state pointer
    state = freeze_state;

    std::cout << "[PromotionDaemon] Commencing compaction scan on frozen MemTable and persisted RowGroups..." << std::endl;

    // 2. Reconstruct all alive rows from existing persisted RowGroups
    size_t num_logical_cols = state->column_names.size();
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders(num_logical_cols);
    for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
        if (state->column_types[col_idx] == 13) {
            builders[col_idx] = std::make_shared<arrow::StringBuilder>();
        } else {
            builders[col_idx] = std::make_shared<arrow::Int64Builder>();
        }
    }

    // Set up logical-to-physical mapping
    std::vector<size_t> logical_to_physical(num_logical_cols);
    size_t p = 0;
    for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
        logical_to_physical[col_idx] = p;
        if (state->column_types[col_idx] == 13) {
            p += 2; // skeleton + kim
        } else {
            p += 1; // numeric
        }
    }

    // Extract alive rows from persisted RowGroups
    uint64_t total_reconstructed = 0;
    for (const auto& rg : state->persisted_groups) {
        for (size_t b = 0; b < rg->blocks.size(); ++b) {
            const auto& block = rg->blocks[b];
            uint64_t delete_mask = block.delete_mask;
            size_t rows_in_block = std::min(size_t(64), rg->num_rows - b * 64);

            // Transpose numeric/planes columns back to scalar arrays
            std::vector<std::vector<uint64_t>> block_scalars(num_logical_cols, std::vector<uint64_t>(64));
            for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
                size_t p_idx = logical_to_physical[col_idx];
                greengate_butterfly_transpose(block.columns[p_idx].planes, block_scalars[col_idx].data());
            }

            // String tail offsets tracking
            std::vector<size_t> col_tail_str_start(num_logical_cols, 0);
            size_t current_str_offset = 0;
            for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
                if (state->column_types[col_idx] == 13) {
                    col_tail_str_start[col_idx] = current_str_offset;
                    size_t p_idx = logical_to_physical[col_idx];
                    uint64_t validity = block.columns[p_idx].validity;
                    for (size_t r = 0; r < rows_in_block; ++r) {
                        if ((validity & (1ULL << r)) != 0) {
                            current_str_offset++;
                        }
                    }
                }
            }

            const char* tail_ptr = block.tail_payload.data();
            uint64_t total_num_strings = 0;
            const uint64_t* offsets = nullptr;
            const char* data_start = nullptr;
            if (tail_ptr) {
                std::memcpy(&total_num_strings, tail_ptr, sizeof(uint64_t));
                offsets = reinterpret_cast<const uint64_t*>(tail_ptr + sizeof(uint64_t));
                data_start = tail_ptr + sizeof(uint64_t) + (total_num_strings + 1) * sizeof(uint64_t);
            }

            // Append alive rows to Builders
            for (size_t r = 0; r < rows_in_block; ++r) {
                if ((delete_mask & (1ULL << r)) != 0) {
                    // Row is alive
                    for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
                        size_t p_idx = logical_to_physical[col_idx];
                        bool is_valid = (block.columns[p_idx].validity & (1ULL << r)) != 0;

                        if (!is_valid) {
                            builders[col_idx]->AppendNull();
                        } else {
                            if (state->column_types[col_idx] == 13) {
                                auto s_builder = std::static_pointer_cast<arrow::StringBuilder>(builders[col_idx]);
                                size_t popcount = __builtin_popcountll(block.columns[p_idx].validity & ((1ULL << r) - 1));
                                size_t string_idx = col_tail_str_start[col_idx] + popcount;
                                uint64_t start_off = offsets[string_idx];
                                uint64_t end_off = offsets[string_idx + 1];
                                std::string_view str_val(data_start + start_off, end_off - start_off - 1);
                                s_builder->Append(std::string(str_val));
                            } else {
                                auto i_builder = std::static_pointer_cast<arrow::Int64Builder>(builders[col_idx]);
                                i_builder->Append(static_cast<int64_t>(block_scalars[col_idx][r]));
                            }
                        }
                    }
                    total_reconstructed++;
                }
            }
        }
    }

    // 3. Extract all rows from the frozen MemTable
    if (state->frozen_memtable) {
        auto batches = state->frozen_memtable->GetBatches();
        for (const auto& batch : batches) {
            size_t num_rows = batch->num_rows();
            for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
                auto col = batch->GetColumnByName(state->column_names[col_idx]);
                if (state->column_types[col_idx] == 13) {
                    auto s_builder = std::static_pointer_cast<arrow::StringBuilder>(builders[col_idx]);
                    auto str_arr = std::static_pointer_cast<arrow::StringArray>(col);
                    for (size_t r = 0; r < num_rows; ++r) {
                        if (str_arr->IsNull(r)) {
                            s_builder->AppendNull();
                        } else {
                            s_builder->Append(std::string(str_arr->GetView(r)));
                        }
                    }
                } else {
                    auto i_builder = std::static_pointer_cast<arrow::Int64Builder>(builders[col_idx]);
                    auto int_arr = std::static_pointer_cast<arrow::Int64Array>(col);
                    for (size_t r = 0; r < num_rows; ++r) {
                        if (int_arr->IsNull(r)) {
                            i_builder->AppendNull();
                        } else {
                            i_builder->Append(int_arr->Value(r));
                        }
                    }
                }
            }
            total_reconstructed += num_rows;
        }
    }

    std::cout << "[PromotionDaemon] Reconstructed " << total_reconstructed << " alive rows for compaction." << std::endl;

    // 4. Build Arrow Arrays and final RecordBatch
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_logical_cols);
    for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
        builders[col_idx]->Finish(&arrays[col_idx]);
    }

    arrow::FieldVector fields;
    for (size_t col_idx = 0; col_idx < num_logical_cols; ++col_idx) {
        if (state->column_types[col_idx] == 13) {
            fields.push_back(arrow::field(state->column_names[col_idx], arrow::utf8()));
        } else {
            fields.push_back(arrow::field(state->column_names[col_idx], arrow::int64()));
        }
    }
    auto schema = arrow::schema(fields);
    auto new_batch = arrow::RecordBatch::Make(schema, total_reconstructed, arrays);

    // 5. Ingest into fresh, clean CBST tiles
    AdaptiveIngester ingester;
    RowGroup compacted_rg = ingester.Ingest(new_batch);
    auto compacted_rg_ptr = std::make_shared<RowGroup>(compacted_rg);

    // 6. Update table state atomically (replace old persisted groups and clear frozen MemTable)
    auto compact_state = std::make_shared<TableState>();
    compact_state->table_name = state->table_name;
    compact_state->column_names = state->column_names;
    compact_state->column_types = state->column_types;
    if (compacted_rg_ptr->num_rows > 0) {
        compact_state->persisted_groups.push_back(compacted_rg_ptr);
    }
    compact_state->active_memtable = state->active_memtable; // Keep incoming writes in active
    compact_state->frozen_memtable = nullptr;
    compact_state->version = state->version + 1;

    MetadataRegistry::Instance().UpdateTableState(table_name_, compact_state);
    std::cout << "[PromotionDaemon] Compaction succeeded. New persisted RowGroup has " 
              << compacted_rg_ptr->num_rows << " rows (" << compacted_rg_ptr->blocks.size() << " blocks)." << std::endl;
}

} // namespace greengate

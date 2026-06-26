#include "lex/lsm/metadata_registry.hpp"
#include <stdexcept>
#include <iostream>

namespace greengate {

void MetadataRegistry::RegisterTable(const std::string& table_name, 
                                   const std::vector<std::string>& column_names, 
                                   const std::vector<uint32_t>& column_types) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto state = std::make_shared<TableState>();
    state->table_name = table_name;
    state->column_names = column_names;
    state->column_types = column_types;
    state->active_memtable = std::make_shared<MemTable>();
    state->version = 1;
    
    tables_[table_name] = state;
    std::cout << "[MetadataRegistry] Registered table: " << table_name << " (version " << state->version << ")" << std::endl;
}

std::shared_ptr<TableState> MetadataRegistry::GetTableState(const std::string& table_name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return nullptr;
    }
    return it->second;
}

void MetadataRegistry::UpdateTableState(const std::string& table_name, std::shared_ptr<TableState> new_state) {
    std::lock_guard<std::mutex> lock(mutex_);
    tables_[table_name] = new_state;
    std::cout << "[MetadataRegistry] Updated table state for: " << table_name << " (new version " << new_state->version << ")" << std::endl;
}

void MetadataRegistry::InsertBatch(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto state = GetTableState(table_name);
    if (!state) {
        throw std::runtime_error("Table not registered: " + table_name);
    }
    state->active_memtable->Append(batch);
}

void MetadataRegistry::DeleteRow(const std::string& table_name, uint64_t global_row_idx) {
    auto state = GetTableState(table_name);
    if (!state) {
        throw std::runtime_error("Table not registered: " + table_name);
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 1. Check if the row falls in the persisted RowGroups
    uint64_t current_base = 0;
    bool found = false;
    
    for (auto& rg : state->persisted_groups) {
        if (global_row_idx >= current_base && global_row_idx < current_base + rg->num_rows) {
            uint64_t local_idx = global_row_idx - current_base;
            size_t block_idx = local_idx / 64;
            size_t bit_idx = local_idx % 64;
            
            rg->blocks[block_idx].delete_mask &= ~(1ULL << bit_idx);
            found = true;
            std::cout << "[MetadataRegistry] Logically deleted row " << global_row_idx 
                      << " (Local: " << local_idx << ") in persisted RowGroup" << std::endl;
            break;
        }
        current_base += rg->num_rows;
    }
    
    // If not found in persisted RowGroups, we mark it as deleted (using a logical index mapping or we'll filter it during scan)
    // For now we don't need a complex tombstone storage for memtable in the registry since compaction will flush it,
    // but we can record it if needed.
}

} // namespace greengate

#include "lex/distributed/global_metadata.hpp"
#include <iostream>

namespace greengate {

void GlobalMetadata::RegisterTable(const std::string& table_name, 
                                   const std::vector<std::string>& column_names, 
                                   const std::vector<uint32_t>& column_types) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Create global table state
    auto state = std::make_shared<TableState>();
    state->table_name = table_name;
    state->column_names = column_names;
    state->column_types = column_types;
    state->active_memtable = std::make_shared<MemTable>();
    state->version = 1;

    global_tables_[table_name] = state;

    // Register locally as well
    MetadataRegistry::Instance().RegisterTable(table_name, column_names, column_types);
    std::cout << "[GlobalMetadata] Registered table globally: " << table_name << std::endl;
}

std::shared_ptr<TableState> GlobalMetadata::GetGlobalState(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = global_tables_.find(table_name);
    if (it == global_tables_.end()) {
        return nullptr;
    }
    return it->second;
}

void GlobalMetadata::UpdateGlobalState(const std::string& table_name, std::shared_ptr<TableState> new_state) {
    std::lock_guard<std::mutex> lock(mutex_);
    global_tables_[table_name] = new_state;
    std::cout << "[GlobalMetadata] Published new state globally for: " << table_name << " (version " << new_state->version << ")" << std::endl;
}

void GlobalMetadata::SyncLocalNode(int node_id, const std::string& table_name) {
    std::shared_ptr<TableState> global_state = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = global_tables_.find(table_name);
        if (it != global_tables_.end()) {
            global_state = it->second;
        }
    }

    if (global_state) {
        // Sync local node's local MetadataRegistry with this snapshot
        MetadataRegistry::Instance().UpdateTableState(table_name, global_state);
        std::cout << "[GlobalMetadata] Synchronized node " << node_id << " to version " << global_state->version << std::endl;
    }
}

void GlobalMetadata::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    global_tables_.clear();
}

} // namespace greengate

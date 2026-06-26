#pragma once
#include "lex/lsm/metadata_registry.hpp"
#include <unordered_map>
#include <mutex>
#include <string>
#include <memory>

namespace greengate {

class GlobalMetadata {
public:
    static GlobalMetadata& Instance() {
        static GlobalMetadata instance;
        return instance;
    }

    void RegisterTable(const std::string& table_name, 
                       const std::vector<std::string>& column_names, 
                       const std::vector<uint32_t>& column_types);

    // Retrieve the current global table state snapshot
    std::shared_ptr<TableState> GetGlobalState(const std::string& table_name);

    // Publish a new table state globally
    void UpdateGlobalState(const std::string& table_name, std::shared_ptr<TableState> new_state);

    // Synchronize a specific local node's MetadataRegistry state with the global metadata backplane
    void SyncLocalNode(int node_id, const std::string& table_name);

    // Clear all entries (for test isolation)
    void Clear();

private:
    GlobalMetadata() = default;
    ~GlobalMetadata() = default;
    GlobalMetadata(const GlobalMetadata&) = delete;
    GlobalMetadata& operator=(const GlobalMetadata&) = delete;

    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<TableState>> global_tables_;
};

} // namespace greengate

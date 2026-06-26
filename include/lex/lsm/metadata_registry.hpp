#pragma once
#include "lex/storage/row_group.hpp"
#include "lex/lsm/mem_table.hpp"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>

namespace greengate {

struct TableState {
    std::string table_name;
    std::vector<std::string> column_names;
    std::vector<uint32_t> column_types;

    // Active persisted RowGroups (loaded in memory or referenced by filepath)
    std::vector<std::shared_ptr<RowGroup>> persisted_groups;

    // MemTables
    std::shared_ptr<MemTable> active_memtable;
    std::shared_ptr<MemTable> frozen_memtable;

    uint64_t version = 0;
};

class MetadataRegistry {
public:
    static MetadataRegistry& Instance() {
        static MetadataRegistry instance;
        return instance;
    }

    void RegisterTable(const std::string& table_name, 
                       const std::vector<std::string>& column_names, 
                       const std::vector<uint32_t>& column_types);

    std::shared_ptr<TableState> GetTableState(const std::string& table_name) const;
    void UpdateTableState(const std::string& table_name, std::shared_ptr<TableState> new_state);

    // Dynamic operations
    void InsertBatch(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch);
    void DeleteRow(const std::string& table_name, uint64_t global_row_idx);

private:
    MetadataRegistry() = default;
    ~MetadataRegistry() = default;
    MetadataRegistry(const MetadataRegistry&) = delete;
    MetadataRegistry& operator=(const MetadataRegistry&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<TableState>> tables_;
};

} // namespace greengate

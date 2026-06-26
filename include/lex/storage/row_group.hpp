#pragma once
#include "lex/storage/cbst_tile.hpp"
#include <vector>
#include <string>

namespace greengate {

struct BlockData {
    std::vector<CbstTile> columns; // One tile per column in this block
    std::vector<char> tail_payload; // Padded raw string bytes / variable-length data
    uint64_t delete_mask = ~0ULL;   // 64-bit tombstone mask (1 = active, 0 = deleted)
};

struct RowGroup {
    uint64_t num_rows;
    uint64_t num_columns;
    std::vector<std::string> column_names;
    std::vector<uint32_t> column_types; // Maps to AarchGate/Apex types
    std::vector<BlockData> blocks;
};

} // namespace greengate

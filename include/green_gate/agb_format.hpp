#pragma once
#include <cstdint>

namespace greengate {

constexpr char AGB_MAGIC[4] = {'A', 'G', 'B', 1};

struct AgbField {
    char name[64];
    uint32_t type_id; // Maps to AarchGate/Apex core::DataType or equivalent (e.g. UINT64)
};

struct AgbHeader {
    char magic[4];
    uint64_t num_records;
    uint64_t num_fields;
    uint64_t num_blocks;
    // Followed contiguously by AgbField fields[num_fields]
    // Followed contiguously by uint64_t bit_planes[num_blocks * num_fields * 64]
};

} // namespace greengate

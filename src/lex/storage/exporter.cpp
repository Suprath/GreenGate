#include "lex/storage/exporter.hpp"
#include <fstream>
#include <stdexcept>
#include <cstring>

namespace greengate {

// Magic prefix for CBST
constexpr char CBST_MAGIC[4] = {'C', 'B', 'S', 'T'};

std::vector<uint8_t> Exporter::Serialize(const RowGroup& rg) {
    std::vector<uint8_t> buffer;
    
    // 1. File Header (64 bytes)
    uint64_t num_blocks = (rg.num_rows + 63) / 64;
    
    buffer.insert(buffer.end(), CBST_MAGIC, CBST_MAGIC + 4);
    
    auto append_uint64 = [&](uint64_t val) {
        uint8_t bytes[8];
        std::memcpy(bytes, &val, 8);
        buffer.insert(buffer.end(), bytes, bytes + 8);
    };
    
    append_uint64(rg.num_rows);
    append_uint64(rg.num_columns);
    append_uint64(num_blocks);
    
    // Header Padding to 64 bytes
    // 4 (magic) + 24 (3 x uint64) = 28 bytes. Need 36 bytes padding.
    buffer.insert(buffer.end(), 36, 0);
    
    // 2. Column Metadata
    for (size_t i = 0; i < rg.num_columns; ++i) {
        uint32_t type = rg.column_types[i];
        uint32_t name_len = static_cast<uint32_t>(rg.column_names[i].size());
        
        uint8_t meta_bytes[8];
        std::memcpy(meta_bytes, &type, 4);
        std::memcpy(meta_bytes + 4, &name_len, 4);
        buffer.insert(buffer.end(), meta_bytes, meta_bytes + 8);
        
        const char* name_ptr = rg.column_names[i].data();
        buffer.insert(buffer.end(), name_ptr, name_ptr + name_len);
    }
    
    // Pad columns metadata section to the next 64-byte boundary
    size_t col_meta_size = buffer.size();
    size_t col_meta_padding = (64 - (col_meta_size % 64)) % 64;
    if (col_meta_padding > 0) {
        buffer.insert(buffer.end(), col_meta_padding, 0);
    }
    
    // 3. Blocks
    for (size_t b = 0; b < num_blocks; ++b) {
        const BlockData& block = rg.blocks[b];
        
        // Write CBST Tiles: num_columns * CbstTile (each CbstTile is 576 bytes)
        for (size_t c = 0; c < rg.num_columns; ++c) {
            const CbstTile& tile = block.columns[c];
            const uint8_t* tile_ptr = reinterpret_cast<const uint8_t*>(&tile);
            buffer.insert(buffer.end(), tile_ptr, tile_ptr + sizeof(CbstTile));
        }
        
        // Write tail size and tail payload
        uint64_t tail_size = block.tail_payload.size();
        append_uint64(tail_size);
        
        if (tail_size > 0) {
            buffer.insert(buffer.end(), block.tail_payload.begin(), block.tail_payload.end());
        }
        
        // Pad block to 64-byte boundary
        // 8 bytes (tail_size) + tail_size bytes (tail_payload) + padding_len must be multiple of 64
        size_t tail_section_len = 8 + tail_size;
        size_t block_padding_len = (64 - (tail_section_len % 64)) % 64;
        if (block_padding_len > 0) {
            buffer.insert(buffer.end(), block_padding_len, 0);
        }
    }
    
    return buffer;
}

RowGroup Exporter::Deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < 64) {
        throw std::runtime_error("Invalid CBST serialized data: too small");
    }
    
    // 1. Read Header
    if (std::memcmp(data.data(), CBST_MAGIC, 4) != 0) {
        throw std::runtime_error("Invalid CBST serialized data: magic mismatch");
    }
    
    uint64_t num_rows = 0;
    uint64_t num_columns = 0;
    uint64_t num_blocks = 0;
    
    std::memcpy(&num_rows, data.data() + 4, 8);
    std::memcpy(&num_columns, data.data() + 12, 8);
    std::memcpy(&num_blocks, data.data() + 20, 8);
    
    size_t offset = 64; // Header ends at 64 bytes
    
    RowGroup rg;
    rg.num_rows = num_rows;
    rg.num_columns = num_columns;
    rg.column_names.resize(num_columns);
    rg.column_types.resize(num_columns);
    
    // 2. Read Column Metadata
    for (size_t i = 0; i < num_columns; ++i) {
        if (offset + 8 > data.size()) {
            throw std::runtime_error("Malformed CBST data: column metadata bounds check failed");
        }
        
        uint32_t type = 0;
        uint32_t name_len = 0;
        std::memcpy(&type, data.data() + offset, 4);
        std::memcpy(&name_len, data.data() + offset + 4, 4);
        offset += 8;
        
        if (offset + name_len > data.size()) {
            throw std::runtime_error("Malformed CBST data: column name bounds check failed");
        }
        
        std::string name(reinterpret_cast<const char*>(data.data() + offset), name_len);
        rg.column_names[i] = name;
        rg.column_types[i] = type;
        offset += name_len;
    }
    
    // Skip padding to 64-byte boundary
    size_t col_meta_padding = (64 - (offset % 64)) % 64;
    offset += col_meta_padding;
    
    // 3. Read Blocks
    rg.blocks.resize(num_blocks);
    for (size_t b = 0; b < num_blocks; ++b) {
        BlockData& block = rg.blocks[b];
        block.columns.resize(num_columns);
        
        // Read tiles
        for (size_t c = 0; c < num_columns; ++c) {
            if (offset + sizeof(CbstTile) > data.size()) {
                throw std::runtime_error("Malformed CBST data: block tile bounds check failed");
            }
            std::memcpy(&block.columns[c], data.data() + offset, sizeof(CbstTile));
            offset += sizeof(CbstTile);
        }
        
        // Read tail size and payload
        if (offset + 8 > data.size()) {
            throw std::runtime_error("Malformed CBST data: block tail size bounds check failed");
        }
        
        uint64_t tail_size = 0;
        std::memcpy(&tail_size, data.data() + offset, 8);
        offset += 8;
        
        if (tail_size > 0) {
            if (offset + tail_size > data.size()) {
                throw std::runtime_error("Malformed CBST data: block tail payload bounds check failed");
            }
            block.tail_payload.resize(tail_size);
            std::memcpy(block.tail_payload.data(), data.data() + offset, tail_size);
            offset += tail_size;
        }
        
        // Skip block padding to 64-byte boundary
        size_t tail_section_len = 8 + tail_size;
        size_t block_padding_len = (64 - (tail_section_len % 64)) % 64;
        offset += block_padding_len;
    }
    
    return rg;
}

void Exporter::ExportToFile(const RowGroup& rg, const std::string& filepath) {
    std::vector<uint8_t> serialized = Serialize(rg);
    std::ofstream out(filepath, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open file for export: " + filepath);
    }
    out.write(reinterpret_cast<const char*>(serialized.data()), serialized.size());
}

RowGroup Exporter::ImportFromFile(const std::string& filepath) {
    std::ifstream in(filepath, std::ios::binary | std::ios::ate);
    if (!in) {
        throw std::runtime_error("Failed to open file for import: " + filepath);
    }
    
    std::streamsize size = in.tellg();
    in.seekg(0, std::ios::beg);
    
    std::vector<uint8_t> buffer(size);
    if (!in.read(reinterpret_cast<char*>(buffer.data()), size)) {
        throw std::runtime_error("Failed to read file for import: " + filepath);
    }
    
    return Deserialize(buffer);
}

} // namespace greengate

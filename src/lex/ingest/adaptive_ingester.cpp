#include "lex/ingest/adaptive_ingester.hpp"
#include "lex/ingest/simd_transposer.hpp"
#include <arrow/api.h>
#include <stdexcept>
#include <algorithm>
#include <cstring>
#include <iostream>

namespace greengate {

// Helper to generate a 64-bit Bloom filter of bigrams for KIM
uint64_t GenerateKimSignature(std::string_view str) {
    uint64_t signature = 0;
    if (str.empty()) return signature;
    if (str.size() == 1) {
        uint8_t ch = static_cast<uint8_t>(str[0]);
        uint32_t h1 = ch * 17U;
        uint32_t h2 = ch * 31U + 5U;
        uint32_t h3 = ch * 127U + 13U;
        signature |= (1ULL << (h1 % 64));
        signature |= (1ULL << (h2 % 64));
        signature |= (1ULL << (h3 % 64));
        return signature;
    }
    for (size_t i = 0; i < str.size() - 1; ++i) {
        uint32_t bigram = (static_cast<uint32_t>(static_cast<uint8_t>(str[i])) << 8) | 
                           static_cast<uint32_t>(static_cast<uint8_t>(str[i+1]));
        uint32_t h1 = bigram * 2654435761U;
        uint32_t h2 = bigram * 2246822519U + 12345U;
        uint32_t h3 = bigram * 3266489917U + 67890U;
        signature |= (1ULL << (h1 % 64));
        signature |= (1ULL << (h2 % 64));
        signature |= (1ULL << (h3 % 64));
    }
    return signature;
}

template <typename T>
static void ExtractColumnBlock(const std::shared_ptr<arrow::Array>& array, 
                               size_t start_row, size_t count, 
                               uint64_t* dest, bool* valid_mask) {
    using ArrowArrayType = typename arrow::TypeTraits<T>::ArrayType;
    auto concrete = std::static_pointer_cast<ArrowArrayType>(array);
    const auto* src = concrete->raw_values();
    
    for (size_t i = 0; i < count; ++i) {
        size_t idx = start_row + i;
        if (concrete->IsNull(idx)) {
            dest[i] = 0;
            valid_mask[i] = false;
        } else {
            dest[i] = static_cast<uint64_t>(src[idx]);
            valid_mask[i] = true;
        }
    }
    for (size_t i = count; i < 64; ++i) {
        dest[i] = 0;
        valid_mask[i] = false;
    }
}

static void ExtractBoolBlock(const std::shared_ptr<arrow::Array>& array, 
                             size_t start_row, size_t count, 
                             uint64_t* dest, bool* valid_mask) {
    auto concrete = std::static_pointer_cast<arrow::BooleanArray>(array);
    for (size_t i = 0; i < count; ++i) {
        size_t idx = start_row + i;
        if (concrete->IsNull(idx)) {
            dest[i] = 0;
            valid_mask[i] = false;
        } else {
            dest[i] = concrete->Value(idx) ? 1ULL : 0ULL;
            valid_mask[i] = true;
        }
    }
    for (size_t i = count; i < 64; ++i) {
        dest[i] = 0;
        valid_mask[i] = false;
    }
}

RowGroup AdaptiveIngester::Ingest(const std::shared_ptr<arrow::RecordBatch>& batch) {
    RowGroup rg;
    rg.num_rows = batch->num_rows();
    
    auto schema = batch->schema();
    size_t num_logical_columns = batch->num_columns();
    
    std::vector<size_t> logical_to_physical(num_logical_columns);
    size_t current_physical_col = 0;
    
    for (size_t f = 0; f < num_logical_columns; ++f) {
        logical_to_physical[f] = current_physical_col;
        auto field = schema->field(f);
        auto type_id = field->type()->id();
        
        if (type_id == arrow::Type::STRING || type_id == arrow::Type::BINARY) {
            rg.column_names.push_back(field->name());
            rg.column_types.push_back(static_cast<uint32_t>(type_id));
            
            rg.column_names.push_back(field->name() + "_kim");
            rg.column_types.push_back(static_cast<uint32_t>(arrow::Type::UINT64));
            
            current_physical_col += 2;
        } else {
            rg.column_names.push_back(field->name());
            rg.column_types.push_back(static_cast<uint32_t>(type_id));
            
            current_physical_col += 1;
        }
    }
    rg.num_columns = current_physical_col;
    
    size_t num_blocks = (rg.num_rows + 63) / 64;
    rg.blocks.resize(num_blocks);
    
    alignas(64) uint64_t temp_buffer[64];
    alignas(64) bool valid_mask[64];
    
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start_row = b * 64;
        size_t rows_in_block = std::min(size_t(64), rg.num_rows - start_row);
        
        BlockData& block = rg.blocks[b];
        block.columns.resize(rg.num_columns);
        
        std::vector<std::string> block_strings;
        
        for (size_t f = 0; f < num_logical_columns; ++f) {
            auto col = batch->column(f);
            size_t p = logical_to_physical[f];
            
            if (col->type_id() != arrow::Type::STRING && col->type_id() != arrow::Type::BINARY) {
                switch (col->type_id()) {
                    case arrow::Type::INT8:
                        ExtractColumnBlock<arrow::Int8Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::INT16:
                        ExtractColumnBlock<arrow::Int16Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::INT32:
                        ExtractColumnBlock<arrow::Int32Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::INT64:
                        ExtractColumnBlock<arrow::Int64Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::UINT8:
                        ExtractColumnBlock<arrow::UInt8Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::UINT16:
                        ExtractColumnBlock<arrow::UInt16Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::UINT32:
                        ExtractColumnBlock<arrow::UInt32Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::UINT64:
                        ExtractColumnBlock<arrow::UInt64Type>(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    case arrow::Type::BOOL:
                        ExtractBoolBlock(col, start_row, rows_in_block, temp_buffer, valid_mask);
                        break;
                    default:
                        throw std::runtime_error("Unsupported Arrow Type in CBST ingestion: " + col->type()->ToString());
                }
                
                SimdTransposer::Transpose64(temp_buffer, valid_mask, 
                                           block.columns[p].planes, 
                                           &block.columns[p].validity);
            } else {
                auto string_col = std::static_pointer_cast<arrow::StringArray>(col);
                
                alignas(64) uint64_t skeleton_buffer[64];
                alignas(64) uint64_t kim_buffer[64];
                
                for (size_t i = 0; i < rows_in_block; ++i) {
                    size_t idx = start_row + i;
                    if (string_col->IsNull(idx)) {
                        skeleton_buffer[i] = 0;
                        kim_buffer[i] = 0;
                        valid_mask[i] = false;
                        block_strings.push_back("");
                    } else {
                        std::string val = string_col->GetString(idx);
                        
                        uint64_t skel = 0;
                        std::memcpy(&skel, val.data(), std::min(size_t(4), val.size()));
                        skeleton_buffer[i] = skel;
                        
                        kim_buffer[i] = GenerateKimSignature(val);
                        valid_mask[i] = true;
                        block_strings.push_back(val);
                    }
                }
                for (size_t i = rows_in_block; i < 64; ++i) {
                    skeleton_buffer[i] = 0;
                    kim_buffer[i] = 0;
                    valid_mask[i] = false;
                }
                
                SimdTransposer::Transpose64(skeleton_buffer, valid_mask, 
                                           block.columns[p].planes, 
                                           &block.columns[p].validity);
                                           
                SimdTransposer::Transpose64(kim_buffer, valid_mask, 
                                           block.columns[p + 1].planes, 
                                           &block.columns[p + 1].validity);
            }
        }
        
        if (!block_strings.empty()) {
            uint64_t num_strings = block_strings.size();
            std::vector<uint64_t> offsets(num_strings + 1, 0);
            
            size_t current_offset = 0;
            for (size_t i = 0; i < num_strings; ++i) {
                offsets[i] = current_offset;
                current_offset += block_strings[i].size() + 1;
            }
            offsets[num_strings] = current_offset;
            
            size_t metadata_size = sizeof(uint64_t) + (num_strings + 1) * sizeof(uint64_t);
            size_t total_tail_size = metadata_size + current_offset;
            
            size_t padded_tail_size = (total_tail_size + 63) & ~63;
            block.tail_payload.resize(padded_tail_size, 0);
            
            std::memcpy(block.tail_payload.data(), &num_strings, sizeof(uint64_t));
            std::memcpy(block.tail_payload.data() + sizeof(uint64_t), offsets.data(), (num_strings + 1) * sizeof(uint64_t));
            
            char* payload_data_start = block.tail_payload.data() + metadata_size;
            for (size_t i = 0; i < num_strings; ++i) {
                if (!block_strings[i].empty()) {
                    std::memcpy(payload_data_start + offsets[i], block_strings[i].data(), block_strings[i].size());
                }
                payload_data_start[offsets[i] + block_strings[i].size()] = '\0';
            }
        }
    }
    
    return rg;
}

} // namespace greengate

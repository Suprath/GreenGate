#include "green_gate/arrow_connector.hpp"
#include "apex/compute/bit_slicer.hpp"
#include <stdexcept>

namespace greengate {

template <typename T>
static void ExtractColumnBlock(const std::shared_ptr<arrow::Array>& array, 
                               size_t start_row, size_t count, uint64_t* dest) {
    using ArrowArrayType = typename arrow::TypeTraits<T>::ArrayType;
    auto concrete = std::static_pointer_cast<ArrowArrayType>(array);
    const auto* src = concrete->raw_values();
    
    // Check if the array has nulls to handle safely
    if (concrete->null_count() > 0) {
        for (size_t i = 0; i < count; ++i) {
            if (concrete->IsNull(start_row + i)) {
                dest[i] = 0; // Null values default to zero
            } else {
                dest[i] = static_cast<uint64_t>(src[start_row + i]);
            }
        }
    } else {
        // Hot path: direct memcpy/cast loop (no branch for null check)
        for (size_t i = 0; i < count; ++i) {
            dest[i] = static_cast<uint64_t>(src[start_row + i]);
        }
    }
    // Padding
    for (size_t i = count; i < 64; ++i) {
        dest[i] = 0;
    }
}

static void ExtractBoolBlock(const std::shared_ptr<arrow::Array>& array, 
                             size_t start_row, size_t count, uint64_t* dest) {
    auto concrete = std::static_pointer_cast<arrow::BooleanArray>(array);
    for (size_t i = 0; i < count; ++i) {
        if (concrete->IsNull(start_row + i)) {
            dest[i] = 0;
        } else {
            dest[i] = concrete->Value(start_row + i) ? 1ULL : 0ULL;
        }
    }
    for (size_t i = count; i < 64; ++i) {
        dest[i] = 0;
    }
}

void ArrowConnector::RegisterSchema(const std::string& schema_name, 
                                    const std::shared_ptr<arrow::Schema>& schema) {
    std::vector<apex::core::FieldDescriptor> fields;
    size_t offset = 0;
    
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        apex::core::DataType type;
        
        switch (field->type()->id()) {
            case arrow::Type::INT8:   type = apex::core::DataType::INT32; break;
            case arrow::Type::INT16:  type = apex::core::DataType::INT32; break;
            case arrow::Type::INT32:  type = apex::core::DataType::INT32; break;
            case arrow::Type::INT64:  type = apex::core::DataType::INT64; break;
            case arrow::Type::UINT8:  type = apex::core::DataType::UINT32; break;
            case arrow::Type::UINT16: type = apex::core::DataType::UINT32; break;
            case arrow::Type::UINT32: type = apex::core::DataType::UINT32; break;
            case arrow::Type::UINT64: type = apex::core::DataType::UINT64; break;
            case arrow::Type::BOOL:   type = apex::core::DataType::UINT32; break; // Map bool to 32-bit unsigned
            default:
                throw std::runtime_error("Unsupported Arrow Type in AarchGate schema mapping: " + field->type()->ToString());
        }
        
        // Offset is mapped to 8-byte boundaries for our bitslice representation
        fields.push_back({std::string(field->name()), static_cast<uint32_t>(offset), 64, type});
        offset += sizeof(uint64_t);
    }
    
    engine_.register_schema(schema_name, fields, offset);
}

void ArrowConnector::TransposeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, 
                                    uint64_t* out_bit_planes) {
    size_t num_rows = batch->num_rows();
    size_t num_fields = batch->num_columns();
    size_t num_blocks = (num_rows + 63) / 64;
    
    apex::compute::BitSlicer slicer;
    alignas(64) uint64_t temp_buffer[64];
    
    for (size_t b = 0; b < num_blocks; ++b) {
        size_t start_row = b * 64;
        size_t rows_in_block = std::min(size_t(64), num_rows - start_row);
        
        for (size_t f = 0; f < num_fields; ++f) {
            auto col = batch->column(f);
            
            switch (col->type_id()) {
                case arrow::Type::INT8:
                    ExtractColumnBlock<arrow::Int8Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::INT16:
                    ExtractColumnBlock<arrow::Int16Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::INT32:
                    ExtractColumnBlock<arrow::Int32Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::INT64:
                    ExtractColumnBlock<arrow::Int64Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::UINT8:
                    ExtractColumnBlock<arrow::UInt8Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::UINT16:
                    ExtractColumnBlock<arrow::UInt16Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::UINT32:
                    ExtractColumnBlock<arrow::UInt32Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::UINT64:
                    ExtractColumnBlock<arrow::UInt64Type>(col, start_row, rows_in_block, temp_buffer);
                    break;
                case arrow::Type::BOOL:
                    ExtractBoolBlock(col, start_row, rows_in_block, temp_buffer);
                    break;
                default:
                    throw std::runtime_error("Unsupported Arrow column type in transposition: " + col->type()->ToString());
            }
            
            // Destination pointer inside the flat bit-planes array
            uint64_t* block_dest = out_bit_planes + (b * num_fields * 64) + (f * 64);
            
            // Transpose values into bit-planes
            slicer.slice_n(temp_buffer, rows_in_block, block_dest, 64);
        }
    }
}

} // namespace greengate

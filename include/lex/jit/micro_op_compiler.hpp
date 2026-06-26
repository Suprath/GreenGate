#pragma once
#include "lex/storage/cbst_tile.hpp"
#include <vector>
#include <cstdint>
#include <string>

namespace greengate {

enum class Opcode {
    LOAD_TILE,          // Load physical tile at index `imm` to virtual register `dest_reg`
    BIT_AND,            // dest_reg = src_reg1 & src_reg2 (64-bit mask operation)
    BIT_OR,             // dest_reg = src_reg1 | src_reg2
    BIT_XOR,            // dest_reg = src_reg1 ^ src_reg2
    BIT_BIC,            // dest_reg = src_reg1 & ~src_reg2
    CMP_SKELETON,       // dest_reg = (first 32 bits of tile `src_reg1` == imm)
    CMP_KIM,            // dest_reg = (64 bits of tile `src_reg1` == imm)
    CMP_GT,             // dest_reg = (numeric tile `src_reg1` > imm)
    CMP_LT,             // dest_reg = (numeric tile `src_reg1` < imm)
    CMP_EQ,             // dest_reg = (numeric tile `src_reg1` == imm)
    MATERIALIZE_SCALAR  // Materializes tile `src_reg1` to `materialized_dest`
};

struct MicroOp {
    Opcode op;
    int dest_reg = -1;
    int src_reg1 = -1;
    int src_reg2 = -1;
    uint64_t imm = 0;
};

// Function pointer signature for JIT-compiled function
// tiles points to the array of CbstTile for this block.
// tail_payload points to the variable length data.
// materialized_dest points to a buffer of 64 uint64_t elements.
// Returns a 64-bit match mask.
using BlockScanFunc = uint64_t (*)(const CbstTile* tiles, const char* tail_payload, uint64_t* materialized_dest, uint64_t delete_mask);

class MicroOpCompiler {
public:
    static BlockScanFunc Compile(const std::vector<MicroOp>& ops);
};

} // namespace greengate

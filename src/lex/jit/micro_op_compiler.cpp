#include "lex/jit/micro_op_compiler.hpp"
#include <asmjit/a64.h>
#include <stdexcept>
#include <unordered_map>
#include <iostream>

extern "C" void greengate_butterfly_transpose(const uint64_t* in_planes, uint64_t* out_scalars) noexcept;

namespace greengate {

static void emit_mov_imm64(asmjit::a64::Assembler& a,
                           const asmjit::a64::Gp& dst,
                           uint64_t imm) noexcept {
    a.movz(dst, imm & 0xFFFF, 0);
    a.movk(dst, (imm >> 16) & 0xFFFF, 16);
    a.movk(dst, (imm >> 32) & 0xFFFF, 32);
    a.movk(dst, (imm >> 48) & 0xFFFF, 48);
}

BlockScanFunc MicroOpCompiler::Compile(const std::vector<MicroOp>& ops) {
    using namespace asmjit;
    using namespace asmjit::a64;

    static JitRuntime rt;
    CodeHolder code;
    code.init(rt.environment());
    Assembler a(&code);

    // Save FP, LR, and callee-saved registers x19..x28
    a.stp(x29, x30, Mem(sp, -16).pre());
    a.mov(x29, sp);
    a.stp(x19, x20, Mem(sp, -16).pre());
    a.stp(x21, x22, Mem(sp, -16).pre());
    a.stp(x23, x24, Mem(sp, -16).pre());
    a.stp(x25, x26, Mem(sp, -16).pre());
    a.stp(x27, x28, Mem(sp, -16).pre());
    
    // Save delete_mask (x3) on the stack
    a.str(x3, Mem(sp, -16).pre());

    // Parameters: x0 = tiles, x1 = tail_payload, x2 = materialized_dest
    // Copy parameters to callee-saved registers
    a.mov(x19, x0); // x19 = tiles
    a.mov(x20, x1); // x20 = tail_payload
    a.mov(x21, x2); // x21 = materialized_dest

    // Register allocation: map virtual registers to x22..x28, then x9..x15
    std::unordered_map<int, Gp> reg_map;
    std::vector<Gp> gp_pool = {
        x22, x23, x24, x25, x26, x27, x28,
        x9,  x10, x11, x12, x13, x14, x15
    };
    int next_gp = 0;

    auto get_reg = [&](int vreg) -> Gp {
        if (reg_map.find(vreg) == reg_map.end()) {
            if (next_gp >= static_cast<int>(gp_pool.size())) {
                throw std::runtime_error("Out of physical GPR registers in JIT compiler");
            }
            reg_map[vreg] = gp_pool[next_gp++];
            std::cout << "[MicroOpCompiler Debug] vreg " << vreg << " -> x" << reg_map[vreg].id() << std::endl;
        }
        return reg_map[vreg];
    };

    // Helper to find a physical register from the pool that is not in use
    auto get_scratch_reg = [&](const std::vector<Gp>& exclude) -> Gp {
        for (const auto& reg : gp_pool) {
            bool in_use = false;
            for (const auto& [_, r] : reg_map) {
                if (r == reg) { in_use = true; break; }
            }
            for (const auto& reg_ex : exclude) {
                if (reg_ex == reg) { in_use = true; break; }
            }
            if (!in_use) {
                std::cout << "[MicroOpCompiler Debug] scratch -> x" << reg.id() << std::endl;
                return reg;
            }
        }
        throw std::runtime_error("Out of scratch registers in JIT compiler");
    };

    // Compile each micro-op
    for (const auto& op : ops) {
        switch (op.op) {
            case Opcode::LOAD_TILE: {
                Gp r = get_reg(op.dest_reg);
                uint64_t offset = op.imm * sizeof(CbstTile);
                if (offset == 0) {
                    a.mov(r, x19);
                } else {
                    // Use a temporary scratch register x8
                    emit_mov_imm64(a, x8, offset);
                    a.add(r, x19, x8);
                }
                break;
            }
            case Opcode::BIT_AND: {
                a.and_(get_reg(op.dest_reg), get_reg(op.src_reg1), get_reg(op.src_reg2));
                break;
            }
            case Opcode::BIT_OR: {
                a.orr(get_reg(op.dest_reg), get_reg(op.src_reg1), get_reg(op.src_reg2));
                break;
            }
            case Opcode::BIT_XOR: {
                a.eor(get_reg(op.dest_reg), get_reg(op.src_reg1), get_reg(op.src_reg2));
                break;
            }
            case Opcode::BIT_BIC: {
                a.bic(get_reg(op.dest_reg), get_reg(op.src_reg1), get_reg(op.src_reg2));
                break;
            }
            case Opcode::CMP_SKELETON: {
                Gp eq = get_reg(op.dest_reg);
                Gp tile_addr = get_reg(op.src_reg1);
                Gp tmp0 = get_scratch_reg({eq, tile_addr});
                Gp tmp1 = get_scratch_reg({eq, tile_addr, tmp0});
                Gp tmp2 = get_scratch_reg({eq, tile_addr, tmp0, tmp1});
                Gp tmp3 = get_scratch_reg({eq, tile_addr, tmp0, tmp1, tmp2});

                a.mov(eq, -1LL);

                uint32_t imm32 = static_cast<uint32_t>(op.imm);
                int num_bits = op.src_reg2;
                if (num_bits <= 0 || num_bits > 32) num_bits = 32;
                
                int bit = num_bits - 1;
                for (; bit >= 3; bit -= 4) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    a.ldr(tmp1, Mem(tile_addr, (bit - 1) * 8));
                    a.ldr(tmp2, Mem(tile_addr, (bit - 2) * 8));
                    a.ldr(tmp3, Mem(tile_addr, (bit - 3) * 8));
                    
                    // Bit 0
                    uint64_t c0 = (imm32 >> bit) & 1;
                    if (c0 == 0) a.bic(eq, eq, tmp0);
                    else a.and_(eq, eq, tmp0);
                    
                    // Bit 1
                    uint64_t c1 = (imm32 >> (bit - 1)) & 1;
                    if (c1 == 0) a.bic(eq, eq, tmp1);
                    else a.and_(eq, eq, tmp1);
                    
                    // Bit 2
                    uint64_t c2 = (imm32 >> (bit - 2)) & 1;
                    if (c2 == 0) a.bic(eq, eq, tmp2);
                    else a.and_(eq, eq, tmp2);
                    
                    // Bit 3
                    uint64_t c3 = (imm32 >> (bit - 3)) & 1;
                    if (c3 == 0) a.bic(eq, eq, tmp3);
                    else a.and_(eq, eq, tmp3);
                }
                
                // Cleanup remaining bits
                for (; bit >= 0; --bit) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    uint64_t c = (imm32 >> bit) & 1;
                    if (c == 0) a.bic(eq, eq, tmp0);
                    else a.and_(eq, eq, tmp0);
                }
                break;
            }
            case Opcode::CMP_KIM: {
                Gp eq = get_reg(op.dest_reg);
                Gp tile_addr = get_reg(op.src_reg1);
                Gp tmp0 = get_scratch_reg({eq, tile_addr});
                Gp tmp1 = get_scratch_reg({eq, tile_addr, tmp0});
                Gp tmp2 = get_scratch_reg({eq, tile_addr, tmp0, tmp1});
                Gp tmp3 = get_scratch_reg({eq, tile_addr, tmp0, tmp1, tmp2});

                a.mov(eq, -1LL);

                uint64_t signature64 = op.imm;
                int num_bits = op.src_reg2;
                if (num_bits <= 0 || num_bits > 64) num_bits = 64; // default to 64 bits to use full Bloom filter
                
                int bit = num_bits - 1;
                for (; bit >= 3; bit -= 4) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    a.ldr(tmp1, Mem(tile_addr, (bit - 1) * 8));
                    a.ldr(tmp2, Mem(tile_addr, (bit - 2) * 8));
                    a.ldr(tmp3, Mem(tile_addr, (bit - 3) * 8));
                    
                    // Bit 0
                    uint64_t c0 = (signature64 >> bit) & 1;
                    if (c0 == 1) a.and_(eq, eq, tmp0);
                    
                    // Bit 1
                    uint64_t c1 = (signature64 >> (bit - 1)) & 1;
                    if (c1 == 1) a.and_(eq, eq, tmp1);
                    
                    // Bit 2
                    uint64_t c2 = (signature64 >> (bit - 2)) & 1;
                    if (c2 == 1) a.and_(eq, eq, tmp2);
                    
                    // Bit 3
                    uint64_t c3 = (signature64 >> (bit - 3)) & 1;
                    if (c3 == 1) a.and_(eq, eq, tmp3);
                }
                
                // Cleanup remaining bits
                for (; bit >= 0; --bit) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    uint64_t c = (signature64 >> bit) & 1;
                    if (c == 1) a.and_(eq, eq, tmp0);
                }
                break;
            }
            case Opcode::CMP_EQ: {
                Gp eq = get_reg(op.dest_reg);
                Gp tile_addr = get_reg(op.src_reg1);
                Gp tmp0 = get_scratch_reg({eq, tile_addr});
                Gp tmp1 = get_scratch_reg({eq, tile_addr, tmp0});
                Gp tmp2 = get_scratch_reg({eq, tile_addr, tmp0, tmp1});
                Gp tmp3 = get_scratch_reg({eq, tile_addr, tmp0, tmp1, tmp2});

                a.mov(eq, -1LL);

                uint64_t signature64 = op.imm;
                int num_bits = op.src_reg2;
                if (num_bits <= 0 || num_bits > 64) num_bits = 64;
                
                int bit = num_bits - 1;
                for (; bit >= 3; bit -= 4) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    a.ldr(tmp1, Mem(tile_addr, (bit - 1) * 8));
                    a.ldr(tmp2, Mem(tile_addr, (bit - 2) * 8));
                    a.ldr(tmp3, Mem(tile_addr, (bit - 3) * 8));
                    
                    // Bit 0
                    uint64_t c0 = (signature64 >> bit) & 1;
                    if (c0 == 0) a.bic(eq, eq, tmp0);
                    else a.and_(eq, eq, tmp0);
                    
                    // Bit 1
                    uint64_t c1 = (signature64 >> (bit - 1)) & 1;
                    if (c1 == 0) a.bic(eq, eq, tmp1);
                    else a.and_(eq, eq, tmp1);
                    
                    // Bit 2
                    uint64_t c2 = (signature64 >> (bit - 2)) & 1;
                    if (c2 == 0) a.bic(eq, eq, tmp2);
                    else a.and_(eq, eq, tmp2);
                    
                    // Bit 3
                    uint64_t c3 = (signature64 >> (bit - 3)) & 1;
                    if (c3 == 0) a.bic(eq, eq, tmp3);
                    else a.and_(eq, eq, tmp3);
                }
                
                // Cleanup remaining bits
                for (; bit >= 0; --bit) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    uint64_t c = (signature64 >> bit) & 1;
                    if (c == 0) a.bic(eq, eq, tmp0);
                    else a.and_(eq, eq, tmp0);
                }
                break;
            }
            case Opcode::CMP_GT: {
                Gp gt = get_reg(op.dest_reg);
                Gp tile_addr = get_reg(op.src_reg1);
                Gp eq = get_scratch_reg({gt, tile_addr});
                Gp tmp_and = get_scratch_reg({gt, tile_addr, eq});
                Gp tmp0 = get_scratch_reg({gt, tile_addr, eq, tmp_and});
                Gp tmp1 = get_scratch_reg({gt, tile_addr, eq, tmp_and, tmp0});

                a.mov(gt, 0);
                a.mov(eq, -1LL);

                uint64_t threshold = op.imm;
                int num_bits = op.src_reg2;
                if (num_bits <= 0 || num_bits > 64) num_bits = 64;
                
                int bit = num_bits - 1;
                for (; bit >= 1; bit -= 2) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    a.ldr(tmp1, Mem(tile_addr, (bit - 1) * 8));
                    
                    // Bit 0
                    uint64_t c0 = (threshold >> bit) & 1;
                    if (c0 == 0) {
                        a.and_(tmp_and, eq, tmp0);
                        a.orr(gt, gt, tmp_and);
                        a.bic(eq, eq, tmp0);
                    } else {
                        a.and_(eq, eq, tmp0);
                    }
                    
                    // Bit 1
                    uint64_t c1 = (threshold >> (bit - 1)) & 1;
                    if (c1 == 0) {
                        a.and_(tmp_and, eq, tmp1);
                        a.orr(gt, gt, tmp_and);
                        a.bic(eq, eq, tmp1);
                    } else {
                        a.and_(eq, eq, tmp1);
                    }
                }
                if (bit == 0) {
                    a.ldr(tmp0, Mem(tile_addr, 0));
                    uint64_t c = threshold & 1;
                    if (c == 0) {
                        a.and_(tmp_and, eq, tmp0);
                        a.orr(gt, gt, tmp_and);
                        a.bic(eq, eq, tmp0);
                    } else {
                        a.and_(eq, eq, tmp0);
                    }
                }
                break;
            }
            case Opcode::CMP_LT: {
                Gp lt = get_reg(op.dest_reg);
                Gp tile_addr = get_reg(op.src_reg1);
                Gp eq = get_scratch_reg({lt, tile_addr});
                Gp tmp_and = get_scratch_reg({lt, tile_addr, eq});
                Gp tmp0 = get_scratch_reg({lt, tile_addr, eq, tmp_and});
                Gp tmp1 = get_scratch_reg({lt, tile_addr, eq, tmp_and, tmp0});

                a.mov(lt, 0);
                a.mov(eq, -1LL);

                uint64_t threshold = op.imm;
                int num_bits = op.src_reg2;
                if (num_bits <= 0 || num_bits > 64) num_bits = 64;
                
                int bit = num_bits - 1;
                for (; bit >= 1; bit -= 2) {
                    a.ldr(tmp0, Mem(tile_addr, bit * 8));
                    a.ldr(tmp1, Mem(tile_addr, (bit - 1) * 8));
                    
                    // Bit 0
                    uint64_t c0 = (threshold >> bit) & 1;
                    if (c0 == 1) {
                        a.bic(tmp_and, eq, tmp0);
                        a.orr(lt, lt, tmp_and);
                        a.and_(eq, eq, tmp0);
                    } else {
                        a.bic(eq, eq, tmp0);
                    }
                    
                    // Bit 1
                    uint64_t c1 = (threshold >> (bit - 1)) & 1;
                    if (c1 == 1) {
                        a.bic(tmp_and, eq, tmp1);
                        a.orr(lt, lt, tmp_and);
                        a.and_(eq, eq, tmp1);
                    } else {
                        a.bic(eq, eq, tmp1);
                    }
                }
                if (bit == 0) {
                    a.ldr(tmp0, Mem(tile_addr, 0));
                    uint64_t c = threshold & 1;
                    if (c == 1) {
                        a.bic(tmp_and, eq, tmp0);
                        a.orr(lt, lt, tmp_and);
                        a.and_(eq, eq, tmp0);
                    } else {
                        a.bic(eq, eq, tmp0);
                    }
                }
                break;
            }
            case Opcode::MATERIALIZE_SCALAR: {
                Gp tile_addr = get_reg(op.src_reg1);
                
                // Save caller-saved registers x9..x15 (aligned to 16 bytes)
                a.stp(x9, x10, Mem(sp, -16).pre());
                a.stp(x11, x12, Mem(sp, -16).pre());
                a.stp(x13, x14, Mem(sp, -16).pre());
                a.str(x15, Mem(sp, -16).pre());
                
                // Copy inputs to ABI arg registers
                a.mov(x0, tile_addr); // Parameter 1: input planes
                a.mov(x1, x21);       // Parameter 2: materialized_dest
                
                // Call helper
                emit_mov_imm64(a, x8, reinterpret_cast<uint64_t>(&greengate_butterfly_transpose));
                a.blr(x8);
                
                // Restore caller-saved registers x9..x15
                a.ldr(x15, Mem(sp).post(16));
                a.ldp(x13, x14, Mem(sp).post(16));
                a.ldp(x11, x12, Mem(sp).post(16));
                a.ldp(x9, x10, Mem(sp).post(16));
                break;
            }
        }
    }

    // Move final result register value to x0 and apply delete mask
    if (!ops.empty()) {
        Gp res = get_reg(ops.back().dest_reg);
        // Load delete_mask from stack (at offset 0 from sp)
        a.ldr(x8, Mem(sp, 0));
        a.and_(x0, res, x8);
    } else {
        a.mov(x0, xzr);
    }

    // Pop delete_mask from stack
    a.add(sp, sp, 16);

    // Restore callee-saved registers and return
    a.ldp(x27, x28, Mem(sp).post(16));
    a.ldp(x25, x26, Mem(sp).post(16));
    a.ldp(x23, x24, Mem(sp).post(16));
    a.ldp(x21, x22, Mem(sp).post(16));
    a.ldp(x19, x20, Mem(sp).post(16));
    a.ldp(x29, x30, Mem(sp).post(16));
    a.ret(x30);

    BlockScanFunc fn;
    Error err = rt.add(&fn, &code);
    if (err != kErrorOk) {
        throw std::runtime_error("AsmJit compilation failed: " + std::to_string(static_cast<uint32_t>(err)));
    }

    return fn;
}

} // namespace greengate

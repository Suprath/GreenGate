#pragma once
#include <cstdint>

namespace greengate {

// 576 bytes = 9 Cache Lines (64-byte boundary aligned)
struct alignas(64) CbstTile {
    uint64_t planes[64];    // 64 bit-planes (512 bytes)
    uint64_t validity;      // 65th validity bit-plane (8 bytes)
    uint64_t padding[7];    // 56 bytes padding to align to 576 bytes
};

} // namespace greengate

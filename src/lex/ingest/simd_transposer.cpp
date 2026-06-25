#include "lex/ingest/simd_transposer.hpp"
#include <cstring>

#if defined(__ARM_NEON) || defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace greengate {

#if defined(__ARM_NEON) || defined(__aarch64__)

inline void Stage5(uint64x2_t* q) {
    uint64x2_t vmask5 = vdupq_n_u64(0x00000000FFFFFFFFULL);
    for (int k = 0; k < 16; ++k) {
        uint64x2_t va = q[k];
        uint64x2_t vb = q[k + 16];
        uint64x2_t vt = vandq_u64(veorq_u64(vshrq_n_u64(va, 32), vb), vmask5);
        q[k + 16] = veorq_u64(vb, vt);
        q[k] = veorq_u64(va, vshlq_n_u64(vt, 32));
    }
}

inline void Stage4(uint64x2_t* q) {
    uint64x2_t vmask4 = vdupq_n_u64(0x0000FFFF0000FFFFULL);
    for (int block = 0; block < 32; block += 16) {
        for (int k = block; k < block + 8; ++k) {
            uint64x2_t va = q[k];
            uint64x2_t vb = q[k + 8];
            uint64x2_t vt = vandq_u64(veorq_u64(vshrq_n_u64(va, 16), vb), vmask4);
            q[k + 8] = veorq_u64(vb, vt);
            q[k] = veorq_u64(va, vshlq_n_u64(vt, 16));
        }
    }
}

inline void Stage3(uint64x2_t* q) {
    uint64x2_t vmask3 = vdupq_n_u64(0x00FF00FF00FF00FFULL);
    for (int block = 0; block < 32; block += 8) {
        for (int k = block; k < block + 4; ++k) {
            uint64x2_t va = q[k];
            uint64x2_t vb = q[k + 4];
            uint64x2_t vt = vandq_u64(veorq_u64(vshrq_n_u64(va, 8), vb), vmask3);
            q[k + 4] = veorq_u64(vb, vt);
            q[k] = veorq_u64(va, vshlq_n_u64(vt, 8));
        }
    }
}

inline void Stage2(uint64x2_t* q) {
    uint64x2_t vmask2 = vdupq_n_u64(0x0F0F0F0F0F0F0F0FULL);
    for (int block = 0; block < 32; block += 4) {
        for (int k = block; k < block + 2; ++k) {
            uint64x2_t va = q[k];
            uint64x2_t vb = q[k + 2];
            uint64x2_t vt = vandq_u64(veorq_u64(vshrq_n_u64(va, 4), vb), vmask2);
            q[k + 2] = veorq_u64(vb, vt);
            q[k] = veorq_u64(va, vshlq_n_u64(vt, 4));
        }
    }
}

inline void Stage1(uint64x2_t* q) {
    uint64x2_t vmask1 = vdupq_n_u64(0x3333333333333333ULL);
    for (int block = 0; block < 32; block += 2) {
        uint64x2_t va = q[block];
        uint64x2_t vb = q[block + 1];
        uint64x2_t vt = vandq_u64(veorq_u64(vshrq_n_u64(va, 2), vb), vmask1);
        q[block + 1] = veorq_u64(vb, vt);
        q[block] = veorq_u64(va, vshlq_n_u64(vt, 2));
    }
}

inline void Stage0(uint64x2_t* q) {
    uint64x2_t vmask0 = vdupq_n_u64(0x5555555555555555ULL);
    for (int k = 0; k < 32; ++k) {
        uint64x2_t shifted = vshrq_n_u64(q[k], 1);
        uint64x2_t swapped = vcombine_u64(vget_high_u64(q[k]), vget_low_u64(q[k]));
        uint64x2_t t_vec = vandq_u64(veorq_u64(shifted, swapped), vmask0);
        uint64x2_t t_left = vshlq_n_u64(t_vec, 1);
        uint64x2_t t_final = vcombine_u64(vget_low_u64(t_left), vget_low_u64(t_vec));
        q[k] = veorq_u64(q[k], t_final);
    }
}

#else

void Transpose64_Scalar(uint64_t* A) noexcept {
    // Stage 5
    for (int i = 0; i < 32; ++i) {
        uint64_t t = ((A[i] >> 32) ^ A[i + 32]) & 0x00000000FFFFFFFFULL;
        A[i + 32] ^= t;
        A[i] ^= (t << 32);
    }
    // Stage 4
    for (int block = 0; block < 64; block += 32) {
        for (int i = block; i < block + 16; ++i) {
            uint64_t t = ((A[i] >> 16) ^ A[i + 16]) & 0x0000FFFF0000FFFFULL;
            A[i + 16] ^= t;
            A[i] ^= (t << 16);
        }
    }
    // Stage 3
    for (int block = 0; block < 64; block += 16) {
        for (int i = block; i < block + 8; ++i) {
            uint64_t t = ((A[i] >> 8) ^ A[i + 8]) & 0x00FF00FF00FF00FFULL;
            A[i + 8] ^= t;
            A[i] ^= (t << 8);
        }
    }
    // Stage 2
    for (int block = 0; block < 64; block += 8) {
        for (int i = block; i < block + 4; ++i) {
            uint64_t t = ((A[i] >> 4) ^ A[i + 4]) & 0x0F0F0F0F0F0F0F0FULL;
            A[i + 4] ^= t;
            A[i] ^= (t << 4);
        }
    }
    // Stage 1
    for (int block = 0; block < 64; block += 4) {
        for (int i = block; i < block + 2; ++i) {
            uint64_t t = ((A[i] >> 2) ^ A[i + 2]) & 0x3333333333333333ULL;
            A[i + 2] ^= t;
            A[i] ^= (t << 2);
        }
    }
    // Stage 0
    for (int i = 0; i < 64; i += 2) {
        uint64_t t = ((A[i] >> 1) ^ A[i + 1]) & 0x5555555555555555ULL;
        A[i + 1] ^= t;
        A[i] ^= (t << 1);
    }
}

#endif

void SimdTransposer::Transpose64(const uint64_t* data, const bool* valid_mask, 
                                 uint64_t* out_planes, uint64_t* out_validity) noexcept {
    uint64_t val_plane = 0xFFFFFFFFffffffffULL;
    if (valid_mask != nullptr) {
        val_plane = 0;
        for (int i = 0; i < 64; ++i) {
            if (valid_mask[i]) {
                val_plane |= (1ULL << i);
            }
        }
    }
    *out_validity = val_plane;

#if defined(__ARM_NEON) || defined(__aarch64__)
    alignas(64) uint64x2_t q[32];
    for (int i = 0; i < 32; ++i) {
        q[i] = vld1q_u64(data + 2 * i);
    }

    Stage5(q);
    Stage4(q);
    Stage3(q);
    Stage2(q);
    Stage1(q);
    Stage0(q);

    for (int i = 0; i < 32; ++i) {
        vst1q_u64(out_planes + 2 * i, q[i]);
    }
#else
    std::memcpy(out_planes, data, 64 * sizeof(uint64_t));
    Transpose64_Scalar(out_planes);
#endif
}

} // namespace greengate

extern "C" void greengate_butterfly_transpose(const uint64_t* in_planes, uint64_t* out_scalars) noexcept {
    uint64_t unused_validity = 0;
    greengate::SimdTransposer::Transpose64(in_planes, nullptr, out_scalars, &unused_validity);
}

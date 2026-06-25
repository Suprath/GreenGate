#pragma once
#include <cstdint>

namespace greengate {

class SimdTransposer {
public:
    // Transposes 64 uint64_t scalar values into 64 bitslices + 1 validity mask in place.
    // data must point to 64 uint64_t values.
    // valid_mask must point to 64 bool values (true if valid, false if null). If valid_mask is nullptr, validity plane is set to all 1s.
    // out_planes must point to 64 uint64_t destination planes.
    // out_validity must point to a single uint64_t destination validity plane.
    static void Transpose64(const uint64_t* data, const bool* valid_mask, 
                            uint64_t* out_planes, uint64_t* out_validity) noexcept;
};

} // namespace greengate

extern "C" void greengate_butterfly_transpose(const uint64_t* in_planes, uint64_t* out_scalars) noexcept;


// SPDX-License-Identifier: MIT OR Apache-2.0

//! Dependency-free XXH64 hashing.
//!
//! The table uses scalar XXH64 for individual keys and checksums. Batch paths
//! hash four fixed-width keys in parallel with AVX2 and fall back to scalar code
//! when AVX2 is unavailable.

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    __m256i, _mm256_add_epi64, _mm256_mul_epu32, _mm256_or_si256, _mm256_set_epi64x,
    _mm256_set1_epi64x, _mm256_slli_epi64, _mm256_srli_epi64, _mm256_storeu_si256,
    _mm256_xor_si256,
};

const PRIME_1: u64 = 11_400_714_785_074_694_791;
const PRIME_2: u64 = 14_029_467_366_897_019_727;
const PRIME_3: u64 = 1_609_587_929_392_839_161;
const PRIME_4: u64 = 9_650_029_242_287_828_579;

const fn u64_as_i64_bits(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}
const PRIME_5: u64 = 2_870_177_450_012_600_261;

#[must_use]
/// Computes the XXH64 digest of `input` with the supplied seed.
///
/// This implementation matches the canonical XXH64 vectors and performs no
/// allocation.
///
/// # Examples
///
/// ```
/// assert_eq!(
///     floresta_db::xxh64(b"", 0),
///     0xef46_db37_51d8_e999,
/// );
/// ```
pub fn xxh64(input: &[u8], seed: u64) -> u64 {
    let mut remaining = input;
    let mut hash = if remaining.len() >= 32 {
        let mut lane_1 = seed.wrapping_add(PRIME_1).wrapping_add(PRIME_2);
        let mut lane_2 = seed.wrapping_add(PRIME_2);
        let mut lane_3 = seed;
        let mut lane_4 = seed.wrapping_sub(PRIME_1);

        while let Some((stripe, tail)) = take_prefix::<32>(remaining) {
            lane_1 = round(lane_1, read_u64(&stripe[0..8]));
            lane_2 = round(lane_2, read_u64(&stripe[8..16]));
            lane_3 = round(lane_3, read_u64(&stripe[16..24]));
            lane_4 = round(lane_4, read_u64(&stripe[24..32]));
            remaining = tail;
        }

        let combined = lane_1
            .rotate_left(1)
            .wrapping_add(lane_2.rotate_left(7))
            .wrapping_add(lane_3.rotate_left(12))
            .wrapping_add(lane_4.rotate_left(18));
        merge_round(
            merge_round(merge_round(merge_round(combined, lane_1), lane_2), lane_3),
            lane_4,
        )
    } else {
        seed.wrapping_add(PRIME_5)
    };

    hash = hash.wrapping_add(input.len() as u64);
    while let Some((word, tail)) = take_prefix::<8>(remaining) {
        let mixed = round(0, read_u64(word));
        hash ^= mixed;
        hash = hash
            .rotate_left(27)
            .wrapping_mul(PRIME_1)
            .wrapping_add(PRIME_4);
        remaining = tail;
    }
    if let Some((word, tail)) = take_prefix::<4>(remaining) {
        hash ^= u64::from(read_u32(word)).wrapping_mul(PRIME_1);
        hash = hash
            .rotate_left(23)
            .wrapping_mul(PRIME_2)
            .wrapping_add(PRIME_3);
        remaining = tail;
    }
    for byte in remaining {
        hash ^= u64::from(*byte).wrapping_mul(PRIME_5);
        hash = hash.rotate_left(11).wrapping_mul(PRIME_1);
    }

    avalanche(hash)
}

pub(crate) fn xxh64_batch4(inputs: [&[u8]; 4], seed: u64) -> [u64; 4] {
    let length = inputs[0].len();
    if inputs[1..].iter().any(|input| input.len() != length) {
        return inputs.map(|input| xxh64(input, seed));
    }

    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx2") {
        // SAFETY: runtime feature detection proves AVX2 is available.
        return unsafe { xxh64_batch4_avx2(inputs, seed) };
    }

    inputs.map(|input| xxh64(input, seed))
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[allow(clippy::too_many_lines)]
unsafe fn xxh64_batch4_avx2(inputs: [&[u8]; 4], seed: u64) -> [u64; 4] {
    // SAFETY: this entire block executes only after AVX2 runtime detection. Every
    // slice range is bounded by the shared input length before it is loaded.
    unsafe {
        macro_rules! multiply {
            ($left:expr, $right:expr) => {{
                let left = $left;
                let right = $right;
                let low = _mm256_mul_epu32(left, right);
                let left_high = _mm256_srli_epi64::<32>(left);
                let right_high = _mm256_srli_epi64::<32>(right);
                let cross = _mm256_add_epi64(
                    _mm256_mul_epu32(left_high, right),
                    _mm256_mul_epu32(left, right_high),
                );
                _mm256_add_epi64(low, _mm256_slli_epi64::<32>(cross))
            }};
        }

        macro_rules! rotate_left {
            ($value:expr, $shift:literal) => {{
                _mm256_or_si256(
                    _mm256_slli_epi64::<$shift>($value),
                    _mm256_srli_epi64::<{ 64 - $shift }>($value),
                )
            }};
        }

        macro_rules! round_lanes {
            ($accumulator:expr, $lane:expr, $prime_1:expr, $prime_2:expr) => {{
                let mixed = multiply!($lane, $prime_2);
                multiply!(
                    rotate_left!(_mm256_add_epi64($accumulator, mixed), 31),
                    $prime_1
                )
            }};
        }

        macro_rules! merge_lanes {
            ($accumulator:expr, $lane:expr, $zero:expr, $prime_1:expr, $prime_2:expr, $prime_4:expr) => {{
                let merged =
                    _mm256_xor_si256($accumulator, round_lanes!($zero, $lane, $prime_1, $prime_2));
                _mm256_add_epi64(multiply!(merged, $prime_1), $prime_4)
            }};
        }

        macro_rules! load_u64x4 {
            ($offset:expr) => {{
                let offset = $offset;
                _mm256_set_epi64x(
                    u64_as_i64_bits(read_u64(&inputs[3][offset..offset + 8])),
                    u64_as_i64_bits(read_u64(&inputs[2][offset..offset + 8])),
                    u64_as_i64_bits(read_u64(&inputs[1][offset..offset + 8])),
                    u64_as_i64_bits(read_u64(&inputs[0][offset..offset + 8])),
                )
            }};
        }

        let zero = _mm256_set1_epi64x(0);
        let prime_1 = _mm256_set1_epi64x(u64_as_i64_bits(PRIME_1));
        let prime_2 = _mm256_set1_epi64x(u64_as_i64_bits(PRIME_2));
        let prime_3 = _mm256_set1_epi64x(u64_as_i64_bits(PRIME_3));
        let prime_4 = _mm256_set1_epi64x(u64_as_i64_bits(PRIME_4));
        let prime_5 = _mm256_set1_epi64x(u64_as_i64_bits(PRIME_5));
        let seed_lanes = _mm256_set1_epi64x(u64_as_i64_bits(seed));
        let length = inputs[0].len();
        let mut offset = 0_usize;

        let mut hash = if length >= 32 {
            let mut lane_1 = _mm256_add_epi64(_mm256_add_epi64(seed_lanes, prime_1), prime_2);
            let mut lane_2 = _mm256_add_epi64(seed_lanes, prime_2);
            let mut lane_3 = seed_lanes;
            let mut lane_4 = _mm256_add_epi64(
                seed_lanes,
                _mm256_set1_epi64x(u64_as_i64_bits(0_u64.wrapping_sub(PRIME_1))),
            );

            while offset + 32 <= length {
                lane_1 = round_lanes!(lane_1, load_u64x4!(offset), prime_1, prime_2);
                lane_2 = round_lanes!(lane_2, load_u64x4!(offset + 8), prime_1, prime_2);
                lane_3 = round_lanes!(lane_3, load_u64x4!(offset + 16), prime_1, prime_2);
                lane_4 = round_lanes!(lane_4, load_u64x4!(offset + 24), prime_1, prime_2);
                offset += 32;
            }

            let combined = _mm256_add_epi64(
                _mm256_add_epi64(rotate_left!(lane_1, 1), rotate_left!(lane_2, 7)),
                _mm256_add_epi64(rotate_left!(lane_3, 12), rotate_left!(lane_4, 18)),
            );
            let combined = merge_lanes!(combined, lane_1, zero, prime_1, prime_2, prime_4);
            let combined = merge_lanes!(combined, lane_2, zero, prime_1, prime_2, prime_4);
            let combined = merge_lanes!(combined, lane_3, zero, prime_1, prime_2, prime_4);
            merge_lanes!(combined, lane_4, zero, prime_1, prime_2, prime_4)
        } else {
            _mm256_add_epi64(seed_lanes, prime_5)
        };

        let length_lane = i64::try_from(length).unwrap_or(i64::MAX);
        hash = _mm256_add_epi64(hash, _mm256_set1_epi64x(length_lane));
        while offset + 8 <= length {
            let mixed = round_lanes!(zero, load_u64x4!(offset), prime_1, prime_2);
            hash = _mm256_xor_si256(hash, mixed);
            hash = _mm256_add_epi64(multiply!(rotate_left!(hash, 27), prime_1), prime_4);
            offset += 8;
        }
        if offset + 4 <= length {
            let words = _mm256_set_epi64x(
                i64::from(read_u32(&inputs[3][offset..offset + 4])),
                i64::from(read_u32(&inputs[2][offset..offset + 4])),
                i64::from(read_u32(&inputs[1][offset..offset + 4])),
                i64::from(read_u32(&inputs[0][offset..offset + 4])),
            );
            hash = _mm256_xor_si256(hash, multiply!(words, prime_1));
            hash = _mm256_add_epi64(multiply!(rotate_left!(hash, 23), prime_2), prime_3);
            offset += 4;
        }
        while offset < length {
            let bytes = _mm256_set_epi64x(
                i64::from(inputs[3][offset]),
                i64::from(inputs[2][offset]),
                i64::from(inputs[1][offset]),
                i64::from(inputs[0][offset]),
            );
            hash = _mm256_xor_si256(hash, multiply!(bytes, prime_5));
            hash = multiply!(rotate_left!(hash, 11), prime_1);
            offset += 1;
        }

        hash = _mm256_xor_si256(hash, _mm256_srli_epi64::<33>(hash));
        hash = multiply!(hash, prime_2);
        hash = _mm256_xor_si256(hash, _mm256_srli_epi64::<29>(hash));
        hash = multiply!(hash, prime_3);
        hash = _mm256_xor_si256(hash, _mm256_srli_epi64::<32>(hash));

        let mut output = [0_u64; 4];
        #[allow(clippy::cast_ptr_alignment)]
        let output_pointer = output.as_mut_ptr().cast::<__m256i>();
        _mm256_storeu_si256(output_pointer, hash);
        output
    }
}

fn round(accumulator: u64, lane: u64) -> u64 {
    accumulator
        .wrapping_add(lane.wrapping_mul(PRIME_2))
        .rotate_left(31)
        .wrapping_mul(PRIME_1)
}

fn merge_round(accumulator: u64, lane: u64) -> u64 {
    (accumulator ^ round(0, lane))
        .wrapping_mul(PRIME_1)
        .wrapping_add(PRIME_4)
}

fn avalanche(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(PRIME_2);
    hash ^= hash >> 29;
    hash = hash.wrapping_mul(PRIME_3);
    hash ^ (hash >> 32)
}

fn take_prefix<const N: usize>(input: &[u8]) -> Option<(&[u8; N], &[u8])> {
    if input.len() < N {
        return None;
    }
    let (prefix, tail) = input.split_at(N);
    let array = <&[u8; N]>::try_from(prefix).ok()?;
    Some((array, tail))
}

fn read_u64(bytes: &[u8]) -> u64 {
    match bytes {
        [
            byte_0,
            byte_1,
            byte_2,
            byte_3,
            byte_4,
            byte_5,
            byte_6,
            byte_7,
        ] => u64::from_le_bytes([
            *byte_0, *byte_1, *byte_2, *byte_3, *byte_4, *byte_5, *byte_6, *byte_7,
        ]),
        _ => 0,
    }
}

fn read_u32(bytes: &[u8]) -> u32 {
    match bytes {
        [a, b, c, d] => u32::from_le_bytes([*a, *b, *c, *d]),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_official_seed_zero_vectors() {
        assert_eq!(xxh64(b"", 0), 0xef46_db37_51d8_e999);
        assert_eq!(xxh64(b"a", 0), 0xd24e_c4f1_a98c_6e5b);
        assert_eq!(xxh64(b"abc", 0), 0x44bc_2cf5_ad77_0999);
    }

    #[test]
    fn hashes_every_tail_length() {
        let bytes: Vec<u8> = (0..96).collect();
        let hashes: Vec<u64> = (0..bytes.len())
            .map(|length| xxh64(&bytes[..length], 1))
            .collect();
        assert!(hashes.windows(2).all(|pair| pair[0] != pair[1]));
    }

    #[test]
    fn simd_batch_matches_scalar_for_every_tail_length() {
        let lanes: [Vec<u8>; 4] = std::array::from_fn(|lane| {
            (0_u16..160)
                .map(|byte| {
                    u8::try_from(byte)
                        .unwrap_or(u8::MAX)
                        .wrapping_mul(u8::try_from(lane + 1).unwrap_or(u8::MAX))
                        .wrapping_add(u8::try_from(lane * 17).unwrap_or(u8::MAX))
                })
                .collect()
        });
        for seed in [0, 1, 0x0123_4567_89ab_cdef, u64::MAX] {
            for length in 0..=128 {
                let inputs = [
                    &lanes[0][..length],
                    &lanes[1][..length],
                    &lanes[2][..length],
                    &lanes[3][..length],
                ];
                let expected = inputs.map(|input| xxh64(input, seed));
                assert_eq!(xxh64_batch4(inputs, seed), expected);
            }
        }
    }

    #[test]
    fn unequal_batch_lengths_use_scalar_fallback() {
        let inputs: [&[u8]; 4] = [b"a", b"ab", b"abc", b"abcd"];
        assert_eq!(
            xxh64_batch4(inputs, 42),
            inputs.map(|input| xxh64(input, 42))
        );
    }
}

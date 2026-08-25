const PRIME_1: u64 = 11_400_714_785_074_694_791;
const PRIME_2: u64 = 14_029_467_366_897_019_727;
const PRIME_3: u64 = 1_609_587_929_392_839_161;
const PRIME_4: u64 = 9_650_029_242_287_828_579;
const PRIME_5: u64 = 2_870_177_450_012_600_261;

#[must_use]
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
}

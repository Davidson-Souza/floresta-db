// SPDX-License-Identifier: MIT OR Apache-2.0

//! Constants and alignment helpers for the persistent file format.
//!
//! Offsets in this module are shared by serialization, traversal, allocation,
//! and recovery code; changing them requires a format-version review.

pub(crate) const FORMAT_PAGE_SIZE: u64 = 65_536;
pub(crate) const DATA_START: u64 = FORMAT_PAGE_SIZE;
pub(crate) const HEADS_START: u64 = FORMAT_PAGE_SIZE;

pub(crate) const NODE_SIZE: usize = 32;
pub(crate) const NODE_SIZE_U64: u64 = 32;
pub(crate) const NODE_ALIGNMENT: u64 = 32;
pub(crate) const NODE_KEY_OFFSET: usize = 0;
pub(crate) const NODE_VALUE_OFFSET: usize = 16;
pub(crate) const NODE_POINTER_OFFSET: usize = 24;
pub(crate) const NODE_CHECKSUM_SHIFT: u32 = 48;
pub(crate) const NODE_POINTER_MASK: u64 = (1_u64 << NODE_CHECKSUM_SHIFT) - 1;
pub(crate) const NODE_CHECKSUM_SEED: u64 = 0x4341_534e_4f44_4532;

pub(crate) const VALUE_BLOB_TAG: u64 = 1 << 63;
pub(crate) const VALUE_OFFSET_MASK: u64 = !VALUE_BLOB_TAG;
pub(crate) const BLOB_HEADER_SIZE: usize = 8;
pub(crate) const BLOB_CHECKSUM_SEED: u64 = 0x4341_5342_4c4f_4232;

pub(crate) const fn align_up(value: u64, alignment: u64) -> Option<u64> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return None;
    }
    let mask = alignment - 1;
    match value.checked_add(mask) {
        Some(sum) => Some(sum & !mask),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligns_without_overflow() {
        assert_eq!(align_up(0, 8), Some(0));
        assert_eq!(align_up(1, 8), Some(8));
        assert_eq!(align_up(8, 8), Some(8));
        assert_eq!(align_up(u64::MAX, 8), None);
        assert_eq!(align_up(1, 3), None);
    }

    #[test]
    fn packs_two_aligned_nodes_per_cache_line() {
        assert_eq!(NODE_SIZE, 32);
        assert_eq!(NODE_ALIGNMENT, 32);
        assert_eq!(NODE_POINTER_OFFSET, 24);
    }
}

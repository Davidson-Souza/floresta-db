pub(crate) const PAGE_SIZE: u64 = 4_096;
pub(crate) const DATA_START: u64 = PAGE_SIZE;
pub(crate) const NODE_ALIGNMENT: u64 = 8;
pub(crate) const NODE_FIXED_SIZE: u64 = 48;
pub(crate) const HEADS_START: u64 = PAGE_SIZE;

pub(crate) const NODE_NEXT_OFFSET: u64 = 0;
pub(crate) const NODE_HASH_OFFSET: usize = 8;
pub(crate) const NODE_BLOB_OFFSET: usize = 16;
pub(crate) const NODE_BLOB_LENGTH_OFFSET: usize = 24;
pub(crate) const NODE_MAGIC_OFFSET: usize = 32;
pub(crate) const NODE_CHECKSUM_OFFSET: usize = 40;
pub(crate) const NODE_KEY_OFFSET: usize = 48;
pub(crate) const NODE_MAGIC: u64 = 0x4341_534e_4f44_4531;
pub(crate) const DELETED_BIT: u64 = 1;

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
}

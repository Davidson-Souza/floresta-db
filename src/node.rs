// SPDX-License-Identifier: MIT OR Apache-2.0

//! Serialization and validation of 32-byte bucket-chain nodes.
//!
//! Every node contains a 16-byte key, one tagged 64-bit value, and one packed
//! pointer. The pointer's upper 16 bits hold the checksum of the immutable key
//! and value while its lower 48 bits hold the next-node offset.

use std::sync::atomic::Ordering;

use crate::allocator::{Allocation, BlockAllocator};
use crate::config::KEY_SIZE;
use crate::error::{Error, Result};
use crate::hash::xxh64;
use crate::layout::{
    BLOB_CHECKSUM_SEED, BLOB_HEADER_SIZE, NODE_ALIGNMENT, NODE_CHECKSUM_SEED, NODE_CHECKSUM_SHIFT,
    NODE_KEY_OFFSET, NODE_POINTER_MASK, NODE_POINTER_OFFSET, NODE_SIZE, NODE_VALUE_OFFSET,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Node {
    pub(crate) offset: u64,
    pub(crate) next: u64,
    pub(crate) value: u64,
    pub(crate) key: [u8; KEY_SIZE],
}

pub(crate) fn allocate_node(body: &BlockAllocator, key: &[u8], value: u64) -> Result<Allocation> {
    let bytes = serialize_node(key, value)?;
    let allocation = body.allocate(NODE_SIZE, NODE_ALIGNMENT)?;
    if let Err(error) = body.write(allocation, &bytes) {
        let _released = body.release(allocation);
        return Err(error);
    }
    Ok(allocation)
}

pub(crate) fn write_node(
    body: &BlockAllocator,
    allocation: Allocation,
    key: &[u8],
    value: u64,
) -> Result<()> {
    let bytes = serialize_node(key, value)?;
    body.write(allocation, &bytes)
}

fn serialize_node(key: &[u8], value: u64) -> Result<[u8; NODE_SIZE]> {
    let key = <&[u8; KEY_SIZE]>::try_from(key).map_err(|_| Error::InvalidKeyLength {
        expected: KEY_SIZE,
        actual: key.len(),
    })?;
    let mut bytes = [0_u8; NODE_SIZE];
    bytes[NODE_KEY_OFFSET..NODE_VALUE_OFFSET].copy_from_slice(key);
    bytes[NODE_VALUE_OFFSET..NODE_POINTER_OFFSET].copy_from_slice(&value.to_le_bytes());
    let pointer = u64::from(node_checksum(key, value)) << NODE_CHECKSUM_SHIFT;
    bytes[NODE_POINTER_OFFSET..].copy_from_slice(&pointer.to_le_bytes());
    Ok(bytes)
}

pub(crate) fn read_node(body: &BlockAllocator, offset: u64) -> Result<Node> {
    if offset == 0 {
        return Err(Error::Corrupt("body node offset is null"));
    }
    if offset % NODE_ALIGNMENT != 0 {
        return Err(Error::Corrupt("body node offset is not cache-line aligned"));
    }
    let mut bytes = [0_u8; NODE_SIZE];
    body.read_into(offset, &mut bytes)?;
    let key = <[u8; KEY_SIZE]>::try_from(&bytes[NODE_KEY_OFFSET..NODE_VALUE_OFFSET])
        .map_err(|_| Error::Corrupt("body node key is truncated"))?;
    let value = read_u64(&bytes, NODE_VALUE_OFFSET)?;
    let pointer = read_u64(&bytes, NODE_POINTER_OFFSET)?;
    let checksum = u16::try_from(pointer >> NODE_CHECKSUM_SHIFT)
        .map_err(|_| Error::Corrupt("body node checksum overflow"))?;
    if checksum != node_checksum(&key, value) {
        return Err(Error::Corrupt("body node checksum does not match"));
    }
    Ok(Node {
        offset,
        next: pointer & NODE_POINTER_MASK,
        value,
        key,
    })
}

pub(crate) fn read_next(body: &BlockAllocator, offset: u64) -> Result<u64> {
    let pointer_offset = offset
        .checked_add(NODE_POINTER_OFFSET as u64)
        .ok_or(Error::Corrupt("body node pointer offset overflow"))?;
    let mut bytes = [0_u8; size_of::<u64>()];
    body.read_into(pointer_offset, &mut bytes)?;
    Ok(u64::from_le_bytes(bytes) & NODE_POINTER_MASK)
}

pub(crate) fn tombstone_node(body: &BlockAllocator, offset: u64) -> Result<()> {
    body.atomic_u64(offset + NODE_POINTER_OFFSET as u64)?
        .store(0, Ordering::Release);
    Ok(())
}

pub(crate) fn set_private_next(body: &BlockAllocator, node: u64, old: u64, new: u64) -> Result<()> {
    if old > NODE_POINTER_MASK || new > NODE_POINTER_MASK {
        return Err(Error::CapacityExhausted("packed node pointer"));
    }
    let pointer = body.atomic_u64(node + NODE_POINTER_OFFSET as u64)?;
    let observed = read_pointer_word(body, node)?;
    if observed & NODE_POINTER_MASK != old {
        return Err(Error::Corrupt("private body node next pointer changed"));
    }
    let replacement = observed & !NODE_POINTER_MASK | new;
    pointer
        .compare_exchange(observed, replacement, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| Error::Corrupt("private body node next pointer changed"))?;
    Ok(())
}

pub(crate) fn compare_exchange_next(
    body: &BlockAllocator,
    node: u64,
    expected: u64,
    replacement: u64,
) -> Result<bool> {
    if expected > NODE_POINTER_MASK || replacement > NODE_POINTER_MASK {
        return Err(Error::CapacityExhausted("packed node pointer"));
    }
    let pointer = body.atomic_u64(node + NODE_POINTER_OFFSET as u64)?;
    let observed = read_pointer_word(body, node)?;
    if observed & NODE_POINTER_MASK != expected {
        return Ok(false);
    }
    let replacement = observed & !NODE_POINTER_MASK | replacement;
    Ok(pointer
        .compare_exchange(observed, replacement, Ordering::AcqRel, Ordering::Acquire)
        .is_ok())
}

pub(crate) fn blob_checksum(value: &[u8]) -> u32 {
    let bytes = xxh64(value, BLOB_CHECKSUM_SEED).to_le_bytes();
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub(crate) fn blob_header(value: &[u8]) -> Result<[u8; BLOB_HEADER_SIZE]> {
    let length = u32::try_from(value.len()).map_err(|_| Error::CapacityExhausted("blob length"))?;
    let mut header = [0_u8; BLOB_HEADER_SIZE];
    header[..4].copy_from_slice(&length.to_le_bytes());
    header[4..].copy_from_slice(&blob_checksum(value).to_le_bytes());
    Ok(header)
}

pub(crate) fn decode_blob_header(bytes: [u8; BLOB_HEADER_SIZE]) -> (u32, u32) {
    let length = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let checksum = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    (length, checksum)
}

fn read_pointer_word(body: &BlockAllocator, offset: u64) -> Result<u64> {
    let pointer_offset = offset
        .checked_add(NODE_POINTER_OFFSET as u64)
        .ok_or(Error::Corrupt("body node pointer offset overflow"))?;
    let mut bytes = [0_u8; size_of::<u64>()];
    body.read_into(pointer_offset, &mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn node_checksum(key: &[u8; KEY_SIZE], value: u64) -> u16 {
    let checksum = xxh64(key, NODE_CHECKSUM_SEED) ^ value.rotate_left(29);
    let folded = checksum ^ (checksum >> 16) ^ (checksum >> 32) ^ (checksum >> 48);
    let bytes = folded.to_le_bytes();
    u16::from_le_bytes([bytes[0], bytes[1]]).max(1)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or(Error::Corrupt("node field overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or(Error::Corrupt("node field is out of bounds"))?;
    let array = <[u8; 8]>::try_from(source).map_err(|_| Error::Corrupt("invalid u64 field"))?;
    Ok(u64::from_le_bytes(array))
}

#[cfg(all(test, not(miri), any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    fn test_paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
        let prefix = format!("floresta-db-node-{}-{name}", std::process::id());
        let directory = std::env::temp_dir();
        (
            directory.join(format!("{prefix}.body")),
            directory.join(format!("{prefix}.counts")),
        )
    }

    #[test]
    fn round_trips_packed_node_and_updates_pointer() -> Result<()> {
        let paths = test_paths("round-trip");
        let _ignored_body = std::fs::remove_file(&paths.0);
        let _ignored_counts = std::fs::remove_file(&paths.1);
        let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024, 64 * 1_024)?;
        let key = *b"0123456789abcdef";
        let allocation = allocate_node(&allocator, &key, 42)?;
        assert_eq!(allocation.offset % 32, 0);
        set_private_next(&allocator, allocation.offset, 0, allocation.offset)?;
        let node = read_node(&allocator, allocation.offset)?;
        assert_eq!(node.key, key);
        assert_eq!(node.value, 42);
        assert_eq!(node.next, allocation.offset);
        allocator.release(allocation)?;
        allocator.sync_all()?;
        drop(allocator);
        std::fs::remove_file(paths.0)?;
        std::fs::remove_file(paths.1)?;
        Ok(())
    }

    #[test]
    fn detects_static_node_corruption() -> Result<()> {
        let paths = test_paths("corrupt");
        let _ignored_body = std::fs::remove_file(&paths.0);
        let _ignored_counts = std::fs::remove_file(&paths.1);
        let allocator = BlockAllocator::create(&paths.0, &paths.1, 64 * 1_024, 64 * 1_024)?;
        let allocation = allocate_node(&allocator, b"0123456789abcdef", 7)?;
        allocator.write_at(allocation, 0, &[1])?;
        assert!(matches!(
            read_node(&allocator, allocation.offset),
            Err(Error::Corrupt(_))
        ));
        allocator.release(allocation)?;
        allocator.sync_all()?;
        drop(allocator);
        std::fs::remove_file(paths.0)?;
        std::fs::remove_file(paths.1)?;
        Ok(())
    }
}

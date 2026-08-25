use std::sync::atomic::Ordering;

use crate::allocator::{Allocation, BlockAllocator};
use crate::error::{Error, Result};
use crate::hash::xxh64;
use crate::layout::{
    DELETED_BIT, NODE_BLOB_LENGTH_OFFSET, NODE_BLOB_OFFSET, NODE_CHECKSUM_OFFSET, NODE_HASH_OFFSET,
    NODE_KEY_OFFSET, NODE_MAGIC, NODE_MAGIC_OFFSET, NODE_NEXT_OFFSET,
};

#[derive(Debug)]
pub(crate) struct Node {
    pub(crate) offset: u64,
    pub(crate) next: u64,
    pub(crate) hash: u64,
    pub(crate) blob_offset: u64,
    pub(crate) blob_length: u64,
    pub(crate) key: Vec<u8>,
}

impl Node {
    pub(crate) fn deleted(&self) -> bool {
        self.next & DELETED_BIT != 0
    }

    pub(crate) fn successor(&self) -> u64 {
        self.next & !DELETED_BIT
    }
}

pub(crate) fn allocate_node(
    body: &BlockAllocator,
    node_size: u64,
    key: &[u8],
    hash: u64,
    blob_offset: u64,
    blob_length: u64,
) -> Result<Allocation> {
    let node_size_usize = usize::try_from(node_size)
        .map_err(|_| Error::InvalidConfig("node size does not fit memory"))?;
    let allocation = body.allocate(node_size_usize, 8)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(node_size_usize)
        .map_err(|_| Error::OutOfMemory)?;
    bytes.resize(node_size_usize, 0);
    write_u64(&mut bytes, NODE_HASH_OFFSET, hash)?;
    write_u64(&mut bytes, NODE_BLOB_OFFSET, blob_offset)?;
    write_u64(&mut bytes, NODE_BLOB_LENGTH_OFFSET, blob_length)?;
    write_u64(&mut bytes, NODE_MAGIC_OFFSET, NODE_MAGIC)?;
    let key_end = NODE_KEY_OFFSET
        .checked_add(key.len())
        .ok_or(Error::InvalidConfig("node key range overflow"))?;
    let key_destination = bytes
        .get_mut(NODE_KEY_OFFSET..key_end)
        .ok_or(Error::InvalidConfig("key does not fit body node"))?;
    key_destination.copy_from_slice(key);
    let checksum = node_checksum(hash, blob_offset, blob_length, key);
    write_u64(&mut bytes, NODE_CHECKSUM_OFFSET, checksum)?;
    body.write(allocation, &bytes)?;
    Ok(allocation)
}

pub(crate) fn read_node(
    body: &BlockAllocator,
    offset: u64,
    node_size: u64,
    key_size: usize,
) -> Result<Node> {
    if offset == 0 || offset & DELETED_BIT != 0 {
        return Err(Error::Corrupt("body node offset is null or tagged"));
    }
    let next = body
        .atomic_u64(offset + NODE_NEXT_OFFSET)?
        .load(Ordering::Acquire);
    let static_offset = offset
        .checked_add(size_of::<u64>() as u64)
        .ok_or(Error::Corrupt("body node offset overflow"))?;
    let static_length = node_size
        .checked_sub(size_of::<u64>() as u64)
        .ok_or(Error::Corrupt("body node is too small"))?;
    let static_length = usize::try_from(static_length)
        .map_err(|_| Error::Corrupt("body node size does not fit memory"))?;
    let bytes = body.read(static_offset, static_length)?;
    let hash = read_static_u64(&bytes, NODE_HASH_OFFSET)?;
    let blob_offset = read_static_u64(&bytes, NODE_BLOB_OFFSET)?;
    let blob_length = read_static_u64(&bytes, NODE_BLOB_LENGTH_OFFSET)?;
    if read_static_u64(&bytes, NODE_MAGIC_OFFSET)? != NODE_MAGIC {
        return Err(Error::Corrupt("body node magic does not match"));
    }
    let checksum = read_static_u64(&bytes, NODE_CHECKSUM_OFFSET)?;
    let key_start = NODE_KEY_OFFSET
        .checked_sub(size_of::<u64>())
        .ok_or(Error::Corrupt("node key offset underflow"))?;
    let key_end = key_start
        .checked_add(key_size)
        .ok_or(Error::Corrupt("node key range overflow"))?;
    let key_source = bytes
        .get(key_start..key_end)
        .ok_or(Error::Corrupt("node key is out of bounds"))?;
    let mut key = Vec::new();
    key.try_reserve_exact(key_size)
        .map_err(|_| Error::OutOfMemory)?;
    key.extend_from_slice(key_source);
    if checksum != node_checksum(hash, blob_offset, blob_length, &key) {
        return Err(Error::Corrupt("body node checksum does not match"));
    }
    Ok(Node {
        offset,
        next,
        hash,
        blob_offset,
        blob_length,
        key,
    })
}

pub(crate) fn set_private_next(body: &BlockAllocator, node: u64, old: u64, new: u64) -> Result<()> {
    body.atomic_u64(node + NODE_NEXT_OFFSET)?
        .compare_exchange(old, new, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| Error::Corrupt("private body node next pointer changed"))?;
    Ok(())
}

fn node_checksum(hash: u64, blob_offset: u64, blob_length: u64, key: &[u8]) -> u64 {
    let mut checksum =
        hash.rotate_left(13) ^ blob_offset.rotate_left(29) ^ blob_length.rotate_left(47);
    checksum ^= xxh64(key, NODE_MAGIC);
    checksum
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) -> Result<()> {
    let end = offset
        .checked_add(8)
        .ok_or(Error::Corrupt("u64 write overflow"))?;
    let destination = bytes
        .get_mut(offset..end)
        .ok_or(Error::Corrupt("u64 write is out of bounds"))?;
    destination.copy_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_static_u64(bytes: &[u8], absolute_offset: usize) -> Result<u64> {
    let offset = absolute_offset
        .checked_sub(size_of::<u64>())
        .ok_or(Error::Corrupt("static node offset underflow"))?;
    let end = offset
        .checked_add(8)
        .ok_or(Error::Corrupt("u64 read overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or(Error::Corrupt("u64 read is out of bounds"))?;
    let array = <[u8; 8]>::try_from(source).map_err(|_| Error::Corrupt("invalid u64 field"))?;
    Ok(u64::from_le_bytes(array))
}

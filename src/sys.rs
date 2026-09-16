// SPDX-License-Identifier: MIT OR Apache-2.0

//! Minimal Linux syscall boundary for mapped storage.
//!
//! The wrappers centralize `mmap`, advice, synchronous writeback, allocation,
//! and hole punching while translating operating-system failures into [`Error`].

#![allow(dead_code)]

use std::ffi::{c_int, c_long, c_void};
use std::io;
use std::os::fd::RawFd;
use std::ptr::NonNull;

use crate::error::{Error, Result};

const PROT_READ: c_int = 0x1;
const PROT_WRITE: c_int = 0x2;
const MAP_SHARED: c_int = 0x01;
const MAP_POPULATE: c_int = 0x08_000;
const MS_SYNC: c_int = 0x4;
const MADV_RANDOM: c_int = 1;
const MADV_DONTDUMP: c_int = 16;
const POSIX_FADV_RANDOM: c_int = 1;
const FALLOC_FL_KEEP_SIZE: c_int = 0x01;
const FALLOC_FL_PUNCH_HOLE: c_int = 0x02;
const MAP_FAILED: *mut c_void = usize::MAX as *mut c_void;
const SC_PAGE_SIZE: c_int = 30;

unsafe extern "C" {
    fn mmap(
        address: *mut c_void,
        length: usize,
        protection: c_int,
        flags: c_int,
        file_descriptor: c_int,
        offset: i64,
    ) -> *mut c_void;
    fn munmap(address: *mut c_void, length: usize) -> c_int;
    fn madvise(address: *mut c_void, length: usize, advice: c_int) -> c_int;
    fn msync(address: *mut c_void, length: usize, flags: c_int) -> c_int;
    fn fallocate(file_descriptor: c_int, mode: c_int, offset: i64, length: i64) -> c_int;
    fn posix_fadvise(file_descriptor: c_int, offset: i64, length: i64, advice: c_int) -> c_int;
    fn sysconf(name: c_int) -> c_long;
}

pub(crate) fn page_size() -> Result<u64> {
    // SAFETY: sysconf has no pointer arguments and SC_PAGE_SIZE is a valid selector.
    let result = unsafe { sysconf(SC_PAGE_SIZE) };
    u64::try_from(result).map_err(|_| Error::Unsupported("cannot determine system page size"))
}

pub(crate) fn map_shared(
    file_descriptor: RawFd,
    length: usize,
    populate: bool,
) -> Result<NonNull<u8>> {
    let flags = if populate {
        MAP_SHARED | MAP_POPULATE
    } else {
        MAP_SHARED
    };
    // SAFETY: the descriptor and mapping length are validated by the caller. The returned
    // region is owned by the corresponding MappedFile until munmap.
    let pointer = unsafe {
        mmap(
            std::ptr::null_mut(),
            length,
            PROT_READ | PROT_WRITE,
            flags,
            file_descriptor,
            0,
        )
    };
    if pointer == MAP_FAILED {
        return Err(io::Error::last_os_error().into());
    }
    NonNull::new(pointer.cast::<u8>()).ok_or(Error::Unsupported("mmap returned a null address"))
}

pub(crate) fn unmap(pointer: NonNull<u8>, length: usize) -> Result<()> {
    // SAFETY: pointer and length identify one active mapping owned by MappedFile.
    let result = unsafe { munmap(pointer.as_ptr().cast::<c_void>(), length) };
    errno_result(result)
}

pub(crate) fn advise_random(
    pointer: NonNull<u8>,
    length: usize,
    file_descriptor: RawFd,
) -> Result<()> {
    // SAFETY: pointer and length identify an active mapped range.
    let result = unsafe { madvise(pointer.as_ptr().cast::<c_void>(), length, MADV_RANDOM) };
    errno_result(result)?;
    // POSIX returns an error number directly rather than setting errno.
    // SAFETY: the descriptor is open and the range covers the complete file.
    let advice_result = unsafe { posix_fadvise(file_descriptor, 0, 0, POSIX_FADV_RANDOM) };
    if advice_result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(advice_result).into())
    }
}

pub(crate) fn advise_heads(pointer: NonNull<u8>, length: usize) -> Result<()> {
    // SAFETY: pointer and length identify an active mapped range.
    let no_dump = unsafe { madvise(pointer.as_ptr().cast::<c_void>(), length, MADV_DONTDUMP) };
    errno_result(no_dump)
}

pub(crate) fn sync(pointer: NonNull<u8>, length: usize) -> Result<()> {
    // SAFETY: pointer is page aligned and length covers an active mapped range.
    let result = unsafe { msync(pointer.as_ptr().cast::<c_void>(), length, MS_SYNC) };
    errno_result(result)
}

pub(crate) fn reserve(file_descriptor: RawFd, offset: u64, length: u64) -> Result<()> {
    let offset = to_i64(offset)?;
    let length = to_i64(length)?;
    // SAFETY: arguments are scalar values and the descriptor is open for writing.
    let result = unsafe { fallocate(file_descriptor, 0, offset, length) };
    errno_result(result)
}

pub(crate) fn punch(file_descriptor: RawFd, offset: u64, length: u64) -> Result<()> {
    let offset = to_i64(offset)?;
    let length = to_i64(length)?;
    // SAFETY: arguments are scalar values and the descriptor is open for writing.
    let result = unsafe {
        fallocate(
            file_descriptor,
            FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            offset,
            length,
        )
    };
    errno_result(result)
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::InvalidConfig("file offset exceeds Linux off_t"))
}

fn errno_result(result: c_int) -> Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().into())
    }
}

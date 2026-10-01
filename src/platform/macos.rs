// SPDX-License-Identifier: MIT OR Apache-2.0

//! macOS shared-mapping and physical-allocation backend.
//!
//! The backend first reserves one stable anonymous address range. It then
//! replaces format-page-aligned portions with shared file mappings as the file
//! grows, so existing addresses never move and no thread observes an unmapped
//! published range.

use std::ffi::{c_int, c_void};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::{Error, Result};

const PROT_NONE: c_int = 0x0;
const PROT_READ: c_int = 0x1;
const PROT_WRITE: c_int = 0x2;
const MAP_SHARED: c_int = 0x0001;
const MAP_PRIVATE: c_int = 0x0002;
const MAP_FIXED: c_int = 0x0010;
const MAP_ANON: c_int = 0x1000;
const MS_SYNC: c_int = 0x10;
const MADV_WILLNEED: c_int = 3;
const F_PREALLOCATE: c_int = 42;
const F_ALLOCATECONTIG: u32 = 0x0000_0002;
const F_ALLOCATEALL: u32 = 0x0000_0004;
const F_PEOFPOSMODE: c_int = 3;
const MAP_FAILED: *mut c_void = usize::MAX as *mut c_void;

#[repr(C)]
struct FileStore {
    flags: u32,
    position_mode: c_int,
    offset: i64,
    length: i64,
    bytes_allocated: i64,
}

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
    fn fcntl(file_descriptor: c_int, command: c_int, ...) -> c_int;
}

pub(crate) struct Mapping {
    pointer: NonNull<u8>,
    length: usize,
    committed: AtomicUsize,
}

impl Mapping {
    pub(crate) fn new(
        file: &File,
        length: usize,
        file_length: usize,
        populate: bool,
    ) -> Result<Self> {
        if file_length == 0 || file_length > length {
            return Err(Error::InvalidConfig("invalid macOS mapped file length"));
        }
        // SAFETY: this reserves inaccessible private address space. File-backed segments replace
        // it below, and Drop releases the complete range exactly once.
        let raw = unsafe {
            mmap(
                std::ptr::null_mut(),
                length,
                PROT_NONE,
                MAP_PRIVATE | MAP_ANON,
                -1,
                0,
            )
        };
        if raw == MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        let pointer = NonNull::new(raw.cast::<u8>())
            .ok_or(Error::Unsupported("macOS mmap returned a null address"))?;
        let mapping = Self {
            pointer,
            length,
            committed: AtomicUsize::new(0),
        };
        if let Err(error) = mapping.map_file_segment(file, 0, file_length) {
            drop(mapping);
            return Err(error);
        }
        mapping.committed.store(file_length, Ordering::Release);
        if populate {
            if let Err(error) = mapping.advise_range(file_length, MADV_WILLNEED) {
                drop(mapping);
                return Err(error);
            }
        }
        Ok(mapping)
    }

    pub(crate) fn pointer(&self) -> NonNull<u8> {
        self.pointer
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn advise_heads(&self) -> Result<()> {
        Ok(())
    }

    pub(crate) fn sync(&self, length: usize) -> Result<()> {
        if length > read_shared(&self.committed) {
            return Err(Error::Corrupt("sync range exceeds macOS file mapping"));
        }
        // SAFETY: the mapping base is page aligned and length lies inside mapped file segments.
        errno_result(unsafe { msync(self.pointer.as_ptr().cast::<c_void>(), length, MS_SYNC) })
    }

    pub(crate) fn grow(&self, file: &File, old: usize, new: usize) -> Result<()> {
        if read_shared(&self.committed) != old || old >= new || new > self.length {
            return Err(Error::Corrupt("invalid macOS mapping growth range"));
        }
        self.map_file_segment(file, old, new - old)?;
        self.committed.store(new, Ordering::Release);
        Ok(())
    }

    fn map_file_segment(&self, file: &File, offset: usize, length: usize) -> Result<()> {
        let file_offset = i64::try_from(offset)
            .map_err(|_| Error::InvalidConfig("mapping offset exceeds macOS off_t"))?;
        // SAFETY: offset and length select inaccessible reserved pages inside this Mapping. The
        // file has already been extended through the segment, and MAP_FIXED replaces only those
        // unpublished pages with a shared file view.
        let target = unsafe { self.pointer.as_ptr().add(offset) };
        let raw = unsafe {
            mmap(
                target.cast::<c_void>(),
                length,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_FIXED,
                file.as_raw_fd(),
                file_offset,
            )
        };
        if raw == MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        if raw.cast::<u8>() != target {
            // SAFETY: a successful unexpected mapping must be released before returning.
            let _unmapped = unsafe { munmap(raw, length) };
            return Err(Error::Unsupported(
                "macOS MAP_FIXED did not preserve the reserved address",
            ));
        }
        Ok(())
    }

    fn advise_range(&self, length: usize, advice: c_int) -> Result<()> {
        // SAFETY: pointer and length identify contiguous committed file segments.
        errno_result(unsafe { madvise(self.pointer.as_ptr().cast::<c_void>(), length, advice) })
    }
}

fn read_shared(atomic: &AtomicUsize) -> usize {
    // SAFETY: supported targets provide aligned single-copy pointer-width reads. The acquire
    // fence orders mapped segments initialized before the committed length was published.
    let value = unsafe { std::ptr::read_volatile(atomic.as_ptr()) };
    std::sync::atomic::fence(Ordering::Acquire);
    value
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the complete range was returned by one anonymous mmap and has not been released.
        let _result = unsafe { munmap(self.pointer.as_ptr().cast::<c_void>(), self.length) };
    }
}

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn ensure_supported() -> Result<()> {
    Ok(())
}

pub(crate) fn reserve(file: &File, offset: u64, length: u64) -> Result<()> {
    let end = offset
        .checked_add(length)
        .ok_or(Error::InvalidConfig("file reservation range overflow"))?;
    if file.metadata()?.len() != end {
        return Err(Error::Corrupt(
            "macOS can only reserve the newly grown file tail",
        ));
    }
    let length = i64::try_from(length)
        .map_err(|_| Error::InvalidConfig("file reservation exceeds macOS off_t"))?;
    let mut store = FileStore {
        flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        position_mode: F_PEOFPOSMODE,
        offset: 0,
        length,
        bytes_allocated: 0,
    };
    // SAFETY: FileStore has the Darwin fstore_t layout and remains live for the call.
    let contiguous = unsafe { fcntl(file.as_raw_fd(), F_PREALLOCATE, &raw mut store) };
    if contiguous == -1 {
        store.flags = F_ALLOCATEALL;
        store.bytes_allocated = 0;
        // SAFETY: same as above; this retries without the contiguous-space preference.
        let fallback = unsafe { fcntl(file.as_raw_fd(), F_PREALLOCATE, &raw mut store) };
        if fallback == -1 {
            return Err(io::Error::last_os_error().into());
        }
    }
    if store.bytes_allocated < length {
        return Err(Error::CapacityExhausted("file reservation"));
    }
    Ok(())
}

fn errno_result(result: c_int) -> Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error().into())
    }
}

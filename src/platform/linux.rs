// SPDX-License-Identifier: MIT OR Apache-2.0

//! Linux shared-mapping and physical-allocation backend.

use std::ffi::{c_int, c_void};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::ptr::NonNull;

use crate::error::{Error, Result};

const PROT_READ: c_int = 0x1;
const PROT_WRITE: c_int = 0x2;
const MAP_SHARED: c_int = 0x01;
const MAP_POPULATE: c_int = 0x08_000;
const MS_SYNC: c_int = 0x4;
const MADV_DONTDUMP: c_int = 16;
const MAP_FAILED: *mut c_void = usize::MAX as *mut c_void;

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
}

pub(crate) struct Mapping {
    pointer: NonNull<u8>,
    length: usize,
}

impl Mapping {
    pub(crate) fn new(
        file: &File,
        length: usize,
        _file_length: usize,
        populate: bool,
    ) -> Result<Self> {
        let flags = if populate {
            MAP_SHARED | MAP_POPULATE
        } else {
            MAP_SHARED
        };
        // SAFETY: the file is open for reading and writing, length is nonzero, and the returned
        // mapping is exclusively owned by this Mapping until munmap in Drop.
        let pointer = unsafe {
            mmap(
                std::ptr::null_mut(),
                length,
                PROT_READ | PROT_WRITE,
                flags,
                file.as_raw_fd(),
                0,
            )
        };
        if pointer == MAP_FAILED {
            return Err(io::Error::last_os_error().into());
        }
        let pointer = NonNull::new(pointer.cast::<u8>())
            .ok_or(Error::Unsupported("Linux mmap returned a null address"))?;
        Ok(Self { pointer, length })
    }

    pub(crate) fn pointer(&self) -> NonNull<u8> {
        self.pointer
    }

    pub(crate) fn advise_heads(&self) -> Result<()> {
        // SAFETY: pointer and length identify this live mapping.
        errno_result(unsafe {
            madvise(
                self.pointer.as_ptr().cast::<c_void>(),
                self.length,
                MADV_DONTDUMP,
            )
        })
    }

    pub(crate) fn sync(&self, length: usize) -> Result<()> {
        if length > self.length {
            return Err(Error::Corrupt("sync range exceeds Linux mapping"));
        }
        // SAFETY: the mapping base is page aligned and length lies inside the mapping.
        errno_result(unsafe { msync(self.pointer.as_ptr().cast::<c_void>(), length, MS_SYNC) })
    }

    #[allow(clippy::unnecessary_wraps, clippy::unused_self)]
    pub(crate) fn grow(&self, _file: &File, _old: usize, _new: usize) -> Result<()> {
        Ok(())
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this pair came from one successful mmap and is unmapped exactly once.
        let _result = unsafe { munmap(self.pointer.as_ptr().cast::<c_void>(), self.length) };
    }
}

#[allow(clippy::unnecessary_wraps)]
pub(crate) fn ensure_supported() -> Result<()> {
    Ok(())
}

pub(crate) fn reserve(file: &File, offset: u64, length: u64) -> Result<()> {
    let offset = to_i64(offset)?;
    let length = to_i64(length)?;
    // SAFETY: arguments are scalar values and the file is open for writing.
    errno_result(unsafe { fallocate(file.as_raw_fd(), 0, offset, length) })
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

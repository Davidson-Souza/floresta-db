// SPDX-License-Identifier: MIT OR Apache-2.0

//! Checked ownership of a shared memory-mapped file.
//!
//! [`MappedFile`] reserves a stable maximum mapping while allowing its backing
//! file to grow page-by-page. Range checks use the published file length, so no
//! thread accesses the portion of the mapping that still lies beyond EOF.

#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};
use crate::layout::PAGE_SIZE;
use crate::sys;

const GROWING_BIT: u64 = 1 << 63;

pub(crate) struct MappedFile {
    file: File,

    pointer: NonNull<u8>,

    mapping_length: usize,

    file_length: AtomicU64,
}

// SAFETY: MappedFile never changes its mapping address. Safe accessors either return atomics or
// copy bytes, while mutable raw access stays confined to unpublished allocator reservations.
unsafe impl Send for MappedFile {}
// SAFETY: see the Send implementation. Shared mutations are required to use mapped atomics.
unsafe impl Sync for MappedFile {}

impl MappedFile {
    pub(crate) fn create(
        path: &Path,
        length: u64,
        populate: bool,
        reserve_all: bool,
    ) -> Result<Self> {
        Self::create_with_lengths(path, length, length, populate, reserve_all)
    }

    pub(crate) fn create_growable(
        path: &Path,
        maximum_length: u64,
        initial_length: u64,
        populate: bool,
    ) -> Result<Self> {
        Self::create_with_lengths(path, maximum_length, initial_length, populate, false)
    }

    fn create_with_lengths(
        path: &Path,
        mapping_length: u64,
        file_length: u64,
        populate: bool,
        reserve_all: bool,
    ) -> Result<Self> {
        validate_mapping_length(mapping_length)?;
        validate_mapping_length(file_length)?;
        if file_length > mapping_length {
            return Err(Error::InvalidConfig(
                "initial file length exceeds its mapping",
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        file.set_len(file_length)?;
        if reserve_all {
            sys::reserve(file.as_raw_fd(), 0, file_length)?;
        }
        Self::map(file, mapping_length, file_length, populate)
    }

    pub(crate) fn open(path: &Path, expected_length: u64, populate: bool) -> Result<Self> {
        validate_mapping_length(expected_length)?;
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        if file.metadata()?.len() != expected_length {
            return Err(Error::Corrupt("mapped file has an unexpected length"));
        }
        Self::map(file, expected_length, expected_length, populate)
    }

    pub(crate) fn open_growable(path: &Path, maximum_length: u64, populate: bool) -> Result<Self> {
        validate_mapping_length(maximum_length)?;
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let file_length = file.metadata()?.len();
        validate_mapping_length(file_length)?;
        if file_length > maximum_length {
            return Err(Error::Corrupt(
                "growable file exceeds its configured mapping",
            ));
        }
        Self::map(file, maximum_length, file_length, populate)
    }

    fn map(file: File, mapping_length: u64, file_length: u64, populate: bool) -> Result<Self> {
        let length = usize::try_from(mapping_length)
            .map_err(|_| Error::InvalidConfig("mapping does not fit the address space"))?;
        let pointer = sys::map_shared(file.as_raw_fd(), length, populate)?;
        Ok(Self {
            file,
            pointer,
            mapping_length: length,
            file_length: AtomicU64::new(file_length),
        })
    }

    pub(crate) fn advise_random(&self) -> Result<()> {
        sys::advise_random(self.pointer, self.mapping_length, self.file.as_raw_fd())
    }

    pub(crate) fn advise_heads(&self) -> Result<()> {
        sys::advise_heads(self.pointer, self.mapping_length)
    }

    pub(crate) fn lock_pages(&self, offset: u64, length: u64) -> Result<()> {
        self.checked_range(offset, length)?;
        let offset = usize::try_from(offset).map_err(|_| Error::Corrupt("lock offset overflow"))?;
        let length = usize::try_from(length).map_err(|_| Error::Corrupt("lock length overflow"))?;
        // SAFETY: checked_range proves the offset starts inside this stable mapping.
        let pointer = unsafe { NonNull::new_unchecked(self.pointer.as_ptr().add(offset)) };
        sys::lock(pointer, length)
    }

    pub(crate) fn grow(&self, new_length: u64) -> Result<bool> {
        validate_mapping_length(new_length)?;
        let maximum = u64::try_from(self.mapping_length)
            .map_err(|_| Error::Corrupt("mapping length cannot be represented"))?;
        if new_length > maximum {
            return Err(Error::CapacityExhausted("mapped file"));
        }

        loop {
            let old = self.file_length.load(Ordering::Acquire);
            if old & GROWING_BIT != 0 {
                std::hint::spin_loop();
                continue;
            }
            if old >= new_length {
                return Ok(false);
            }
            let growing = old | GROWING_BIT;
            if self
                .file_length
                .compare_exchange(old, growing, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }

            let result = self.file.set_len(new_length);
            let published = if result.is_ok() { new_length } else { old };
            self.file_length
                .compare_exchange(growing, published, Ordering::Release, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("mapped file growth state changed"))?;
            result?;
            return Ok(true);
        }
    }

    pub(crate) fn file_length(&self) -> u64 {
        loop {
            let length = self.file_length.load(Ordering::Acquire);
            if length & GROWING_BIT == 0 {
                return length;
            }
            std::hint::spin_loop();
        }
    }

    pub(crate) fn reserve(&self, offset: u64, length: u64) -> Result<()> {
        self.checked_range(offset, length)?;
        sys::reserve(self.file.as_raw_fd(), offset, length)
    }

    pub(crate) fn sync_all(&self) -> Result<()> {
        let length = usize::try_from(self.file_length())
            .map_err(|_| Error::Corrupt("file length does not fit memory"))?;
        sys::sync(self.pointer, length)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub(crate) fn atomic_u64(&self, offset: u64) -> Result<&AtomicU64> {
        self.checked_range(offset, size_of::<u64>() as u64)?;
        let offset =
            usize::try_from(offset).map_err(|_| Error::Corrupt("atomic offset overflow"))?;
        // SAFETY: the checked offset is in this stable mapping. Alignment is checked below.
        let pointer = unsafe { self.pointer.as_ptr().add(offset) };
        if pointer as usize % align_of::<AtomicU64>() != 0 {
            return Err(Error::Corrupt("atomic offset is not aligned"));
        }
        // SAFETY: mapped files are zero-initialized before use, AtomicU64 accepts every bit
        // pattern, and all shared accesses to this location use AtomicU64.
        #[allow(clippy::cast_ptr_alignment)]
        Ok(unsafe { &*pointer.cast::<AtomicU64>() })
    }

    pub(crate) fn copy_out(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        let length_u64 =
            u64::try_from(length).map_err(|_| Error::Corrupt("read length overflow"))?;
        self.checked_range(offset, length_u64)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(length)
            .map_err(|_| Error::OutOfMemory)?;
        output.resize(length, 0);
        let offset = usize::try_from(offset).map_err(|_| Error::Corrupt("read offset overflow"))?;
        // SAFETY: both ranges are valid for length bytes and cannot overlap because output is a
        // separately allocated vector.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.pointer.as_ptr().add(offset),
                output.as_mut_ptr(),
                length,
            );
        }
        Ok(output)
    }

    /// Copies bytes into a range exclusively owned by an unpublished allocation.
    ///
    /// # Safety
    ///
    /// No other thread may read or write the destination until publication completes.
    pub(crate) unsafe fn copy_in(&self, offset: u64, input: &[u8]) -> Result<()> {
        let length =
            u64::try_from(input.len()).map_err(|_| Error::Corrupt("write length overflow"))?;
        self.checked_range(offset, length)?;
        let offset =
            usize::try_from(offset).map_err(|_| Error::Corrupt("write offset overflow"))?;
        // SAFETY: upheld by the caller; checked_range guarantees the destination is mapped.
        unsafe {
            std::ptr::copy_nonoverlapping(
                input.as_ptr(),
                self.pointer.as_ptr().add(offset),
                input.len(),
            );
        }
        Ok(())
    }

    fn checked_range(&self, offset: u64, length: u64) -> Result<()> {
        let end = offset
            .checked_add(length)
            .ok_or(Error::Corrupt("mapped range overflow"))?;
        if end > self.file_length() {
            return Err(Error::Corrupt("mapped range is beyond the file length"));
        }
        Ok(())
    }
}

impl Drop for MappedFile {
    fn drop(&mut self) {
        let _result = sys::unmap(self.pointer, self.mapping_length);
    }
}

fn validate_mapping_length(length: u64) -> Result<()> {
    if length == 0 || length % PAGE_SIZE != 0 || length & GROWING_BIT != 0 {
        return Err(Error::InvalidConfig(
            "mapping length must be page aligned, representable, and nonzero",
        ));
    }
    if sys::page_size()? != PAGE_SIZE {
        return Err(Error::Unsupported(
            "only 4096-byte Linux pages are supported",
        ));
    }
    Ok(())
}

#[cfg(all(test, not(miri)))]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("floresta-db-{}-{name}.data", std::process::id()))
    }

    #[test]
    fn maps_atomics_and_bytes() -> Result<()> {
        let path = test_path("mapping");
        let _ignored = std::fs::remove_file(&path);
        {
            let mapping = MappedFile::create(&path, PAGE_SIZE * 2, false, true)?;
            mapping.lock_pages(0, PAGE_SIZE * 2)?;
            assert!(mapping.lock_pages(PAGE_SIZE * 2, PAGE_SIZE).is_err());
            let atomic = mapping.atomic_u64(0)?;
            atomic
                .compare_exchange(0, 42, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("test CAS failed"))?;
            // SAFETY: this range has no concurrent readers or writers in the test.
            unsafe { mapping.copy_in(PAGE_SIZE, b"mapped")? };
            assert_eq!(mapping.copy_out(PAGE_SIZE, 6)?, b"mapped");
            mapping.sync_all()?;
        }
        let reopened = MappedFile::open(&path, PAGE_SIZE * 2, false)?;
        assert_eq!(reopened.atomic_u64(0)?.load(Ordering::Acquire), 42);
        drop(reopened);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn grows_backing_file_inside_stable_mapping() -> Result<()> {
        let path = test_path("growth");
        let _ignored = std::fs::remove_file(&path);
        {
            let mapping = MappedFile::create_growable(&path, PAGE_SIZE * 3, PAGE_SIZE, false)?;
            assert_eq!(std::fs::metadata(&path)?.len(), PAGE_SIZE);
            assert!(mapping.copy_out(PAGE_SIZE, 1).is_err());
            assert!(mapping.grow(PAGE_SIZE * 2)?);
            assert!(!mapping.grow(PAGE_SIZE * 2)?);
            // SAFETY: the grown range has no concurrent readers or writers in the test.
            unsafe { mapping.copy_in(PAGE_SIZE, b"grown")? };
            mapping.sync_all()?;
            assert_eq!(std::fs::metadata(&path)?.len(), PAGE_SIZE * 2);
        }
        let reopened = MappedFile::open_growable(&path, PAGE_SIZE * 3, false)?;
        assert_eq!(reopened.copy_out(PAGE_SIZE, 5)?, b"grown");
        drop(reopened);
        std::fs::remove_file(path)?;
        Ok(())
    }
}

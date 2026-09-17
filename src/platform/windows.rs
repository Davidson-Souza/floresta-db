// SPDX-License-Identifier: MIT OR Apache-2.0

//! Windows storage-backend boundary.
//!
//! Windows file-mapping objects extend a file to the mapping's maximum size.
//! That conflicts with this engine's invariant that one stable maximum mapping
//! sits above a file whose logical and physically reserved tail grows one block
//! at a time. Database storage therefore remains unavailable on Windows until a
//! backend can preserve both invariants without changing the on-disk layout.

use std::fs::File;
use std::ptr::NonNull;

use crate::error::{Error, Result};

const UNSUPPORTED: &str =
    "Windows cannot preserve stable file mappings with block-by-block file growth";

pub(crate) struct Mapping {
    pointer: NonNull<u8>,
}

#[allow(clippy::unused_self)]
impl Mapping {
    pub(crate) fn new(
        _file: &File,
        _length: usize,
        _file_length: usize,
        _populate: bool,
    ) -> Result<Self> {
        Err(Error::Unsupported(UNSUPPORTED))
    }

    pub(crate) fn pointer(&self) -> NonNull<u8> {
        self.pointer
    }

    pub(crate) fn advise_random(&self, _file: &File) -> Result<()> {
        Err(Error::Unsupported(UNSUPPORTED))
    }

    pub(crate) fn advise_heads(&self) -> Result<()> {
        Err(Error::Unsupported(UNSUPPORTED))
    }

    pub(crate) fn sync(&self, _length: usize) -> Result<()> {
        Err(Error::Unsupported(UNSUPPORTED))
    }

    pub(crate) fn grow(&self, _file: &File, _old: usize, _new: usize) -> Result<()> {
        Err(Error::Unsupported(UNSUPPORTED))
    }
}

pub(crate) fn ensure_supported() -> Result<()> {
    Err(Error::Unsupported(UNSUPPORTED))
}

pub(crate) fn reserve(_file: &File, _offset: u64, _length: u64) -> Result<()> {
    Err(Error::Unsupported(UNSUPPORTED))
}

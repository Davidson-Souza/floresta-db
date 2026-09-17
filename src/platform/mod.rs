// SPDX-License-Identifier: MIT OR Apache-2.0

//! Operating-system storage backends.
//!
//! Each backend owns its mapping resources and translates the host's file
//! allocation, mapping, advice, and writeback APIs into one shared interface.

#[cfg(all(
    target_os = "linux",
    not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64"
    ))
))]
compile_error!("Linux support is limited to x86-64, AArch64, and RISC-V 64");
#[cfg(all(
    target_os = "macos",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
compile_error!("macOS support is limited to x86-64 and AArch64");
#[cfg(all(
    target_os = "windows",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
compile_error!("Windows builds are limited to x86-64 and AArch64");

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::{Mapping, ensure_supported, reserve};

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub(crate) use macos::{Mapping, ensure_supported, reserve};

#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub(crate) use windows::{Mapping, ensure_supported, reserve};

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
compile_error!("floresta-db has no storage backend for this operating system");

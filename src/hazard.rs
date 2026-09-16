// SPDX-License-Identifier: MIT OR Apache-2.0

//! Hazard pointers and deferred storage reclamation.
//!
//! Readers publish offsets before dereferencing mapped nodes. Writers reserve
//! retirement slots and reclaim an offset only after no hazard protects it.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

const OWNER_FREE: u64 = 0;
const OWNER_BUSY: u64 = 1;
const RETIRE_FREE: u64 = 0;
const RETIRE_RESERVED: u64 = 1;
const RETIRE_PUBLISHED: u64 = 2;
const RETIRE_CLAIMED: u64 = 3;

struct HazardSlot {
    owner: AtomicU64,
    hazards: [AtomicU64; 2],
}

pub(crate) struct HazardRegistry {
    slots: Vec<HazardSlot>,
}

pub(crate) struct HazardGuard<'registry> {
    registry: &'registry HazardRegistry,
    index: usize,
}

struct RetiredSlot {
    state: AtomicU64,
    offset: AtomicU64,
}

pub(crate) struct RetireQueue {
    slots: Vec<RetiredSlot>,
}

pub(crate) struct RetireReservation<'queue> {
    queue: &'queue RetireQueue,
    index: usize,
    offset: u64,
    active: bool,
}

impl HazardRegistry {
    pub(crate) fn new(max_threads: u16) -> Result<Self> {
        let capacity = usize::from(max_threads);
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| Error::OutOfMemory)?;
        for _index in 0..capacity {
            slots.push(HazardSlot {
                owner: AtomicU64::new(OWNER_FREE),
                hazards: [AtomicU64::new(0), AtomicU64::new(0)],
            });
        }
        Ok(Self { slots })
    }

    pub(crate) fn acquire(&self) -> Result<HazardGuard<'_>> {
        for (index, slot) in self.slots.iter().enumerate() {
            if slot
                .owner
                .compare_exchange(OWNER_FREE, OWNER_BUSY, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Ok(HazardGuard {
                    registry: self,
                    index,
                });
            }
        }
        Err(Error::Busy("all worker hazard slots are occupied"))
    }

    pub(crate) fn contains(&self, offset: u64) -> bool {
        self.slots.iter().any(|slot| {
            slot.hazards
                .iter()
                .any(|hazard| hazard.load(Ordering::SeqCst) == offset)
        })
    }
}

impl HazardGuard<'_> {
    pub(crate) fn protect(&self, hazard_index: usize, offset: u64) -> Result<()> {
        let hazard = self
            .slot()?
            .hazards
            .get(hazard_index)
            .ok_or(Error::Corrupt("hazard index is out of range"))?;
        cas_replace(hazard, offset);
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<()> {
        let slot = self.slot()?;
        for hazard in &slot.hazards {
            cas_replace(hazard, 0);
        }
        Ok(())
    }

    fn slot(&self) -> Result<&HazardSlot> {
        self.registry
            .slots
            .get(self.index)
            .ok_or(Error::Corrupt("hazard owner index is out of range"))
    }
}

impl Drop for HazardGuard<'_> {
    fn drop(&mut self) {
        let Some(slot) = self.registry.slots.get(self.index) else {
            return;
        };
        for hazard in &slot.hazards {
            cas_replace(hazard, 0);
        }
        let _released =
            slot.owner
                .compare_exchange(OWNER_BUSY, OWNER_FREE, Ordering::SeqCst, Ordering::SeqCst);
    }
}

impl RetireQueue {
    pub(crate) fn new(max_threads: u16) -> Result<Self> {
        let capacity = usize::from(max_threads)
            .checked_mul(4)
            .and_then(|value| value.checked_add(8))
            .ok_or(Error::InvalidConfig("retirement queue size overflow"))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| Error::OutOfMemory)?;
        for _index in 0..capacity {
            slots.push(RetiredSlot {
                state: AtomicU64::new(RETIRE_FREE),
                offset: AtomicU64::new(0),
            });
        }
        Ok(Self { slots })
    }

    pub(crate) fn reserve(&self, offset: u64) -> Result<RetireReservation<'_>> {
        if offset == 0 {
            return Err(Error::Corrupt("cannot retire a null offset"));
        }
        for (index, slot) in self.slots.iter().enumerate() {
            if slot
                .state
                .compare_exchange(
                    RETIRE_FREE,
                    RETIRE_RESERVED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            if slot
                .offset
                .compare_exchange(0, offset, Ordering::Release, Ordering::Acquire)
                .is_err()
            {
                let _released = slot.state.compare_exchange(
                    RETIRE_RESERVED,
                    RETIRE_FREE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return Err(Error::Corrupt("retirement slot was not empty"));
            }
            return Ok(RetireReservation {
                queue: self,
                index,
                offset,
                active: true,
            });
        }
        Err(Error::Busy("retirement queue is full"))
    }

    pub(crate) fn reclaim<F>(&self, hazards: &HazardRegistry, mut release: F) -> Result<usize>
    where
        F: FnMut(u64) -> Result<()>,
    {
        let mut reclaimed = 0_usize;
        let mut first_error = None;
        for slot in &self.slots {
            if slot
                .state
                .compare_exchange(
                    RETIRE_PUBLISHED,
                    RETIRE_CLAIMED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                continue;
            }
            let offset = slot.offset.load(Ordering::Acquire);
            if hazards.contains(offset) {
                slot.state
                    .compare_exchange(
                        RETIRE_CLAIMED,
                        RETIRE_PUBLISHED,
                        Ordering::Release,
                        Ordering::Acquire,
                    )
                    .map_err(|_| Error::Corrupt("hazardous retirement state changed"))?;
                continue;
            }
            if let Err(error) = release(offset) {
                slot.state
                    .compare_exchange(
                        RETIRE_CLAIMED,
                        RETIRE_PUBLISHED,
                        Ordering::Release,
                        Ordering::Acquire,
                    )
                    .map_err(|_| Error::Corrupt("failed retirement state changed"))?;
                if first_error.is_none() {
                    first_error = Some(error);
                }
                continue;
            }
            slot.offset
                .compare_exchange(offset, 0, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| Error::Corrupt("retired offset changed while claimed"))?;
            slot.state
                .compare_exchange(
                    RETIRE_CLAIMED,
                    RETIRE_FREE,
                    Ordering::Release,
                    Ordering::Acquire,
                )
                .map_err(|_| Error::Corrupt("claimed retirement state changed"))?;
            reclaimed = reclaimed.saturating_add(1);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(reclaimed),
        }
    }
}

impl RetireReservation<'_> {
    pub(crate) fn commit(mut self) -> Result<()> {
        let slot = self
            .queue
            .slots
            .get(self.index)
            .ok_or(Error::Corrupt("retirement reservation index is invalid"))?;
        slot.state
            .compare_exchange(
                RETIRE_RESERVED,
                RETIRE_PUBLISHED,
                Ordering::Release,
                Ordering::Acquire,
            )
            .map_err(|_| Error::Corrupt("retirement publication state changed"))?;
        self.active = false;
        Ok(())
    }
}

impl Drop for RetireReservation<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let Some(slot) = self.queue.slots.get(self.index) else {
            return;
        };
        if slot
            .offset
            .compare_exchange(self.offset, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _released = slot.state.compare_exchange(
                RETIRE_RESERVED,
                RETIRE_FREE,
                Ordering::Release,
                Ordering::Acquire,
            );
        }
    }
}

fn cas_replace(atomic: &AtomicU64, replacement: u64) {
    let mut observed = atomic.load(Ordering::SeqCst);
    loop {
        match atomic.compare_exchange(observed, replacement, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return,
            Err(actual) => observed = actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protects_offsets_until_a_guard_clears() -> Result<()> {
        let registry = HazardRegistry::new(1)?;
        let guard = registry.acquire()?;
        guard.protect(0, 42)?;
        assert!(registry.contains(42));
        guard.clear()?;
        assert!(!registry.contains(42));
        Ok(())
    }

    #[test]
    fn delays_and_then_reclaims_published_offsets() -> Result<()> {
        let registry = HazardRegistry::new(1)?;
        let retired = RetireQueue::new(1)?;
        let guard = registry.acquire()?;
        guard.protect(1, 9)?;
        retired.reserve(9)?.commit()?;
        assert_eq!(retired.reclaim(&registry, |_offset| Ok(()))?, 0);
        guard.clear()?;
        let mut released = 0;
        assert_eq!(
            retired.reclaim(&registry, |offset| {
                released = offset;
                Ok(())
            })?,
            1
        );
        assert_eq!(released, 9);
        Ok(())
    }

    #[test]
    fn failed_release_remains_queued_for_retry() -> Result<()> {
        let registry = HazardRegistry::new(1)?;
        let retired = RetireQueue::new(1)?;
        retired.reserve(11)?.commit()?;
        assert!(matches!(
            retired.reclaim(&registry, |_offset| Err(Error::Corrupt(
                "injected release failure"
            ))),
            Err(Error::Corrupt(_))
        ));
        let mut released = 0;
        assert_eq!(
            retired.reclaim(&registry, |offset| {
                released = offset;
                Ok(())
            })?,
            1
        );
        assert_eq!(released, 11);
        Ok(())
    }

    #[test]
    fn dropped_reservation_returns_its_slot_without_publication() -> Result<()> {
        let registry = HazardRegistry::new(1)?;
        let retired = RetireQueue::new(1)?;
        drop(retired.reserve(13)?);
        assert_eq!(retired.reclaim(&registry, |_offset| Ok(()))?, 0);
        retired.reserve(14)?.commit()?;
        assert_eq!(retired.reclaim(&registry, |_offset| Ok(()))?, 1);
        Ok(())
    }
}

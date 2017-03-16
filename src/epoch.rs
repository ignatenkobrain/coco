//! The global epoch
//!
//! The global `STATE` number holds two pieces of data: the current global epoch and whether
//! garbage needs to be urgently collected. Every so often the global epoch is incremented - we say
//! it "advances". It can advance only if all currently pinned threads have been pinned in the
//! current epoch.
//!
//! If an object became unreachable in some epoch, we can be sure that no thread will hold a
//! reference to it after two epoch advancements - that is the moment when it will be safe to free
//! it's memory.

use std::cmp;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{self, AtomicUsize, ATOMIC_USIZE_INIT};

/// The global state (epoch and urgency).
///
/// The last bit in this number indicates that garbage must be urgently collected, and the rest of
/// the bits encode the current global epoch, which is always an even number.
///
/// More precisely:
///
/// * Urgency: `state & 1 == 1`
/// * Epoch: `state & !1`
///
/// The global epoch is advanced by increasing the state by 2, and wrapping it around on overflow.
/// A pinned thread may advance the epoch only if all pinned threads have been pinned with the
/// current epoch.
static STATE: AtomicUsize = ATOMIC_USIZE_INIT;

/// Returns the current epoch and urgency flag.
#[inline]
pub fn load() -> (usize, bool) {
    let state = STATE.load(SeqCst);
    (state & !1, state & 1 == 1)
}

#[inline]
pub fn load_relaxed() -> usize {
    STATE.load(Relaxed)
}

/// Enables or disables the urgency mode.
#[inline]
pub fn set_urgency(is_urgent: bool) {
    match is_urgent {
        true => STATE.fetch_or(1, SeqCst),
        false => STATE.fetch_and(!1, SeqCst),
    };
}

/// Advances the global epoch if the current one is `epoch`.
#[inline]
pub fn try_advance(epoch: usize) {
    let state = STATE.load(Relaxed);
    if state & !1 == epoch {
        STATE.compare_and_swap(state, state.wrapping_add(2), SeqCst);
    }
}

/// Returns the distance between two epochs.
///
/// For example, the distance between adjacent epochs is 1.
#[inline]
pub fn distance(epoch1: usize, epoch2: usize) -> usize {
    let diff = epoch1.wrapping_sub(epoch2);
    cmp::min(diff, 0usize.wrapping_sub(diff)) / 2
}

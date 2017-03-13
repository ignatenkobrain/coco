//! Epoch-based memory reclamation
//!
//! TODO: Explain how EBR is used, with examples (maybe a simple treiber stack, with drop impl that
//! pins every X steps)
//!
//! TODO: A treiber stack that uses AtomicPtr instead of Atomic

// TODO: Debug for atomics, ptrs and pin
// TODO: swap method on atomics

mod atomic;
mod epoch;
mod garbage;
mod stash;
mod tagged_atomic;
mod thread;

pub use atomic::{Atomic, Ptr};
pub use stash::Stash;
pub use tagged_atomic::{TaggedAtomic, TaggedPtr};
pub use thread::{Guard, pin, unlinked};

// TODO: unit tests

// TODO: rename mem to memory
// TODO: maybe rename Stash to Garbage
// TODO: module state.rs with global STATE

// TODO: what happens if a guard is stored in another thread-local?

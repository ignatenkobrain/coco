//! Epoch-based memory reclamation
//!
//! TODO: Explain how EBR is used, with examples (maybe a simple treiber stack, with drop impl that
//! pins every X steps)

#[macro_use(defer)]
extern crate scopeguard;

mod atomic;
mod garbage;
mod tagged_atomic;
mod thread;

pub use self::atomic::{Atomic, Ptr};
pub use self::tagged_atomic::{TaggedAtomic, TaggedPtr};
pub use self::thread::{pin, Pin, unlinked};

// TODO: unit tests

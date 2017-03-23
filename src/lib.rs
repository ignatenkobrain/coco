//! Epoch-based memory management
//!
//! TODO: Explain how EBR is used, with examples
//! TODO: Explain pinning
//! TODO: Explain pointers
//! TODO: Explain garbage collection

#[macro_use(defer)]
extern crate scopeguard;

mod atomic;
mod garbage;
mod thread;
mod tagged_atomic;

pub use atomic::{Atomic, Ptr};
pub use garbage::Garbage;
pub use thread::{Pin, pin, defer_free};
pub use tagged_atomic::{TaggedAtomic, TaggedPtr};

// TODO: unit tests

//! Epoch-based memory management.
//!
//! TODO: Explain how EBR is used, with examples
//! TODO: Explain pinning
//! TODO: Explain pointers
//! TODO: Explain garbage collection

mod atomic;
mod garbage;
mod thread;
mod tagged_atomic;

pub use self::atomic::{Atomic, Ptr};
pub use self::garbage::Garbage;
pub use self::thread::{Pin, pin, defer_free};
pub use self::tagged_atomic::{TaggedAtomic, TaggedPtr};

// TODO: unit tests
// TODO: sanitization

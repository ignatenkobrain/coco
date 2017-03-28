//! Concurrent collections
//!
//! TODO: Explain

// TODO: Explain unsafe impls of Send and Sync
// TODO: Can `epoch::defer_free` be independent of `Pin`?

extern crate either;

#[macro_use(defer)]
extern crate scopeguard;

pub mod deque;
pub mod epoch;
pub mod queue;
pub mod stack;

pub use deque::deque;
pub use queue::Queue;
pub use stack::Stack;

//! Concurrent collections
//!
//! TODO: Explain

#[macro_use(defer)]
extern crate scopeguard;

pub mod epoch;
pub mod queue;
pub mod stack;

pub use queue::Queue;
pub use stack::Stack;

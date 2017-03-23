//! Concurrent collections
//!
//! TODO: Explain

#[macro_use(defer)]
extern crate scopeguard;

pub mod epoch;
pub mod queue;

pub use queue::Queue;

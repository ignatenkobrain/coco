//! Concurrent collections
// TODO: Explain

extern crate either;

#[macro_use(defer)]
extern crate scopeguard;

pub mod deque;
pub mod epoch;

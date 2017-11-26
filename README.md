# Concurrent collections (deprecated)

[![Build Status](https://travis-ci.org/stjepang/coco.svg?branch=master)](https://travis-ci.org/stjepang/coco)
[![License](https://img.shields.io/badge/license-Apache--2.0%2FMIT-blue.svg)](https://github.com/stjepang/coco)
[![Cargo](https://img.shields.io/crates/v/coco.svg)](https://crates.io/crates/coco)
[![Documentation](https://docs.rs/coco/badge.svg)](https://docs.rs/coco)

**NOTE:** This crate is now deprecated in favor of Crossbeam. See [crossbeam-epoch](https://github.com/crossbeam-rs/crossbeam-epoch) and [crossbeam-deque](https://github.com/crossbeam-rs/crossbeam-deque).

This crate offers several collections that are designed for performance in multithreaded
contexts. They can be freely shared among multiple threads running in parallel, and concurrently
modified without the overhead of locking.

The following collections are available:

* `Stack`: A lock-free stack.
* `deque`: A lock-free work-stealing deque.

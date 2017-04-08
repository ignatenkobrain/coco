#!/bin/bash

set -ex

cargo test
cargo test --features strict_gc

sanitization_suite() {
    cd sanitize

    # Sanitize once in debug mode (slow).
    ./run.sh thread --bin "$1"
    ./run.sh thread --features coco/strict_gc --bin "$1"

    # Sanitize many times in release mode (fast).
    for i in {1..10}; do ./run.sh thread --release --bin "$1"; done
    for i in {1..10}; do ./run.sh thread --release --bin "$1" --features coco/strict_gc; done
}

if [[ "$TRAVIS_RUST_VERSION" == "nightly" ]]; then
    sanitization_suite stack
fi

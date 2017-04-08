#!/bin/bash

set -ex

cargo test
cargo test --features gc_strict

sanitization_suite() {
    cd sanitize

    # Sanitize once in debug mode (slow).
    ./run.sh thread --bin "$1"
    ./run.sh thread --features coco/gc_strict --bin "$1"

    # Sanitize many times in release mode (fast).
    for i in {1..10}; do ./run.sh thread --release --bin "$1"; done
    for i in {1..10}; do ./run.sh thread --release --bin "$1" --features coco/gc_strict; done
}

if [[ "$TRAVIS_RUST_VERSION" == "nightly" ]]; then
    sanitization_suite stack
fi

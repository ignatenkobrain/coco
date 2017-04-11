#!/bin/bash

set -ex

cargo test
cargo test --features strict_gc

if [[ "$TRAVIS_RUST_VERSION" == "nightly" ]]; then
    cd sanitize

    cargo test
    cargo test --features coco/strict_gc

    for i in {1..10}; do
        cargo test --release
    done
    for i in {1..10}; do
        cargo test --release --features coco/strict_gc
    done
fi

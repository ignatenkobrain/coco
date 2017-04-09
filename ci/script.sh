#!/bin/bash

set -ex

cargo test
cargo test --features strict_gc

if [[ "$TRAVIS_RUST_VERSION" == "nightly" ]]; then
    cd sanitize
    tests=(gc stack)

    for t in $tests; do
        ./run.sh thread --bin "$t"
    done

    for t in $tests; do
        ./run.sh thread --features coco/strict_gc --bin "$t"
    done

    for t in $tests; do
        for i in {1..10}; do
            ./run.sh thread --release --bin "$t"
        done
    done

    for t in $tests; do
        for i in {1..10}; do
            ./run.sh thread --release --bin "$t" --features coco/strict_gc
        done
    done
fi

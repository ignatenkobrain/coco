#!/bin/bash

cd $(readlink -f "$(dirname "$0")")

san="$1"; shift

case "$san" in
	leak)    export RUSTFLAGS="-Z sanitizer=leak"   CARGO_TARGET_DIR="target/lsan";;
	thread)  export RUSTFLAGS="-Z sanitizer=thread" CARGO_TARGET_DIR="target/tsan";;
	*)       echo "Uknown sanitizer: '$san'"; exit 1;;
esac

set -ex
cargo run --target x86_64-unknown-linux-gnu "$@"

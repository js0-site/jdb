#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

if [[ "$(uname -m)" == "x86_64" ]]; then
  export RUSTFLAGS="$RUSTFLAGS -C target-feature=+aes,+sse2"
fi

cargo test --all-features -- --nocapture

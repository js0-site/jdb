#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

if ! command -v cargo-nextest &>/dev/null; then
  cargo binstall -y cargo-nextest
fi

cargo nextest run --all-features --no-capture

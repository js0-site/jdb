#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR

run() {
  cargo bench --features $1 --no-default-features -- --output-format bencher
}

run murmur3
run gxhash

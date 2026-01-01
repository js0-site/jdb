#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR

mkdir -p report

run() {
  cargo bench --features $1 --no-default-features -- --output-format bencher >report/$1.txt
}

run "murmur3,binary-fuse"
run "gxhash,binary-fuse"

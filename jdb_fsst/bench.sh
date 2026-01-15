#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR

cargo bench --features=bench_all -- --nocapture
./benches/benched.js

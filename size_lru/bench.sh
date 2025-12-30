#!/bin/bash
set -e

./test.sh
cargo bench --bench comparison --features all
./table.js
./svg.js

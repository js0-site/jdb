#!/bin/bash
set -e

./test.sh
cargo bench --bench comparison
cargo bench --bench table
./table.js
./svg.js

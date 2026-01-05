#!/bin/bash
set -e

cargo bench --bench pgm_vs_binary --features jemalloc

bun table.js
bun svg.js

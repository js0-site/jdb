#!/bin/bash
set -e

echo "=== Running regression tests ==="
cargo test --test regression

echo ""
echo "=== Running comparison benchmarks ==="
cargo bench --bench comparison

echo ""
echo "=== Generating table ==="
cargo bench --bench table

echo ""
echo "=== Generating SVG charts ==="
cargo bench --bench gen_svg

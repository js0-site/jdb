#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
# set -x

# Clean old reports / 清理旧报告
rm -rf report
mkdir -p report

# Get features / 获取 features
FEATURES=$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[] | select(.name == "jdb_bench") | .features | keys[]' | grep -v '^default$' | grep -v '^all$')

# Run bench for each feature / 逐个运行 benchmark
if [ -n "$FEATURES" ]; then
  for feature in $FEATURES; do
    cargo bench --features="$feature" -- --nocapture
  done
fi

# Generate summary report using bench.js / 使用 bench.js 生成汇总报告
bun bench.js

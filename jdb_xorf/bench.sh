#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR

mkdir -p report

echo "=========================================="
echo "开始性能对比测试"
echo "时间: $(date)"
echo "=========================================="

run() {
  echo ""
  echo "运行测试特性组合: $1"
  echo "----------------------------------------"
  cargo bench --features "$1" --no-default-features -- --output-format bencher | tee "report/$1.txt"
  echo "----------------------------------------"
}

# 测试 murmur3 和 gxhash 的性能对比
run "murmur3,binary-fuse"
run "gxhash,binary-fuse"

echo ""
echo "=========================================="
echo "性能测试完成"
echo "报告保存在 report/ 目录"
echo "=========================================="

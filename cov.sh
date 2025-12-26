#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

if ! command -v cargo-llvm-cov &>/dev/null; then
  cargo install cargo-llvm-cov
fi

# 运行测试收集覆盖率数据（不生成报告）
# Run tests and collect coverage data (no report)
cargo llvm-cov --all-features --no-report -- --nocapture

case $OSTYPE in
darwin*)
  cargo llvm-cov report --html --output-dir coverage
  open ./coverage/html/index.html
  ;;
*)
  cargo llvm-cov report
  ;;
esac

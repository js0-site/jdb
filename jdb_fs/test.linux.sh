#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

ARCH=$(uname -m)
case $ARCH in
x86_64)
  TARGET=x86_64-unknown-linux-gnu
  PLATFORM=linux/amd64
  ;;
arm64 | aarch64)
  TARGET=aarch64-unknown-linux-gnu
  PLATFORM=linux/arm64
  ;;
*)
  echo "Unsupported arch: $ARCH"
  exit 1
  ;;
esac

# Install cargo-zigbuild if not exists
# 如果不存在则安装 cargo-zigbuild
if ! cargo zigbuild --help &>/dev/null; then
  cargo install cargo-zigbuild
fi

if ! rustup target list --installed | grep -q $TARGET; then
  rustup target add $TARGET
fi

cargo zigbuild --target $TARGET --all-features --tests

# Find test binary 查找测试二进制
BIN=$(cargo zigbuild --target $TARGET --all-features --tests --message-format=json 2>/dev/null |
  jq -r 'select(.executable != null and .target.kind[] == "test") | .executable' | head -1)

# Run in docker 在 docker 中运行
docker run --rm --privileged --platform $PLATFORM -v "$BIN":/test:ro debian:bookworm-slim /test --nocapture

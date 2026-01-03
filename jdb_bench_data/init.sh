#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

if [ ! -d "data/txt" ]; then
  mkdir -p data
  cd data
  wget --no-check-certificate -c https://huggingface.co/datasets/i18n-site/txt/resolve/main/txt.tar.zstd
  zstd -dc txt.tar.zstd | tar -xf -
  rm txt.tar.zstd
fi

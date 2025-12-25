#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

cargo test --all-features -- --nocapture

if [[ $(uname) != "Linux" ]]; then
  ./test.linux.sh
fi

if [[ $(uname) != *"MINGW"* && $(uname) != *"CYGWIN"* && $(uname) != *"MSYS"* ]]; then
  ./test.windows.sh
fi

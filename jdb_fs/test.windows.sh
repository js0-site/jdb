#!/usr/bin/env bash

set -e
DIR=$(realpath $0) && DIR=${DIR%/*}
cd $DIR
set -x

TARGET=x86_64-pc-windows-msvc

if ! cargo xwin --help &>/dev/null; then
  cargo install cargo-xwin
fi

if ! rustup target list --installed | grep -q $TARGET; then
  rustup target add $TARGET
fi

cargo xwin build --target $TARGET --all-features
cargo xwin build --target $TARGET --all-features --tests

BIN=$(cargo xwin build --target $TARGET --all-features --tests --message-format=json | jq -r 'select(.executable != null and .target.kind[] == "test") | .executable' | head -1)

# if [[ $(uname) == "Darwin" ]]; then
#   if ! command -v wine64 &>/dev/null; then
#     brew install --cask --no-quarantine gcenx/wine/wine-crossover
#   fi
#   export WINEPREFIX=~/.wine64
#   if [[ ! -d "$WINEPREFIX" ]]; then
#     winetricks --force vcrun2022
#   fi
#   wine64 "$BIN" --nocapture
# fi

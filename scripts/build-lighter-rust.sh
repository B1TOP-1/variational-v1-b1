#!/usr/bin/env bash
set -euo pipefail

bybot_dir="${BYBOT_DIR:-$HOME/git/bybot/bybot}"
lighter_dir="$bybot_dir/lighter"

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo was not found. Install Rust with rustup and reopen the shell." >&2
  exit 1
fi

if [[ ! -f "$lighter_dir/Cargo.toml" ]]; then
  echo "bybot/lighter was not found: $lighter_dir" >&2
  echo "Clone git@github.com:B1TOP-1/bybot.git to $bybot_dir first." >&2
  exit 1
fi

echo "Building Lighter Rust gateway from $lighter_dir"
cargo build --manifest-path "$lighter_dir/Cargo.toml" --release --bin variational_lighter_gateway
binary="$lighter_dir/target/release/variational_lighter_gateway"
test -x "$binary"
echo "Lighter Rust gateway ready: $binary"

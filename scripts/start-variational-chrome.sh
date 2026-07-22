#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$script_dir/.." && pwd)"
profile_dir="${VARIATIONAL_CHROME_PROFILE:-$HOME/.config/variational-chrome}"
extension_dir="${VARIATIONAL_CHROME_EXTENSION:-$repo_dir/chrome_extension}"
start_url="${VARIATIONAL_CHROME_URL:-https://omni.variational.io/perpetual/BTC}"

browser=""
for candidate in google-chrome-stable google-chrome chromium chromium-browser; do
  if command -v "$candidate" >/dev/null 2>&1; then
    browser="$(command -v "$candidate")"
    break
  fi
done

if [[ -z "$browser" ]]; then
  echo "Chrome/Chromium was not found in PATH." >&2
  exit 1
fi

if [[ ! -d "$extension_dir" ]]; then
  echo "Variational extension directory not found: $extension_dir" >&2
  exit 1
fi

if pgrep -af -- "--user-data-dir=$profile_dir" >/dev/null 2>&1; then
  echo "The dedicated Variational Chrome is already running: $profile_dir" >&2
  exit 1
fi

mkdir -p "$profile_dir"

flags=(
  "--user-data-dir=$profile_dir"
  "--load-extension=$extension_dir"
  "--disable-extensions-except=$extension_dir"
  "--silent-debugger-extension-api"
  "--process-per-site"
  "--disable-background-networking"
  "--disable-component-update"
  "--disable-default-apps"
  "--disable-sync"
  "--disable-translate"
  "--disable-breakpad"
  "--no-first-run"
  "--no-default-browser-check"
  "--disable-background-timer-throttling"
  "--disable-renderer-backgrounding"
  "--disable-backgrounding-occluded-windows"
)

# Useful on VPS images whose /dev/shm is commonly limited to 64 MB. This uses
# /tmp for shared-memory files; set to 0 when the host has a sufficiently large
# /dev/shm and faster shared memory is preferred.
if [[ "${VARIATIONAL_DISABLE_DEV_SHM:-1}" == "1" ]]; then
  flags+=("--disable-dev-shm-usage")
fi

# GPU shutdown saves a small process, but software rendering can cost more CPU.
# Keep it opt-in because the best choice depends on the VPS display stack.
if [[ "${VARIATIONAL_DISABLE_GPU:-0}" == "1" ]]; then
  flags+=("--disable-gpu")
fi

echo "Starting dedicated Variational Chrome"
echo "  browser:   $browser"
echo "  profile:   $profile_dir"
echo "  extension: $extension_dir"
echo "  url:       $start_url"
echo "Verify flags after startup at chrome://version"

exec "$browser" "${flags[@]}" "$@" "$start_url"

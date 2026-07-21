#!/usr/bin/env bash
set -euo pipefail

if pgrep -f 'google-chrome|chromium' >/dev/null 2>&1; then
  echo "Chrome/Chromium is already running. Close it completely, then run this script again." >&2
  echo "Use: pkill -9 -f 'chrome|chromium'" >&2
  exit 1
fi

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

echo "Starting: $browser --silent-debugger-extension-api"
echo "Verify the active command line at chrome://version"
exec "$browser" --silent-debugger-extension-api "$@"

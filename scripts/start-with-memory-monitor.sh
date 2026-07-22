#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$script_dir/.." && pwd)"
started_at="$(date '+%Y-%m-%d_%H-%M-%S_%z')"
memory_dir="${MEMORY_MONITOR_OUTPUT_DIR:-$repo_dir/log/memory/$started_at}"

python_bin=""
python_candidates=("$repo_dir/venv/bin/python" "$repo_dir/.venv/bin/python" python3)
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  python_candidates=("$VIRTUAL_ENV/bin/python" "${python_candidates[@]}")
fi
for candidate in "${python_candidates[@]}"; do
  if [[ -n "$candidate" ]] && command -v "$candidate" >/dev/null 2>&1; then
    python_bin="$(command -v "$candidate")"
    break
  fi
done

if [[ -z "$python_bin" ]]; then
  echo "Python was not found. Create the project venv first." >&2
  exit 1
fi

mkdir -p "$memory_dir"
MEMORY_MONITOR_OUTPUT_DIR="$memory_dir" \
  "$script_dir/monitor-memory.sh" >"$memory_dir/monitor.log" 2>&1 &
monitor_pid=$!

cleanup() {
  if kill -0 "$monitor_pid" >/dev/null 2>&1; then
    kill "$monitor_pid" >/dev/null 2>&1 || true
    wait "$monitor_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

echo "Memory monitor is running with main.py (pid=$monitor_pid)"
echo "Memory logs: $memory_dir"
echo "Chrome is not started by this command; use your existing Chrome normally."

cd "$repo_dir"
set +e
"$python_bin" main.py "$@"
exit_code=$?
set -e
exit "$exit_code"

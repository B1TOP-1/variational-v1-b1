#!/usr/bin/env bash
set -euo pipefail

interval_seconds="${MEMORY_MONITOR_INTERVAL_SECONDS:-10}"
available_alert_mb="${MEMORY_MONITOR_AVAILABLE_ALERT_MB:-250}"
process_alert_mb="${MEMORY_MONITOR_PROCESS_ALERT_MB:-500}"
snapshot_cooldown_seconds="${MEMORY_MONITOR_SNAPSHOT_COOLDOWN_SECONDS:-300}"
top_process_count="${MEMORY_MONITOR_TOP_PROCESS_COUNT:-40}"
repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
started_at="$(date '+%Y-%m-%d_%H-%M-%S_%z')"
output_dir="${MEMORY_MONITOR_OUTPUT_DIR:-$repo_dir/log/memory/$started_at}"

if [[ ! -r /proc/meminfo ]]; then
  echo "This monitor requires Linux /proc and is intended for the VPS." >&2
  exit 1
fi

for value_name in interval_seconds available_alert_mb process_alert_mb snapshot_cooldown_seconds top_process_count; do
  value="${!value_name}"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
    echo "Invalid $value_name: $value" >&2
    exit 1
  fi
done

mkdir -p "$output_dir/snapshots"
summary_file="$output_dir/system.csv"
process_file="$output_dir/processes.csv"
alert_file="$output_dir/alerts.log"

printf '%s\n' 'timestamp,epoch,total_mb,available_mb,used_mb,swap_total_mb,swap_free_mb,swap_used_mb,load_1m' >"$summary_file"
printf '%s\n' 'timestamp,epoch,rank,pid,ppid,rss_mb,cpu_percent,elapsed_seconds,role,command' >"$process_file"

classify_process() {
  local command="$1"
  case "$command" in
    *chrome-extension://*|*--extension-process*) printf '%s' 'chrome-extension' ;;
    *--type=renderer*) printf '%s' 'chrome-renderer' ;;
    *--type=gpu-process*) printf '%s' 'chrome-gpu' ;;
    *--type=utility*network*) printf '%s' 'chrome-network' ;;
    *--type=utility*storage*) printf '%s' 'chrome-storage' ;;
    *--type=utility*) printf '%s' 'chrome-utility' ;;
    *google-chrome*|*chromium*) printf '%s' 'chrome-browser' ;;
    *python*main.py*) printf '%s' 'variational-main' ;;
    *variational_lighter_gateway*) printf '%s' 'lighter-rust' ;;
    *astro-core*) printf '%s' 'astro-core' ;;
    *sub2api*) printf '%s' 'sub2api' ;;
    *) printf '%s' 'other' ;;
  esac
}

csv_escape() {
  local value="${1//$'\n'/ }"
  value="${value//\"/\"\"}"
  printf '"%s"' "$value"
}

take_snapshot() {
  local timestamp="$1"
  local reason="$2"
  local snapshot="$output_dir/snapshots/${timestamp//[: +]/_}.txt"
  {
    echo "reason=$reason"
    echo "timestamp=$timestamp"
    echo
    free -h || true
    echo
    ps -eo user,pid,ppid,rss,%mem,%cpu,etimes,stat,args --sort=-rss || true
  } >"$snapshot"
  printf '%s | %s | %s\n' "$timestamp" "$reason" "$snapshot" >>"$alert_file"
}

echo "Memory monitor started"
echo "  interval:          ${interval_seconds}s"
echo "  available alert:   ${available_alert_mb} MB"
echo "  per-process alert: ${process_alert_mb} MB"
echo "  output:            $output_dir"
echo "Stop with Ctrl+C. Sampling is appended directly to disk."

last_snapshot_epoch=0
trap 'echo; echo "Memory monitor stopped. Logs: $output_dir"; exit 0' INT TERM

while true; do
  timestamp="$(date '+%Y-%m-%d %H:%M:%S %z')"
  epoch="$(date '+%s')"
  total_kb="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  available_kb="$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)"
  swap_total_kb="$(awk '/^SwapTotal:/ {print $2}' /proc/meminfo)"
  swap_free_kb="$(awk '/^SwapFree:/ {print $2}' /proc/meminfo)"
  total_mb=$((total_kb / 1024))
  available_mb=$((available_kb / 1024))
  used_mb=$(((total_kb - available_kb) / 1024))
  swap_total_mb=$((swap_total_kb / 1024))
  swap_free_mb=$((swap_free_kb / 1024))
  swap_used_mb=$(((swap_total_kb - swap_free_kb) / 1024))
  load_1m="$(awk '{print $1}' /proc/loadavg)"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$timestamp" "$epoch" "$total_mb" "$available_mb" "$used_mb" \
    "$swap_total_mb" "$swap_free_mb" "$swap_used_mb" "$load_1m" >>"$summary_file"

  rank=0
  largest_rss_mb=0
  largest_pid=""
  largest_role=""
  while read -r pid ppid rss cpu elapsed command; do
    [[ -n "${pid:-}" ]] || continue
    rank=$((rank + 1))
    rss_mb=$(((rss + 1023) / 1024))
    role="$(classify_process "$command")"
    (( rss_mb > largest_rss_mb )) && {
      largest_rss_mb="$rss_mb"
      largest_pid="$pid"
      largest_role="$role"
    }
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$timestamp" "$epoch" "$rank" "$pid" "$ppid" "$rss_mb" "$cpu" "$elapsed" \
      "$role" "$(csv_escape "$command")" >>"$process_file"
  done < <(ps -eo pid=,ppid=,rss=,%cpu=,etimes=,args= --sort=-rss | head -n "$top_process_count")

  alert_reason=""
  if (( available_mb < available_alert_mb )); then
    alert_reason="available memory ${available_mb}MB is below ${available_alert_mb}MB"
  elif (( largest_rss_mb > process_alert_mb )); then
    alert_reason="process pid=${largest_pid} role=${largest_role} rss=${largest_rss_mb}MB exceeds ${process_alert_mb}MB"
  fi

  if [[ -n "$alert_reason" ]] && (( epoch - last_snapshot_epoch >= snapshot_cooldown_seconds )); then
    take_snapshot "$timestamp" "$alert_reason"
    last_snapshot_epoch="$epoch"
    echo "$timestamp ALERT: $alert_reason"
  fi

  sleep "$interval_seconds"
done

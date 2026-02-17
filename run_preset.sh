#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="${PYTHON_BIN_FALLBACK:-python3}"
fi

usage() {
  cat <<'EOF'
Usage:
  ./run_preset.sh <safe|medium|aggressive> [paper|live] [extra args...]

Examples:
  ./run_preset.sh safe
  ./run_preset.sh medium live
  AUTO_APPROVE=1 ./run_preset.sh medium live
  SIZE_USD=5 COIN=BTC TIMEFRAME=15m ./run_preset.sh aggressive live
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

PRESET="$1"
shift

MODE="paper"
if [[ $# -gt 0 ]]; then
  case "$1" in
    paper|live)
      MODE="$1"
      shift
      ;;
  esac
fi

COIN="$(echo "${COIN:-BTC}" | tr '[:lower:]' '[:upper:]')"
TIMEFRAME="${TIMEFRAME:-}"
if [[ -z "$TIMEFRAME" ]]; then
  TIMEFRAME="15m"
fi

SIZE_USD="${SIZE_USD:-5}"
CONTROL_FILE="${CONTROL_FILE:-/tmp/pm_traderctl}"
EXEC_LOG_FILE="${EXEC_LOG_FILE:-$ROOT_DIR/executions.log.jsonl}"
ENTRY_FILL_TIMEOUT_SEC="${ENTRY_FILL_TIMEOUT_SEC:-25}"
ENTRY_FILL_POLL_SEC="${ENTRY_FILL_POLL_SEC:-1.0}"

COMMON_ARGS=(
  --coin "$COIN"
  --timeframe "$TIMEFRAME"
  --size-usd "$SIZE_USD"
  --control-file "$CONTROL_FILE"
  --executions-log-file "$EXEC_LOG_FILE"
  --entry-fill-timeout-sec "$ENTRY_FILL_TIMEOUT_SEC"
  --entry-fill-poll-sec "$ENTRY_FILL_POLL_SEC"
)

case "$PRESET" in
  safe)
    PRESET_ARGS=(
      --min-bias 60
      --min-obi 0.45
      --min-price 0.42
      --max-price 0.62
      --cooldown-sec 900
      --max-trades-per-day 2
      --eval-interval-sec 5
      --tp-pct 9
      --sl-pct 5
      --max-hold-sec 1800
      --reverse-exit-bias 60
    )
    ;;
  medium)
    PRESET_ARGS=(
      --min-bias 55
      --min-obi 0.40
      --min-price 0.40
      --max-price 0.68
      --cooldown-sec 420
      --max-trades-per-day 4
      --eval-interval-sec 3
      --tp-pct 10
      --sl-pct 6
      --max-hold-sec 1200
      --reverse-exit-bias 55
    )
    ;;
  aggressive)
    PRESET_ARGS=(
      --min-bias 45
      --min-obi 0.30
      --min-price 0.35
      --max-price 0.80
      --cooldown-sec 180
      --max-trades-per-day 8
      --eval-interval-sec 2
      --tp-pct 8
      --sl-pct 7
      --max-hold-sec 900
      --reverse-exit-bias 45
    )
    ;;
  *)
    echo "Unknown preset: $PRESET"
    usage
    exit 1
    ;;
esac

MODE_ARGS=()
if [[ "$MODE" == "paper" ]]; then
  MODE_ARGS+=(--paper)
else
  LIVE_CONFIRM_TOKEN="${LIVE_CONFIRM_TOKEN:-I_UNDERSTAND_REAL_MONEY_RISK}"
  if [[ "${PM_ENABLE_LIVE:-}" != "1" ]]; then
    echo "PM_ENABLE_LIVE=1 is required for live mode."
    exit 1
  fi
  if [[ -z "${PM_PRIVATE_KEY:-}" || -z "${PM_FUNDER:-}" ]]; then
    echo "PM_PRIVATE_KEY and PM_FUNDER are required for live mode."
    exit 1
  fi
  MODE_ARGS+=(--live --confirm-live-token "$LIVE_CONFIRM_TOKEN")
fi

if [[ "${AUTO_APPROVE:-0}" == "1" ]]; then
  MODE_ARGS+=(--auto-approve)
fi

echo "[preset] preset=$PRESET mode=$MODE coin=$COIN timeframe=$TIMEFRAME size_usd=$SIZE_USD auto_approve=${AUTO_APPROVE:-0} exec_log=$EXEC_LOG_FILE"
exec "$PYTHON_BIN" "$ROOT_DIR/main.py" \
  "${MODE_ARGS[@]}" \
  "${COMMON_ARGS[@]}" \
  "${PRESET_ARGS[@]}" \
  "$@"

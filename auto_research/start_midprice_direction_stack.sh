#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BTC_SCRIPT="$ROOT_DIR/auto_research/start_direction_midprice_btc.sh"
ETH_SCRIPT="$ROOT_DIR/auto_research/start_direction_midprice_eth.sh"

ACTION="${1:-start}"

case "$ACTION" in
  start)
    "$BTC_SCRIPT" start
    "$ETH_SCRIPT" start
    ;;
  restart)
    "$0" stop || true
    sleep 1
    "$0" start
    ;;
  stop)
    "$BTC_SCRIPT" stop || true
    "$ETH_SCRIPT" stop || true
    ;;
  status)
    echo "===BTC MIDPRICE DIRECTION==="
    AUTORUN_DIR="$ROOT_DIR/var/research/autorun/midprice_direction_btc" "$ROOT_DIR/auto_research/status_autorun.sh" || true
    echo "===ETH MIDPRICE DIRECTION==="
    AUTORUN_DIR="$ROOT_DIR/var/research/autorun/midprice_direction_eth" "$ROOT_DIR/auto_research/status_autorun.sh" || true
    ;;
  *)
    echo "usage: start_midprice_direction_stack.sh {start|restart|stop|status}" >&2
    exit 2
    ;;
esac

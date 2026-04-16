#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SERVICE_NAME="${PM15MIN_MANAGED_PROXY_SERVICE_NAME:-pm15min-managed-proxy}"
SYSTEMD_USER_DIR="${PM15MIN_SYSTEMD_USER_DIR:-$HOME/.config/systemd/user}"
CONFIG_PATH="${PM15MIN_V2RAYA_CONFIG_PATH:-$HOME/_v2raya-lite/config.json}"
STATE_DIR="${PM15MIN_MANAGED_PROXY_STATE_DIR:-$HOME/.local/state/pm15min-managed-proxy}"
PYTHON_BIN="${PM15MIN_MANAGED_PROXY_PYTHON:-python3}"
TIMEOUT_SEC="${PM15MIN_MANAGED_PROXY_TIMEOUT_SEC:-5.0}"
REFRESH_COMMAND="${PM15MIN_PROXY_REFRESH_COMMAND:-}"
HEALTHCHECK_URLS="${PM15MIN_MANAGED_PROXY_HEALTHCHECK_URLS:-https://api.exchange.coinbase.com/time https://www.google.com/generate_204}"
PROXY_HOST="${PM15MIN_MANAGED_PROXY_HOST:-127.0.0.1}"
PROXY_SCHEME="${PM15MIN_MANAGED_PROXY_SCHEME:-socks5h}"
CANDIDATE_PORTS="${PM15MIN_MANAGED_PROXY_CANDIDATE_PORTS:-}"

mkdir -p "$SYSTEMD_USER_DIR"
mkdir -p "$STATE_DIR"

SERVICE_PATH="$SYSTEMD_USER_DIR/${SERVICE_NAME}.service"
TIMER_PATH="$SYSTEMD_USER_DIR/${SERVICE_NAME}.timer"

healthcheck_args=()
for url in $HEALTHCHECK_URLS; do
  healthcheck_args+=("--healthcheck-url" "$url")
done

refresh_args=()
if [[ -n "$REFRESH_COMMAND" ]]; then
  refresh_args+=("--refresh-command" "$REFRESH_COMMAND")
fi

candidate_args=("--proxy-host" "$PROXY_HOST" "--proxy-scheme" "$PROXY_SCHEME")
if [[ -n "$CANDIDATE_PORTS" ]]; then
  for port in $CANDIDATE_PORTS; do
    candidate_args+=("--candidate-port" "$port")
  done
else
  candidate_args+=("--config-path" "$CONFIG_PATH")
fi

command=(
  "$PYTHON_BIN"
  "$PROJECT_DIR/scripts/maintenance/managed_proxy_failover.py"
  "--state-dir" "$STATE_DIR"
  "--timeout-sec" "$TIMEOUT_SEC"
  "${candidate_args[@]}"
  "${healthcheck_args[@]}"
  "${refresh_args[@]}"
)
printf -v EXEC_START_CMD '%q ' "${command[@]}"

cat > "$SERVICE_PATH" <<EOF
[Unit]
Description=PM15MIN managed proxy failover probe

[Service]
Type=oneshot
WorkingDirectory=$PROJECT_DIR
ExecStart=/bin/bash -lc 'cd $PROJECT_DIR && $EXEC_START_CMD'
EOF

cat > "$TIMER_PATH" <<EOF
[Unit]
Description=Run PM15MIN managed proxy failover probe every 5 minutes

[Timer]
OnBootSec=1min
OnUnitActiveSec=5min
Persistent=true
Unit=${SERVICE_NAME}.service

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now "${SERVICE_NAME}.timer"
systemctl --user start "${SERVICE_NAME}.service"

echo "Installed ${SERVICE_NAME}.service and ${SERVICE_NAME}.timer"
echo "State dir: $STATE_DIR"
echo "Active env file: $STATE_DIR/active_proxy.env"

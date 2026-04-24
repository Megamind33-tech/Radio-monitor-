#!/usr/bin/env bash
# deploy/watchdog.sh
# ─────────────────────────────────────────────────────────────────────────────
# Never-sleep guarantee for the Zambian Airplay Monitor on DigitalOcean.
#
# This script is invoked once a minute by mostify-watchdog.timer. It:
#   1. Pings the local /healthz endpoint of the Node server.
#   2. Confirms a recent DetectionLog row exists (i.e. the polling loop is alive).
#   3. Restarts mostify-app.service if either probe fails three times in a row.
#
# Exit codes:
#   0 — healthy (or restart issued; not an error)
#   1 — restart issued because of consecutive probe failures
#   2 — could not check (curl missing, etc.)
#
# Logs: /opt/mostify/logs/watchdog.log
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

APP_PORT="${APP_PORT:-3000}"
APP_HOST="${APP_HOST:-127.0.0.1}"
SERVICE_NAME="${SERVICE_NAME:-mostify-app.service}"
STATE_FILE="${STATE_FILE:-/var/run/mostify-watchdog.state}"
MAX_FAILS="${WATCHDOG_MAX_FAILS:-3}"
HEALTH_TIMEOUT="${WATCHDOG_HEALTH_TIMEOUT:-10}"

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

log() { echo "[$(ts)] $*"; }

if ! command -v curl >/dev/null 2>&1; then
  log "ERROR: curl is required for the watchdog"
  exit 2
fi

# 1. HTTP healthz probe.
http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    --max-time "$HEALTH_TIMEOUT" \
    "http://${APP_HOST}:${APP_PORT}/healthz" || echo "000")

http_ok=0
case "$http_code" in
  2*|3*) http_ok=1 ;;
esac

# 2. Stale-detection probe (recent DetectionLog row in DB).
poll_ok=0
if [[ -f "/opt/mostify/prisma/prod.db" ]]; then
  if command -v sqlite3 >/dev/null 2>&1; then
    last=$(sqlite3 /opt/mostify/prisma/prod.db \
        "SELECT strftime('%s', MAX(observedAt)) FROM DetectionLog;" 2>/dev/null || echo "0")
    now=$(date -u +%s)
    age=$(( now - ${last:-0} ))
    if (( age < 600 )); then
      poll_ok=1
    fi
  else
    # Without sqlite3, do not block on DB freshness — trust the HTTP probe.
    poll_ok=1
  fi
else
  poll_ok=1
fi

prev=0
if [[ -f "$STATE_FILE" ]]; then
  prev=$(cat "$STATE_FILE" 2>/dev/null || echo 0)
fi

if (( http_ok == 1 && poll_ok == 1 )); then
  echo 0 > "$STATE_FILE"
  log "ok http=$http_code poll_age_s_threshold_lt_600=true streak_reset"
  exit 0
fi

next=$(( prev + 1 ))
echo "$next" > "$STATE_FILE"
log "fail #$next http=$http_code poll_ok=$poll_ok"

if (( next >= MAX_FAILS )); then
  log "RESTART: $SERVICE_NAME after $next consecutive failures"
  systemctl restart "$SERVICE_NAME" >/dev/null 2>&1 || \
      log "WARN: systemctl restart $SERVICE_NAME failed"
  echo 0 > "$STATE_FILE"
  exit 1
fi

exit 0

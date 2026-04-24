#!/usr/bin/env bash
# deploy/update_server.sh
# ─────────────────────────────────────────────────────────────────────────────
# Safe in-place update for a DigitalOcean droplet (paste into the web console
# or SSH). Matches deploy/setup_digitalocean.sh defaults: /opt/mostify, user
# mostify, branch main.
#
# What it does:
#   1. git fetch + pull (preserves .env; never commits)
#   2. npm ci (reproducible from package-lock.json)
#   3. prisma migrate deploy (+ generate); falls back like setup script
#   4. npm run build
#   5. pip install -r requirements.txt (if present)
#   6. restarts mostify-app (+ optional timer units)
#
# Usage (on the droplet, as root):
#   chmod +x deploy/update_server.sh
#   sudo bash deploy/update_server.sh
#
#   # Custom install path / user / branch (same env vars as setup):
#   sudo APP_DIR=/opt/mostify APP_USER=mostify GIT_BRANCH=main bash deploy/update_server.sh
#
#   # Do not restart systemd units (e.g. you will restart manually):
#   sudo bash deploy/update_server.sh --no-restart
#
#   # Also restart timers (ORB poller, drain, watchdog, validators, harvest):
#   sudo bash deploy/update_server.sh --restart-timers
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

APP_DIR="${APP_DIR:-/opt/mostify}"
APP_USER="${APP_USER:-mostify}"
GIT_REMOTE="${GIT_REMOTE:-origin}"
GIT_BRANCH="${GIT_BRANCH:-main}"
RESTART_APP=1
RESTART_TIMERS=0

for arg in "$@"; do
  case "$arg" in
    --no-restart)     RESTART_APP=0 ;;
    --restart-timers) RESTART_TIMERS=1 ;;
    -h|--help)
      sed -n '2,/^# ───/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
  esac
done

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

[[ $EUID -ne 0 ]] && error "Run as root: sudo bash $0"

info "=== Radio monitor — server update ==="
info "APP_DIR=$APP_DIR  APP_USER=$APP_USER  branch=$GIT_BRANCH"

[[ -d "$APP_DIR/.git" ]] || error "Not a git repo: $APP_DIR (clone the app there first)"

if ! id "$APP_USER" &>/dev/null; then
  error "Unix user '$APP_USER' does not exist. Create it or set APP_USER=..."
fi

run_as_app() { sudo -u "$APP_USER" -H bash -lc "cd \"$APP_DIR\" && $*"; }

info "Fetching and pulling $GIT_REMOTE/$GIT_BRANCH…"
run_as_app "git fetch \"$GIT_REMOTE\" \"$GIT_BRANCH\" && git checkout \"$GIT_BRANCH\" && git pull \"$GIT_REMOTE\" \"$GIT_BRANCH\""
short_head=$(sudo -u "$APP_USER" git -C "$APP_DIR" rev-parse --short HEAD 2>/dev/null || echo "?")
success "Git HEAD: $short_head"

info "Installing Node dependencies (npm ci)…"
run_as_app "npm ci --no-fund --no-audit"

info "Database migrations…"
if run_as_app "npx prisma migrate deploy"; then
  :
else
  warn "migrate deploy failed; trying prisma db push (dev / throwaway DBs only)…"
  run_as_app "npx prisma db push --accept-data-loss" || error "Prisma migration failed"
fi

info "Prisma generate…"
run_as_app "npx prisma generate"

info "Building app…"
run_as_app "npm run build"

if [[ -f "$APP_DIR/requirements.txt" ]]; then
  info "Python dependencies…"
  run_as_app "pip3 install --user --quiet -r requirements.txt" || warn "pip install had issues (optional on some hosts)"
fi

mkdir -p "$APP_DIR/logs"
chown "$APP_USER":"$APP_USER" "$APP_DIR/logs" 2>/dev/null || true

if [[ "$RESTART_APP" -eq 1 ]]; then
  info "Reloading systemd and restarting app…"
  systemctl daemon-reload
  if systemctl is-enabled mostify-app.service &>/dev/null || systemctl is-active mostify-app.service &>/dev/null; then
    systemctl restart mostify-app.service
    success "mostify-app.service restarted"
  else
    warn "mostify-app.service not found or not enabled — start it after setup: systemctl start mostify-app"
  fi

  if [[ "$RESTART_TIMERS" -eq 1 ]]; then
    for unit in mostify-orb-poller.timer mostify-drain.timer mostify-watchdog.timer \
                mostify-stream-validator.timer mostify-harvest-zambia.timer; do
      if systemctl cat "$unit" &>/dev/null; then
        systemctl restart "$unit" 2>/dev/null && info "restarted $unit" || true
      fi
    done
    success "Timer units restarted (where installed)"
  fi
else
  info "Skipping systemd restart (--no-restart). Run: systemctl restart mostify-app"
fi

echo
success "Update finished."
echo "  Logs: journalctl -u mostify-app -f"
echo "  Or:   tail -f $APP_DIR/logs/app.log"

#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/radio-monitor"
LOG_FILE="$APP_DIR/logs/radiobox-weekly-playlist-recovery.log"
LOCK_FILE="/tmp/mostify-radiobox-weekly-playlist.lock"

cd "$APP_DIR"
mkdir -p logs data/radiobox-weekly-playlists

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "==== $(date -Is) RadioBox weekly recovery already running ====" >> "$LOG_FILE"
  exit 0
fi

echo "==== $(date -Is) RadioBox weekly recovery started ====" >> "$LOG_FILE"

echo "[1/2] Running safe OnlineRadioBox track poller..." >> "$LOG_FILE"
node scripts/orb_track_poller.mjs >> "$LOG_FILE" 2>&1

echo "[2/2] Syncing detections to spins..." >> "$LOG_FILE"
node scripts/sync_detections_to_spins.mjs >> "$LOG_FILE" 2>&1

echo "==== $(date -Is) RadioBox weekly recovery finished ====" >> "$LOG_FILE"

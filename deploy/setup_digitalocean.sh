#!/usr/bin/env bash
# setup_digitalocean.sh
# ─────────────────────────────────────────────────────────────────────────────
# Complete DigitalOcean Ubuntu 22/24 setup for the Zambian Airplay Monitor.
# Run once on a fresh droplet, or re-run to update.  Safe to re-run.
#
# What this does:
#   1. Installs system packages: ffmpeg, fpcalc (Chromaprint), Node 20, Python 3
#   2. Installs Node + Python app dependencies
#   3. Runs Prisma migrations
#   4. Creates optimised .env (if not already present)
#   5. Installs THREE systemd services:
#        mostify-app.service        — the Node.js API + UI server
#        mostify-orb-poller.service — OnlineRadioBox track polling (every 45s)
#        mostify-catalog-repair.timer — nightly catalog repair run
#   6. Enables log rotation
#   7. Opens firewall port 3000 (or 80/443 if Nginx proxy is requested)
#
# Usage:
#   chmod +x deploy/setup_digitalocean.sh
#   sudo bash deploy/setup_digitalocean.sh
#
#   # Optional: put app behind Nginx on port 80
#   sudo bash deploy/setup_digitalocean.sh --with-nginx
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

WITH_NGINX=0
for arg in "$@"; do [[ "$arg" == "--with-nginx" ]] && WITH_NGINX=1; done

APP_DIR="${APP_DIR:-/opt/mostify}"
APP_USER="${APP_USER:-mostify}"
NODE_VERSION="20"

# ── colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── must be root ──────────────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "Run as root: sudo bash $0"

info "=== Zambian Airplay Monitor — DigitalOcean Setup ==="
echo

# ─────────────────────────────────────────────────────────────────────────────
# 1. SYSTEM PACKAGES
# ─────────────────────────────────────────────────────────────────────────────
info "Updating package index…"
apt-get update -qq

info "Installing ffmpeg (audio capture + decode)…"
apt-get install -y -qq ffmpeg

info "Installing Chromaprint fpcalc (fingerprinting)…"
apt-get install -y -qq libchromaprint-tools 2>/dev/null || \
  apt-get install -y -qq chromaprint-tools 2>/dev/null || \
  (
    warn "Package name differs — trying manual install of fpcalc…"
    apt-get install -y -qq libavcodec-dev libavformat-dev libavutil-dev libfftw3-dev
    TMP=$(mktemp -d)
    CHROMA_VER="1.5.1"
    wget -qO "$TMP/chroma.tar.gz" \
      "https://github.com/acoustid/chromaprint/releases/download/v${CHROMA_VER}/chromaprint-fpcalc-${CHROMA_VER}-linux-x86_64.tar.gz"
    tar -xzf "$TMP/chroma.tar.gz" -C "$TMP"
    install -m 755 "$TMP/chromaprint-fpcalc-${CHROMA_VER}-linux-x86_64/fpcalc" /usr/local/bin/fpcalc
    rm -rf "$TMP"
  )

# Verify
fpcalc -version 2>/dev/null && success "fpcalc is working: $(fpcalc -version 2>&1 | head -1)" || warn "fpcalc not found — fingerprinting will fall back to ffmpeg chromaprint"
ffmpeg -version 2>/dev/null | head -1 && success "ffmpeg is available" || error "ffmpeg install failed"

info "Installing other system deps…"
apt-get install -y -qq python3 python3-pip curl git jq ufw

# ─────────────────────────────────────────────────────────────────────────────
# 2. NODE.JS 20
# ─────────────────────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null || [[ "$(node -e 'process.stdout.write(process.version.slice(1).split(".")[0])')" -lt "$NODE_VERSION" ]]; then
  info "Installing Node.js ${NODE_VERSION}…"
  curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash -
  apt-get install -y -qq nodejs
fi
success "Node.js: $(node -v)  npm: $(npm -v)"

# ─────────────────────────────────────────────────────────────────────────────
# 3. APP USER + DIRECTORY
# ─────────────────────────────────────────────────────────────────────────────
if ! id "$APP_USER" &>/dev/null; then
  info "Creating system user '$APP_USER'…"
  useradd -r -m -s /bin/bash -d "$APP_DIR" "$APP_USER"
fi

if [[ ! -d "$APP_DIR" ]]; then
  error "APP_DIR $APP_DIR does not exist. Clone your repo there first:
  git clone https://github.com/Megamind33-tech/Radio-monitor- $APP_DIR"
fi

chown -R "$APP_USER":"$APP_USER" "$APP_DIR"
info "App directory: $APP_DIR  (owner: $APP_USER)"

# ─────────────────────────────────────────────────────────────────────────────
# 4. .ENV FILE
# ─────────────────────────────────────────────────────────────────────────────
ENV_FILE="$APP_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  info "Creating .env from .env.example…"
  cp "$APP_DIR/.env.example" "$ENV_FILE"
  # Apply production-safe defaults that improve match rate:
  cat >> "$ENV_FILE" <<'EOF'

# ── Production overrides for DigitalOcean ─────────────────────────────────
APP_ENV="production"
LOG_LEVEL="info"
DATABASE_URL="file:./prisma/prod.db"

# Fingerprint pipeline (tune for your DO droplet size):
#  2-core: MAX_CONCURRENT=2, MIN_GAP=750
#  4-core: MAX_CONCURRENT=4, MIN_GAP=400
FINGERPRINT_PIPELINE_MAX_CONCURRENT=2
FINGERPRINT_PIPELINE_MIN_GAP_MS=750

# Sample length: 60s gives AcoustID much better context than 20s default
FINGERPRINT_SAMPLE_SECONDS=60
DEFAULT_SAMPLE_SECONDS=60

# Retry 3 times (different offsets in the song) before giving up
FINGERPRINT_MAX_RETRIES=3
FINGERPRINT_RETRY_DELAY_MS=5000

# Store unresolved samples so you can drain them later
ARCHIVE_UNRESOLVED_SAMPLES=true
UNRESOLVED_SAMPLE_MAX_PER_STATION=50

# Store 30s archive of first play of each song for evidence + local library
ARCHIVE_SAMPLE_SECONDS=30

# Re-run catalog lookup on recent unresolved rows every 3 minutes
CATALOG_REPAIR_ENABLED=true
CATALOG_REPAIR_BATCH_LIMIT=25

# Aggressive ICY verification — catch stuck metadata
ALLOW_STREAM_METADATA_MATCH_WITHOUT_ID=true

# Raise concurrency when you have more stations
MAX_STATION_CONCURRENCY=8
EOF
  warn "Edit $ENV_FILE and fill in ACOUSTID_API_KEY, MUSICBRAINZ_USER_AGENT, DATABASE_URL before starting."
else
  success ".env already exists — skipping creation"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5. NODE + PYTHON DEPENDENCIES + PRISMA
# ─────────────────────────────────────────────────────────────────────────────
info "Installing Node dependencies…"
cd "$APP_DIR"
sudo -u "$APP_USER" npm install --no-fund --no-audit 2>&1 | tail -5

info "Running Prisma migrations…"
sudo -u "$APP_USER" npx prisma migrate deploy 2>&1 | tail -10 || \
  (warn "migrate deploy failed; trying db push instead…" && \
   sudo -u "$APP_USER" npx prisma db push --accept-data-loss 2>&1 | tail -10)

info "Generating Prisma client…"
sudo -u "$APP_USER" npx prisma generate 2>&1 | tail -3

info "Building frontend…"
sudo -u "$APP_USER" npm run build 2>&1 | tail -10

if [[ -f requirements.txt ]]; then
  info "Installing Python dependencies…"
  sudo -u "$APP_USER" pip3 install --quiet --user -r requirements.txt || true
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. DATA DIRECTORIES
# ─────────────────────────────────────────────────────────────────────────────
for dir in "/tmp/radio_monitor" "$APP_DIR/data/song_samples" "$APP_DIR/data/unresolved_samples" "$APP_DIR/scripts/data"; do
  mkdir -p "$dir"
  chown "$APP_USER":"$APP_USER" "$dir"
done
success "Data directories created"

# ─────────────────────────────────────────────────────────────────────────────
# 7. SYSTEMD — MAIN APP SERVICE
# ─────────────────────────────────────────────────────────────────────────────
info "Installing systemd service: mostify-app.service…"
cat > /etc/systemd/system/mostify-app.service <<EOF
[Unit]
Description=Zambian Airplay Monitor (Node.js server)
Documentation=https://github.com/Megamind33-tech/Radio-monitor-
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$ENV_FILE

# Give Node enough heap for large station catalogs
Environment=NODE_OPTIONS="--max-old-space-size=512"

ExecStart=$(which node) dist/server/main.js
ExecStartPost=/bin/sleep 5

Restart=always
RestartSec=10
StartLimitIntervalSec=60
StartLimitBurst=5

# Resource limits (adjust per droplet size)
LimitNOFILE=65536
# MemoryMax=800M   # uncomment on 1GB droplets

# Logging
StandardOutput=append:$APP_DIR/logs/app.log
StandardError=append:$APP_DIR/logs/app.error.log

[Install]
WantedBy=multi-user.target
EOF

mkdir -p "$APP_DIR/logs"
chown "$APP_USER":"$APP_USER" "$APP_DIR/logs"
success "mostify-app.service installed"

# ─────────────────────────────────────────────────────────────────────────────
# 8. SYSTEMD — ORB POLLER (catches track changes from OnlineRadioBox)
# ─────────────────────────────────────────────────────────────────────────────
info "Installing systemd service: mostify-orb-poller.service…"
cat > /etc/systemd/system/mostify-orb-poller.service <<EOF
[Unit]
Description=Zambian Airplay Monitor — OnlineRadioBox Track Poller
After=mostify-app.service
Requires=mostify-app.service

[Service]
Type=oneshot
User=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$(which node) scripts/orb_track_poller.mjs
StandardOutput=append:$APP_DIR/logs/orb-poller.log
StandardError=append:$APP_DIR/logs/orb-poller.error.log
EOF

cat > /etc/systemd/system/mostify-orb-poller.timer <<EOF
[Unit]
Description=Run ORB track poller every 45 seconds
After=mostify-app.service

[Timer]
OnBootSec=30s
OnUnitActiveSec=45s
Unit=mostify-orb-poller.service

[Install]
WantedBy=timers.target
EOF
success "mostify-orb-poller timer installed"

# ─────────────────────────────────────────────────────────────────────────────
# 9. SYSTEMD — NIGHTLY UNRESOLVED DRAIN
# ─────────────────────────────────────────────────────────────────────────────
info "Installing systemd timer: mostify-drain.timer…"
cat > /etc/systemd/system/mostify-drain.service <<EOF
[Unit]
Description=Zambian Airplay Monitor — Drain unresolved sample backlog
After=mostify-app.service

[Service]
Type=oneshot
User=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$(which node) scripts/drain_unresolved_aggressive.mjs --max-passes 200 --batch 30
TimeoutStartSec=3600
StandardOutput=append:$APP_DIR/logs/drain.log
StandardError=append:$APP_DIR/logs/drain.error.log
EOF

cat > /etc/systemd/system/mostify-drain.timer <<EOF
[Unit]
Description=Drain unresolved audio samples nightly
After=mostify-app.service

[Timer]
# Run at 3am every day (low traffic window)
OnCalendar=*-*-* 03:00:00
RandomizedDelaySec=300
Persistent=true
Unit=mostify-drain.service

[Install]
WantedBy=timers.target
EOF
success "mostify-drain timer installed"

# ─────────────────────────────────────────────────────────────────────────────
# 10. LOG ROTATION
# ─────────────────────────────────────────────────────────────────────────────
cat > /etc/logrotate.d/mostify <<EOF
$APP_DIR/logs/*.log {
  daily
  rotate 14
  compress
  delaycompress
  missingok
  notifempty
  copytruncate
}
EOF
success "Log rotation configured"

# ─────────────────────────────────────────────────────────────────────────────
# 11. FIREWALL
# ─────────────────────────────────────────────────────────────────────────────
if command -v ufw &>/dev/null; then
  ufw allow OpenSSH  >/dev/null 2>&1 || true
  ufw allow 3000/tcp >/dev/null 2>&1 || true
  [[ $WITH_NGINX -eq 1 ]] && { ufw allow 'Nginx Full' >/dev/null 2>&1 || true; }
  ufw --force enable >/dev/null 2>&1 || true
  success "UFW: SSH + port 3000 opened"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 12. NGINX REVERSE PROXY (optional)
# ─────────────────────────────────────────────────────────────────────────────
if [[ $WITH_NGINX -eq 1 ]]; then
  apt-get install -y -qq nginx certbot python3-certbot-nginx
  cat > /etc/nginx/sites-available/mostify <<'EOF'
server {
  listen 80;
  server_name _;  # replace with your domain

  # Increase timeouts for SSE (real-time events)
  proxy_read_timeout 3600s;
  proxy_send_timeout 3600s;

  location / {
    proxy_pass http://127.0.0.1:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection 'upgrade';
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_cache_bypass $http_upgrade;

    # Required for SSE (Server-Sent Events)
    proxy_buffering off;
    proxy_cache off;
  }
}
EOF
  ln -sf /etc/nginx/sites-available/mostify /etc/nginx/sites-enabled/mostify
  rm -f /etc/nginx/sites-enabled/default
  nginx -t && systemctl restart nginx
  success "Nginx configured (port 80 → 3000)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 13. RELOAD SYSTEMD + ENABLE SERVICES
# ─────────────────────────────────────────────────────────────────────────────
systemctl daemon-reload
systemctl enable mostify-app.service
systemctl enable mostify-orb-poller.timer
systemctl enable mostify-drain.timer
success "Services enabled"

# ─────────────────────────────────────────────────────────────────────────────
# DONE
# ─────────────────────────────────────────────────────────────────────────────
echo
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  SETUP COMPLETE${NC}"
echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
echo
echo "  Next steps:"
echo
echo "  1. Edit your .env file:"
echo "     nano $ENV_FILE"
echo "     → Set ACOUSTID_API_KEY (get free key at acoustid.org/new-application)"
echo "     → Set MUSICBRAINZ_USER_AGENT (your app name + email)"
echo "     → Set DATABASE_URL if using external Postgres"
echo
echo "  2. Start the app:"
echo "     systemctl start mostify-app"
echo "     systemctl start mostify-orb-poller.timer"
echo "     systemctl start mostify-drain.timer"
echo
echo "  3. Watch the logs:"
echo "     journalctl -u mostify-app -f"
echo "     tail -f $APP_DIR/logs/app.log"
echo
echo "  4. After ~1 hour of runtime, run the optimizer:"
echo "     cd $APP_DIR && node scripts/optimize_stations.mjs --fix-all"
echo
echo "  5. Check match rate:"
echo "     node scripts/diagnose_match_rate.mjs"
echo
fpcalc -version 2>/dev/null && echo "  ✅ fpcalc:  $(fpcalc -version 2>&1 | head -1)" || echo "  ⚠️  fpcalc not found — fingerprinting will use ffmpeg fallback"
echo "  ✅ ffmpeg:  $(ffmpeg -version 2>&1 | head -1)"
echo "  ✅ node:    $(node -v)"
echo

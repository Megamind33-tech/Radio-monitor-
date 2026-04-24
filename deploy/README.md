# Production: systemd (Linux VM)

Run the **built** Node server continuously, plus **timed** jobs for OnlineRadioBox track polling and optional stream health audits. Adjust paths and user to match your server.

## Prerequisites on the host

- Node.js 20+ (note the full path: `which node`)
- `npm ci` (or `npm install`) and `npm run build` in the app directory
- `npx prisma migrate deploy` (or `db push` only for throwaway dev DBs)
- `ffmpeg`, `ffprobe`, `fpcalc` on `PATH` for the main monitor user
- Python 3 + `pycryptodome` if you rely on **MyTuner** auto-refresh (`pip install pycryptodome`)

## 1. Install directory and env file

Example install root: `/opt/radio-monitor` (replace throughout).

Create a root-owned env file (not world-readable) with at least:

```bash
DATABASE_URL="postgresql://..."
ACOUSTID_API_KEY="..."
MUSICBRAINZ_USER_AGENT="MOSTIFY/1.0.0 ( you@example.com )"
NODE_ENV=production
ARCHIVE_UNRESOLVED_SAMPLES="true"
UNRESOLVED_SAMPLE_MAX_PER_STATION="0"   # keep all unresolved audio captures (no pruning)
SONG_SAMPLE_ARCHIVE_DIR="/opt/radio-monitor/data/song_samples"
UNRESOLVED_SAMPLE_DIR="/opt/radio-monitor/data/unresolved_samples"
```

Optional: `DEEZER_LOOKUP_ENABLED`, `LOG_LEVEL`, etc.

**Paid audio fallbacks** (after AcoustID miss when ICY is flagged non-song; requires same `ffmpeg`/`fpcalc` as fingerprinting):

```bash
# AudD — https://dashboard.audd.io/
AUDD_API_TOKEN="your_token"

# ACRCloud Identify — host from project console, e.g. identify-eu-west-1.acrcloud.com
ACRCLOUD_HOST="identify-eu-west-1.acrcloud.com"
ACRCLOUD_ACCESS_KEY="..."
ACRCLOUD_ACCESS_SECRET="..."

# Set to false to never call AudD/ACRCloud (AcoustID + free catalog only)
# PAID_AUDIO_FALLBACKS_ENABLED="true"
```

Reload systemd after editing env: `sudo systemctl daemon-reload` (if unit references env file) and `sudo systemctl restart mostify-monitor.service`.

```bash
sudo install -d -o root -g root -m 0755 /etc/mostify-monitor
sudo install -m 0600 /path/to/your.env /etc/mostify-monitor/mostify-monitor.env
```

## 2. Edit unit files

Copy the templates from `deploy/systemd/` to `/etc/systemd/system/` and **edit**:

- `User=` / `Group=` — uncomment and set the same dedicated Unix user on **all** units that write under the app tree (`scripts/data/`, song samples). The process user needs read access to `/etc/mostify-monitor/mostify-monitor.env` if you use root-only permissions on that file (e.g. `chmod 640` and `chgrp mostify`).
- `WorkingDirectory=` — your clone path (must contain `dist/` after build)
- `ExecStart=` — first argument must be the output of `which node` on that machine

```bash
sudo cp deploy/systemd/*.service deploy/systemd/*.timer /etc/systemd/system/
sudo nano /etc/systemd/system/mostify-monitor.service
# ... same for poller + audit if used
sudo systemctl daemon-reload
```

## 3. Enable services

**Main app** (dashboard + station scheduler):

```bash
sudo systemctl enable --now mostify-monitor.service
sudo systemctl status mostify-monitor.service
```

**ORB track poller** (one-shot on a timer; complements ICY for ORB-backed stations):

```bash
sudo systemctl enable --now mostify-orb-poller.timer
sudo systemctl list-timers 'mostify-*'
```

**Stream health audit** (optional; writes CSV under the app tree):

```bash
sudo systemctl enable --now mostify-stream-health-audit.timer
journalctl -u mostify-stream-health-audit.service -n 50 --no-pager
```

## 4. Logs and operations

```bash
journalctl -u mostify-monitor.service -f
sudo systemctl restart mostify-monitor.service
```

After `git pull` and `npm run build`, restart the main service. Schema changes require `npx prisma migrate deploy` before restart.

## Reverse proxy (optional)

Put **nginx** or **Caddy** in front of port **3000** for HTTPS and a stable public URL; keep the app bound to localhost if you only expose through the proxy.

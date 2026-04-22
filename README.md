# Zambian Airplay Monitor — Starter Kit

Scrapes ICY metadata (now-playing data) from internet radio streams and logs every track change to Supabase. No audio fingerprinting, no ACRCloud bill, no fake science. Just HTTP + Postgres.

## Files

| File | What it does |
|------|----------------|
| `audit_stations.py` | Day-1 tool. Tests which stations broadcast usable metadata. Writes `stations_audit.csv`. Doesn't touch the DB. |
| `monitor.py` | Long-running engine. Connects to every station, logs track changes to Supabase. Auto-reconnects. |
| `schema.sql` | Run once in Supabase SQL editor to create tables + handy views. |
| `stations.csv` | Your list of stations. Fill in stream URLs. |
| `requirements.txt` | Python dependencies (`pip install -r requirements.txt`). |

This repository also contains a separate Node/React stack under `server/` and `src/` for broader fingerprinting experiments; the **ICY + Supabase** workflow uses only the Python files above.

## Install

```bash
pip install -r requirements.txt
```

Or:

```bash
pip install aiohttp asyncpg
```

## Setup — one-time

### 1. Create Supabase tables

- Open your Supabase project → SQL Editor → New query
- Paste the entire contents of `schema.sql`
- Click Run

### 2. Get your DB connection string

- Supabase dashboard → Project Settings → Database → **Connection string** → **URI**
- Use the **Session** pooler (port 5432) for normal use
- Copy the URL (looks like `postgresql://postgres.xxx:PASSWORD@aws-0-region.pooler.supabase.com:5432/postgres`)
- Replace `[YOUR-PASSWORD]` with your actual database password

### 3. Export it

```bash
export SUPABASE_DB_URL='postgresql://postgres.xxx:PASSWORD@...pooler.supabase.com:5432/postgres'
```

For production, put this in a `.env` file loaded by systemd or pm2.

## Usage

### Step 1 — get stream URLs

Find each station's stream URL. Options:

- **TuneIn** / **Streema** / **myTuner Radio** — search "Zambia", right-click play → copy link
- **radio-browser.info** has a free API:

  ```bash
  curl -s 'https://de1.api.radio-browser.info/json/stations/bycountry/Zambia' \
    | python3 -c "import json,sys,re; \
      [print(f'{re.sub(r\"[^a-z0-9]+\",\"_\",s[\"name\"].lower())[:20]},{s[\"name\"]},{s[\"url_resolved\"]}') \
       for s in json.load(sys.stdin) if s['url_resolved']]"
  ```

- **Station website** — view page source on their web player

Paste into `stations.csv`:

```csv
station_id,name,stream_url
hot_fm,Hot FM Zambia,http://stream.example.com/hot
phoenix,Radio Phoenix,http://stream.example.com/phx
```

### Step 2 — audit (no DB needed)

```bash
python audit_stations.py stations.csv
```

For each station, takes ~30 seconds (connect, sample, wait 25s, sample again). Writes a CSV report with verdict per station:

- `✅ good` — metadata is live and changes between tracks. **USE THESE.**
- `⚠️ partial` — metadata exists but didn't change in 25s. Could be a long track. Re-run or watch longer.
- `❌ no metadata` — station streams but sends no metadata. Drop or fall back to fingerprinting.
- `💀 dead` — stream URL broken or offline. Find a new URL.

### Step 3 — monitor

Strip `none`/`dead` stations from `stations.csv`, then:

```bash
python monitor.py stations.csv
```

Every track change gets written to Supabase in real time. Leave it running.

## Node stack: Radio Garden (Zambia) + import

The harvester `scripts/zambia_station_harvest.py` pulls **all Radio Garden stations** exposed for Zambia on their public API: the country page JSON at `https://radio.garden/api/ara/content/page/XbLRE6NT` includes a **Popular Stations** channel list plus **Places in Zambia** maps; every map’s `/channels` feed is merged (deduped by resolved stream URL). That matches what you see on [radio.garden](https://radio.garden/) for Zambia; there is no separate hidden catalog beyond what their API returns.

```bash
npm run harvest:zambia
npm run import:zambia
# optional full replace of Zambia catalog in Prisma:
# npm run import:zambia:replace
```

Harvest output enables **fingerprint fallback** and **archiveSongSamples** for imported rows (when present in JSON) to support audio matching alongside ICY.

## Inspect the data (Supabase SQL Editor)

```sql
-- what's playing right now on each station
select * from v_now_playing;

-- today's activity per station (sanity check the monitor is working)
select * from v_station_activity_today;

-- top artists across all stations last 7 days
select parsed_artist, sum(spins) as total_spins
from v_top_artists_7d
group by parsed_artist
order by total_spins desc
limit 25;

-- every play of a specific artist in the last week
select station_id, captured_at, raw_title
from detections
where parsed_artist ilike '%chef 187%'
  and captured_at >= now() - interval '7 days'
order by captured_at desc;

-- station health — when did each one last connect/disconnect?
select station_id, event_type, event_at, detail
from station_events
order by event_at desc
limit 50;
```

## Run forever (production)

### systemd (recommended for VPS)

Create `/etc/systemd/system/zamplay-monitor.service`:

```ini
[Unit]
Description=Zambian Airplay Monitor
After=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/zamplay-monitor
EnvironmentFile=/home/ubuntu/zamplay-monitor/.env
ExecStart=/usr/bin/python3 /home/ubuntu/zamplay-monitor/monitor.py /home/ubuntu/zamplay-monitor/stations.csv
Restart=always
RestartSec=10
StandardOutput=append:/var/log/zamplay-monitor.log
StandardError=append:/var/log/zamplay-monitor.log

[Install]
WantedBy=multi-user.target
```

Where `.env` contains:

```
SUPABASE_DB_URL=postgresql://...
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable zamplay-monitor
sudo systemctl start zamplay-monitor
sudo journalctl -u zamplay-monitor -f
```

## What this does NOT do yet (roadmap)

1. **Fuzzy match against your ZAMCOPS member catalog.** `raw_title` is whatever the station typed: "Macky 2", "Macky2", "MACKY II" are different strings. Next step is a matching layer using `rapidfuzz` against a `zamcops_songs` table.

2. **Filter ads and talk segments better.** `is_useful_title()` in `monitor.py` is a starting filter. Watch your data for a few days, then extend the junk list.

3. **Audio evidence capture.** For royalty disputes, add a per-station ffmpeg sidecar writing rolling 60-min WAV chunks to B2/S3 with a 14-day retention.

4. **Dashboard.** Use the existing UI on top of the Supabase client. The views in `schema.sql` are already shaped for this.

## Known limits (the honest bit)

- If a station's automation doesn't push ICY metadata, this approach gives nothing for that station. Audit first.
- Metadata is what the station says it's playing, not audio-verified. Fine for royalty logging, weaker for legal disputes.
- DJs sometimes override the queue; metadata may lag or be wrong.
- FM-only stations with no internet stream need a physical receiver + RTL-SDR. Out of scope here.

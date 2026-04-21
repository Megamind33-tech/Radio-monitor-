"""
monitor.py
----------
Continuously monitors internet radio streams for ICY metadata and logs every
track change to Supabase (Postgres) via asyncpg.

For each station it runs an async worker that:
  1. Connects to the stream with Icy-MetaData: 1
  2. Reads ICY metadata blocks
  3. When StreamTitle changes -> write a detection row
  4. Auto-reconnects on any failure with exponential backoff

Designed to run forever. Use systemd / pm2 / nohup in production.

Setup:
  1. In Supabase SQL editor, run the SQL from schema.sql (shipped alongside this file)
  2. Get your DB URL: Supabase dashboard -> Project Settings -> Database
     -> "Connection string" -> "URI" tab. Use the Session pooler (port 5432)
     unless you're running many monitor processes.
  3. export SUPABASE_DB_URL='postgresql://postgres.<ref>:<password>@<host>:5432/postgres'
  4. python monitor.py stations.csv
"""

import asyncio
import csv
import os
import sys
import time
import signal
from dataclasses import dataclass
from typing import Optional

import aiohttp    # pip install aiohttp
import asyncpg    # pip install asyncpg

# ---------- tunables ----------
USER_AGENT = "ZamPlayMonitor/0.1"
RECONNECT_BASE_DELAY = 5        # seconds
RECONNECT_MAX_DELAY = 300       # cap backoff at 5 min
IDLE_RECONNECT_AFTER = 600      # reconnect after 10 min of no title change
READ_CHUNK = 4096
DB_POOL_MIN = 2
DB_POOL_MAX = 10
# ------------------------------


# ---------------- DB operations ----------------

async def upsert_station(pool, station_id: str, name: str, url: str):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            insert into stations (station_id, name, stream_url)
            values ($1, $2, $3)
            on conflict (station_id) do update set
                name = excluded.name,
                stream_url = excluded.stream_url
            """,
            station_id, name, url,
        )


async def insert_detection(pool, station_id: str, raw_title: str,
                           artist: Optional[str], title: Optional[str]):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            insert into detections (station_id, raw_title, parsed_artist, parsed_title)
            values ($1, $2, $3, $4)
            """,
            station_id, raw_title, artist, title,
        )


async def insert_event(pool, station_id: str, event_type: str, detail: str = ""):
    """Never let event-log failures kill a worker."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                insert into station_events (station_id, event_type, detail)
                values ($1, $2, $3)
                """,
                station_id, event_type, detail[:500],
            )
    except Exception as e:
        print(f"[db] failed to log event for {station_id}: {e}")


# ---------------- ICY parsing ----------------

def parse_stream_title(meta_bytes: bytes) -> Optional[str]:
    """Extract StreamTitle='...' from an ICY metadata block."""
    if not meta_bytes:
        return None
    text = meta_bytes.rstrip(b"\x00").decode("utf-8", errors="replace")
    for field in text.split(";"):
        field = field.strip()
        if field.startswith("StreamTitle="):
            value = field[len("StreamTitle="):].strip()
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            elif value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            return value.strip() or None
    return None


def split_artist_title(raw: str) -> tuple[Optional[str], Optional[str]]:
    """Best-effort split. Stations are inconsistent; we always keep raw too."""
    if not raw:
        return None, None
    for sep in [" - ", " – ", " — ", " | "]:
        if sep in raw:
            a, _, t = raw.partition(sep)
            return a.strip() or None, t.strip() or None
    return None, raw.strip()


def is_useful_title(title: str) -> bool:
    """Filter obvious non-song metadata. Tune this as you learn your stations."""
    if not title:
        return False
    t = title.strip().lower()
    if len(t) < 3:
        return False
    junk = {"advertisement", "adverts", "ads", "ad break", "commercial",
            "live", "on air", "news", "station id", "jingle", "jingles"}
    if t in junk:
        return False
    return True


# ---------------- per-station worker ----------------

@dataclass
class Station:
    station_id: str
    name: str
    stream_url: str


async def station_worker(station: Station, pool, stop_event: asyncio.Event):
    backoff = RECONNECT_BASE_DELAY
    last_title: Optional[str] = None
    last_change_ts = time.time()

    while not stop_event.is_set():
        session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=30),
            headers={"Icy-MetaData": "1", "User-Agent": USER_AGENT},
        )
        try:
            async with session.get(station.stream_url) as resp:
                metaint_hdr = resp.headers.get("icy-metaint")
                if not metaint_hdr:
                    await insert_event(pool, station.station_id, "error",
                                       "no icy-metaint header")
                    print(f"[{station.station_id}] no metaint header -- station doesn't broadcast metadata")
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=3600)
                        break
                    except asyncio.TimeoutError:
                        continue

                metaint = int(metaint_hdr)
                await insert_event(pool, station.station_id, "connected",
                                   f"metaint={metaint}")
                print(f"[{station.station_id}] connected, metaint={metaint}")
                backoff = RECONNECT_BASE_DELAY

                while not stop_event.is_set():
                    # Skip metaint bytes of audio
                    audio_remaining = metaint
                    while audio_remaining > 0:
                        chunk = await resp.content.read(min(READ_CHUNK, audio_remaining))
                        if not chunk:
                            raise ConnectionError("stream closed mid-audio")
                        audio_remaining -= len(chunk)

                    # 1-byte length prefix
                    length_bytes = await resp.content.readexactly(1)
                    meta_len = length_bytes[0] * 16

                    if meta_len > 0:
                        meta_bytes = await resp.content.readexactly(meta_len)
                        raw_title = parse_stream_title(meta_bytes)

                        if raw_title and raw_title != last_title and is_useful_title(raw_title):
                            artist, title = split_artist_title(raw_title)
                            await insert_detection(pool, station.station_id,
                                                   raw_title, artist, title)
                            print(f"[{station.station_id}] ▶ {raw_title}")
                            last_title = raw_title
                            last_change_ts = time.time()

                    # Health check: reconnect if metadata has been stuck for ages
                    if time.time() - last_change_ts > IDLE_RECONNECT_AFTER:
                        print(f"[{station.station_id}] idle too long, reconnecting")
                        last_change_ts = time.time()
                        break

        except asyncio.CancelledError:
            break
        except Exception as e:
            await insert_event(pool, station.station_id, "disconnected", str(e)[:200])
            print(f"[{station.station_id}] disconnected: {e}  (retry in {backoff}s)")
        finally:
            await session.close()

        if stop_event.is_set():
            break

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=backoff)
            break
        except asyncio.TimeoutError:
            pass
        backoff = min(backoff * 2, RECONNECT_MAX_DELAY)


# ---------------- orchestrator ----------------

def load_stations(path: str) -> list[Station]:
    stations = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            if row[0].lower() == "station_id":
                continue
            if len(row) < 3:
                continue
            if not row[2].strip():
                print(f"skipping {row[0]} (no stream_url)")
                continue
            stations.append(Station(row[0].strip(), row[1].strip(), row[2].strip()))
    return stations


async def main_async(stations: list[Station], db_url: str):
    pool = await asyncpg.create_pool(
        dsn=db_url,
        min_size=DB_POOL_MIN,
        max_size=DB_POOL_MAX,
        command_timeout=10,
    )

    for s in stations:
        await upsert_station(pool, s.station_id, s.name, s.stream_url)

    stop_event = asyncio.Event()

    def handle_signal():
        print("\nshutdown requested, closing workers...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    workers = [asyncio.create_task(station_worker(s, pool, stop_event))
               for s in stations]

    print(f"monitoring {len(stations)} stations -> Supabase. Ctrl+C to stop.\n")
    await asyncio.gather(*workers, return_exceptions=True)
    await pool.close()
    print("stopped.")


def main():
    if len(sys.argv) < 2:
        print("usage: python monitor.py stations.csv")
        print("needs env var: SUPABASE_DB_URL")
        sys.exit(1)

    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("error: set SUPABASE_DB_URL env var")
        print("  get it from Supabase -> Project Settings -> Database -> Connection string (URI)")
        sys.exit(1)

    stations = load_stations(sys.argv[1])
    if not stations:
        print("no stations loaded. fill in stream_url in stations.csv first.")
        sys.exit(1)

    asyncio.run(main_async(stations, db_url))


if __name__ == "__main__":
    main()

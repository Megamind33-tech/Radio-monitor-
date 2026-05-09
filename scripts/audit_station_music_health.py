#!/usr/bin/env python3
import csv
import os
import sqlite3
from datetime import datetime, timezone

DB="/opt/radio-monitor/prisma/dev_runtime.db"
OUT_DIR="/opt/radio-monitor/data/monitor-audit"

os.makedirs(OUT_DIR, exist_ok=True)

def parse_dt(v):
    if not v:
        return None
    s=str(v).replace("Z","+00:00")
    try:
        if "T" in s:
            return datetime.fromisoformat(s)
        return datetime.fromisoformat(s.replace(" ","T")+"+00:00")
    except Exception:
        return None

def minutes_old(v):
    dt=parse_dt(v)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt=dt.replace(tzinfo=timezone.utc)
    return round((datetime.now(timezone.utc)-dt).total_seconds()/60, 1)

def yes(v):
    return str(v).lower() in ("1","true","yes")

con=sqlite3.connect(DB)
con.row_factory=sqlite3.Row

stations=con.execute("""
SELECT
  id,
  name,
  streamUrl,
  preferredStreamUrl,
  isActive,
  lastPollStatus,
  lastPollError,
  monitorState,
  monitorStateReason,
  contentClassification,
  lastHealthyAt,
  lastGoodAudioAt,
  lastMetadataAt,
  lastSongDetectedAt,
  consecutivePollFailures,
  consecutiveHealthyPolls,
  streamOnlineLast,
  audioDetectedLast,
  metadataAvailableLast,
  songIdentifiedLast,
  fingerprintFallbackEnabled,
  metadataPriorityEnabled,
  sourceIdsJson
FROM Station
ORDER BY name;
""").fetchall()

def latest_endpoint(station_id):
    return con.execute("""
    SELECT
      streamUrl,
      resolvedUrl,
      lastValidationStatus,
      isSuppressed,
      consecutiveFailures,
      lastFailureReason,
      lastValidatedAt,
      lastHealthyAt
    FROM StationStreamEndpoint
    WHERE stationId=?
    ORDER BY lastValidatedAt DESC
    LIMIT 1;
    """,(station_id,)).fetchone()

rows=[]
groups={
    "working_music": [],
    "online_audio_but_no_music_match": [],
    "online_but_mute_or_no_audio": [],
    "stream_problem": [],
    "inactive_in_db": [],
    "needs_observation": [],
}

for s in stations:
    ep=latest_endpoint(s["id"])

    is_active=bool(s["isActive"])
    stream_online=yes(s["streamOnlineLast"])
    audio_detected=yes(s["audioDetectedLast"])
    metadata_available=yes(s["metadataAvailableLast"])
    song_identified=yes(s["songIdentifiedLast"])

    poll_failures=int(s["consecutivePollFailures"] or 0)
    endpoint_failures=int(ep["consecutiveFailures"] if ep else 0)
    endpoint_status=(ep["lastValidationStatus"] if ep else "") or ""
    endpoint_suppressed=bool(ep["isSuppressed"]) if ep else False

    last_good_audio_min=minutes_old(s["lastGoodAudioAt"])
    last_song_min=minutes_old(s["lastSongDetectedAt"])
    last_healthy_min=minutes_old(s["lastHealthyAt"])
    last_metadata_min=minutes_old(s["lastMetadataAt"])

    stream_problem = (
        (not stream_online and poll_failures >= 1)
        or endpoint_status == "inactive"
        or endpoint_suppressed
        or endpoint_failures >= 3
    )

    if not is_active:
        group="inactive_in_db"
    elif stream_problem:
        group="stream_problem"
    elif stream_online and audio_detected and song_identified:
        group="working_music"
    elif stream_online and audio_detected and not song_identified:
        group="online_audio_but_no_music_match"
    elif stream_online and not audio_detected:
        group="online_but_mute_or_no_audio"
    elif stream_online and last_good_audio_min is not None and last_good_audio_min > 30:
        group="online_but_mute_or_no_audio"
    elif stream_online and (last_song_min is None or last_song_min > 30):
        group="online_audio_but_no_music_match"
    else:
        group="needs_observation"

    row={
        "group": group,
        "id": s["id"],
        "name": s["name"],
        "isActive": s["isActive"],
        "streamOnlineLast": s["streamOnlineLast"],
        "audioDetectedLast": s["audioDetectedLast"],
        "metadataAvailableLast": s["metadataAvailableLast"],
        "songIdentifiedLast": s["songIdentifiedLast"],
        "monitorState": s["monitorState"],
        "monitorStateReason": s["monitorStateReason"],
        "contentClassification": s["contentClassification"],
        "lastPollStatus": s["lastPollStatus"],
        "lastPollError": s["lastPollError"],
        "lastHealthyAgeMin": last_healthy_min,
        "lastGoodAudioAgeMin": last_good_audio_min,
        "lastMetadataAgeMin": last_metadata_min,
        "lastSongDetectedAgeMin": last_song_min,
        "consecutivePollFailures": poll_failures,
        "consecutiveHealthyPolls": s["consecutiveHealthyPolls"],
        "fingerprintFallbackEnabled": s["fingerprintFallbackEnabled"],
        "metadataPriorityEnabled": s["metadataPriorityEnabled"],
        "streamUrl": s["streamUrl"],
        "preferredStreamUrl": s["preferredStreamUrl"],
        "endpointStatus": endpoint_status,
        "endpointSuppressed": int(endpoint_suppressed),
        "endpointFailures": endpoint_failures,
        "endpointReason": ep["lastFailureReason"] if ep else "",
        "endpointValidatedAt": ep["lastValidatedAt"] if ep else "",
        "endpointResolvedUrl": ep["resolvedUrl"] if ep else "",
        "sourceIdsJson": s["sourceIdsJson"],
    }

    rows.append(row)
    groups[group].append(row)

fields=list(rows[0].keys()) if rows else []

with open(f"{OUT_DIR}/station_music_health_all.csv","w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=fields)
    w.writeheader()
    w.writerows(rows)

for group, items in groups.items():
    with open(f"{OUT_DIR}/{group}.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        w.writerows(items)

print("STATION GROUPS")
for group, items in groups.items():
    print(f"{group}: {len(items)}")

print("")
print("Focus files:")
for group in ["online_audio_but_no_music_match","online_but_mute_or_no_audio","stream_problem","working_music"]:
    print(f"{OUT_DIR}/{group}.csv")

print("")
print("ZNBC / suspicious candidates:")
for r in rows:
    name=(r["name"] or "").lower()
    if "znbc" in name or r["group"] in ("online_but_mute_or_no_audio","online_audio_but_no_music_match"):
        print(f'{r["group"]} | {r["id"]} | {r["name"]} | state={r["monitorState"]} | audio={r["audioDetectedLast"]} | song={r["songIdentifiedLast"]} | goodAudioAge={r["lastGoodAudioAgeMin"]} | songAge={r["lastSongDetectedAgeMin"]}')

con.close()

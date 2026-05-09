#!/usr/bin/env python3
import csv
import os
import re
import sqlite3
from urllib.parse import urlparse, unquote

DB="/opt/radio-monitor/data/fingerprint-index/zambian_fingerprint_index.db"
REVIEW="/opt/radio-monitor/data/fingerprint-index/metadata_review_safe.csv"

GENERIC_TITLES = [
    "top downloads",
    "ghana music",
    "gospel",
    "2026 all ngoni",
    "all ngoni",
    "ncwala songs",
    "ncwala",
    "zambian music",
]

KNOWN_ARTISTS = set("""
yo maps
macky 2
chef 187
slapdee
jemax
dizmo
drimz
t sean
t-sean
t low
t-low
chile one
king illest
76 drums
tommy d
ruff kid
gk boy
miracle best
bruce amara
daddy phil
y cool
xaven
veda njucci
veda nyucci
stan walilaya
prophet dm siame
one buffalo
night walker
emz dee
saliyah miyanda
sam deep
namatai
mr crown
flex euly
felugo de future
godelive
meddy
star boy
philezee chibeta
sido man
slap dee
soka boy
sperk
zaubo
zosi power
d bwoy telem
""".strip().splitlines())

def clean(s):
    s = unquote(s or "")
    s = s.replace("_"," ").replace("+"," ")
    s = re.sub(r"\.(mp3|wav|m4a|aac|ogg|flac)$","",s,flags=re.I)
    s = re.sub(r"\?.*$","",s)
    s = re.sub(r"\b(official music video|official video|official audio|music video|download|mp3)\b","",s,flags=re.I)
    s = re.sub(r"\bZHZ\b","",s,flags=re.I)
    s = re.sub(r"-?ZHZ$","",s,flags=re.I)
    s = re.sub(r"\s+"," ",s)
    return s.strip(" -–—_|:•.")

def nice(s):
    s = clean(s)
    out = []
    for w in s.split():
        lw = w.lower()
        if lw in ["ft","feat","featuring"]:
            out.append("ft")
        elif lw in ["dj","dm"]:
            out.append(lw.upper())
        elif lw in ["x","&"]:
            out.append(w)
        else:
            out.append(w[:1].upper()+w[1:])
    return " ".join(out)

def is_generic_title(s):
    low = clean(s).lower()
    return any(g in low for g in GENERIC_TITLES)

def filename(url):
    base = os.path.basename(urlparse(url or "").path)
    base = unquote(base)
    base = re.sub(r"\.(mp3|wav|m4a|aac|ogg|flac)$","",base,flags=re.I)
    return base

def remove_prod_tail(s):
    s = re.sub(r"[- ]?(prod|prod\.|produced)[- ]?(by)?[- ].*$","",s,flags=re.I)
    return s.strip(" -")

def split_slug_words(s):
    s = s.replace("---"," - ")
    s = s.replace("--"," - ")
    s = s.replace("_","-")
    s = re.sub(r"\s+"," ",s)
    return s

def known_artist_prefix(text):
    low = clean(text.replace("-"," ")).lower()
    for artist in sorted(KNOWN_ARTISTS, key=len, reverse=True):
        if low == artist:
            continue
        if low.startswith(artist + " "):
            rest = low[len(artist):].strip()
            if rest:
                return artist, rest
    return None

def infer_from_filename(raw_filename):
    raw = filename(raw_filename)
    raw = remove_prod_tail(raw)
    raw = re.sub(r"-?ZHZ$","",raw,flags=re.I)

    # Strong separator: Yo-Maps---Hello or Artist--Title
    if "---" in raw or "--" in raw:
        t = split_slug_words(raw)
        parts = [clean(p.replace("-"," ")) for p in t.split(" - ") if clean(p)]
        if len(parts) >= 2:
            artist = parts[0]
            title = " - ".join(parts[1:])
            return nice(artist), nice(title), "high", "strong_separator_filename"

    # Known artist prefix: Flex-Euly-Cinderella, King-Illest-ft-76-Drums-Mukaninga
    kp = known_artist_prefix(raw)
    if kp:
        artist, rest = kp
        rest = clean(rest.replace("-"," "))

        # If rest starts with ft/feat, preserve feature as part of artist until likely title words.
        # This is still review-medium unless the known artist is very clear.
        if re.match(r"^(ft|feat|featuring)\s+", rest, flags=re.I):
            return nice(artist), nice(rest), "review", "known_artist_with_feature_needs_review"

        return nice(artist), nice(rest), "high", "known_artist_prefix_filename"

    # Fallback: do not guess blindly.
    return "", "", "none", "no_safe_guess"

def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row

    # Learn already-good artists from DB
    for r in con.execute("SELECT DISTINCT artist FROM tracks WHERE artist IS NOT NULL AND artist!='' AND artist!='UNKNOWN'"):
        KNOWN_ARTISTS.add(clean(r["artist"]).lower())

    rows = con.execute("""
        SELECT id, artist, title, audio_url, source_page
        FROM tracks
        WHERE artist='UNKNOWN' OR artist='' OR artist IS NULL
        ORDER BY id
    """).fetchall()

    fixed = 0
    review = []

    for r in rows:
        suggested_artist, suggested_title, confidence, reason = infer_from_filename(r["audio_url"])

        if confidence == "high" and suggested_artist and suggested_title:
            con.execute(
                "UPDATE tracks SET artist=?, title=? WHERE id=?",
                (suggested_artist, suggested_title, r["id"])
            )
            fixed += 1
            print(f"FIXED {r['id']}: {suggested_artist} - {suggested_title} [{reason}]")
        else:
            review.append({
                "id": r["id"],
                "old_artist": r["artist"],
                "old_title": r["title"],
                "suggested_artist": suggested_artist,
                "suggested_title": suggested_title,
                "confidence": confidence,
                "reason": reason,
                "file": filename(r["audio_url"]),
                "audio_url": r["audio_url"],
                "source_page": r["source_page"],
            })

    con.commit()
    con.close()

    with open(REVIEW, "w", newline="", encoding="utf-8") as f:
        fields = ["id","old_artist","old_title","suggested_artist","suggested_title","confidence","reason","file","audio_url","source_page"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(review)

    print("")
    print(f"Unknown rows checked: {len(rows)}")
    print(f"Safe high-confidence fixes applied: {fixed}")
    print(f"Rows still needing review: {len(review)}")
    print(f"Review CSV: {REVIEW}")

if __name__ == "__main__":
    main()

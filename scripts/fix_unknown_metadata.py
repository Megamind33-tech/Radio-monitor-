#!/usr/bin/env python3
import csv
import os
import re
import sqlite3
from urllib.parse import urlparse, unquote

DB="/opt/radio-monitor/data/fingerprint-index/zambian_fingerprint_index.db"
REVIEW="/opt/radio-monitor/data/fingerprint-index/metadata_review_needed.csv"

GENERIC_WORDS=[
    "all ngoni",
    "ncwala",
    "top downloads",
    "gospel",
    "latest",
    "official music video",
    "official video",
    "official audio",
    "download",
    "mp3",
]

KNOWN_ARTISTS=set("""
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
tommy d & ruff kid
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
flex zulu
driemo
bobby east
k millian
jk
kay figo
mampi
cleo ice queen
nezi
kanina kandalama
towela kaira
abel chungu
pompi
mag44
mordecaii
chanda na kay
4 na 5
d bwoy telem
brawen
kantu
wezi
mumba yachi
james sakala
afunika
""".strip().splitlines())

def clean(s):
    s=unquote(s or "")
    s=s.replace("_"," ").replace("+"," ")
    s=re.sub(r"\.(mp3|wav|m4a|aac|ogg|flac)$","",s,flags=re.I)
    s=re.sub(r"\?.*$","",s)
    s=re.sub(r"\b(official audio|official video|official music video|music video|download|mp3)\b","",s,flags=re.I)
    s=re.sub(r"\b(prod|prod\.|produced)\s+by\b.*$","",s,flags=re.I)
    s=re.sub(r"\s+"," ",s)
    return s.strip(" -–—_|:•")

def title_case(s):
    s=clean(s)
    out=[]
    for w in s.split():
        lw=w.lower()
        if lw in ["ft","feat","featuring"]:
            out.append("ft")
        elif lw in ["dj","dm"]:
            out.append(lw.upper())
        elif lw in ["x","&"]:
            out.append(w)
        else:
            out.append(w[:1].upper()+w[1:])
    return " ".join(out)

def filename(url):
    return clean(os.path.basename(urlparse(url or "").path))

def slug_text(url):
    path=urlparse(url or "").path.strip("/")
    base=path.split("/")[-1]
    base=re.sub(r"-mp3-download$","",base,flags=re.I)
    return clean(base)

def is_generic(s):
    low=(s or "").lower()
    return any(g in low for g in GENERIC_WORDS)

def primary_artist(s):
    s=clean(s)
    return re.split(r"\s+(ft\.?|feat\.?|featuring)\s+",s,flags=re.I)[0].strip()

def known(s):
    return primary_artist(s).lower() in KNOWN_ARTISTS or clean(s).lower() in KNOWN_ARTISTS

def normalize_dash_text(s):
    s=clean(s)
    s=s.replace("–","-").replace("—","-")
    s=re.sub(r"\s*-\s*"," - ",s)
    s=re.sub(r"\s+"," ",s)
    return s.strip(" -")

def guess_from_dash(text):
    text=normalize_dash_text(text)
    if " - " not in text:
        return None

    parts=[clean(p) for p in text.split(" - ") if clean(p)]
    if len(parts)<2:
        return None

    left=parts[0]
    right=" - ".join(parts[1:])

    if is_generic(left):
        return None

    # Known artist on the left: Artist - Title
    if known(left):
        return title_case(left), title_case(right), "high", "known_artist_left"

    # Known artist on the right: Title - Artist
    if known(right):
        return title_case(right), title_case(left), "high", "known_artist_right_reversed"

    # If left contains ft/feat and has a known primary artist: Artist ft X - Title
    if re.search(r"\s(ft|feat|featuring)\s",left,flags=re.I) and known(primary_artist(left)):
        return title_case(left), title_case(right), "high", "known_feature_artist_left"

    # Medium only: could be Artist - Title, but not safe enough for royalty auto-fix
    return title_case(left), title_case(right), "medium", "dash_default_artist_title"

def guess_from_by(text):
    text=clean(text)
    m=re.search(r"^(.*?)\s+by\s+(.+)$",text,flags=re.I)
    if not m:
        return None
    title=clean(m.group(1))
    artist=clean(m.group(2))
    if not title or not artist:
        return None
    conf="high" if known(artist) else "medium"
    return title_case(artist), title_case(title), conf, "title_by_artist"

def guess_from_triple_dash_filename(raw):
    # Handles Yo-Maps---Hello.mp3 style before all hyphens are converted
    raw=unquote(raw or "")
    raw=os.path.basename(urlparse(raw).path)
    raw=re.sub(r"\.(mp3|wav|m4a|aac|ogg|flac)$","",raw,flags=re.I)
    if "---" not in raw:
        return None
    left,right=raw.split("---",1)
    left=clean(left.replace("-"," "))
    right=clean(right.replace("-"," "))
    if left and right and known(left):
        return title_case(left), title_case(right), "high", "triple_dash_filename"
    return None

def guess_from_known_prefix(text):
    # Handles filenames where the artist starts the string but no clean separator exists
    t=clean(text.replace("-"," "))
    low=t.lower()

    for artist in sorted(KNOWN_ARTISTS,key=len,reverse=True):
        if low.startswith(artist+" "):
            rest=clean(t[len(artist):])
            if not rest:
                continue

            # If rest starts with ft/feat and has many words, leave as review unless dash parser handled it.
            if re.match(r"^(ft|feat|featuring)\s+",rest,flags=re.I):
                return title_case(artist), title_case(rest), "medium", "known_artist_prefix_with_feature_review"

            return title_case(artist), title_case(rest), "high", "known_artist_prefix"

    return None

def best_guess(row):
    texts=[]

    current_title=row["title"] or ""
    audio_url=row["audio_url"] or ""
    source_page=row["source_page"] or ""

    # Prefer current title if it is not generic.
    if current_title and not is_generic(current_title):
        texts.append(("current_title",current_title))

    # Use filename and source page slug as fallback.
    texts.append(("filename",filename(audio_url)))
    texts.append(("source_slug",slug_text(source_page)))

    guesses=[]

    for source,text in texts:
        if not text:
            continue

        for fn in [guess_from_triple_dash_filename, guess_from_by, guess_from_dash, guess_from_known_prefix]:
            g=fn(text if fn != guess_from_triple_dash_filename else audio_url)
            if g:
                artist,title,conf,reason=g
                guesses.append({
                    "artist":artist,
                    "title":title,
                    "confidence":conf,
                    "reason":reason,
                    "source":source,
                    "raw":text,
                })

    if not guesses:
        return None

    guesses.sort(key=lambda g: (
        0 if g["confidence"]=="high" else 1,
        0 if g["source"]=="current_title" else 1,
    ))

    return guesses[0]

def main():
    con=sqlite3.connect(DB)
    con.row_factory=sqlite3.Row

    # Learn artists already successfully parsed.
    for r in con.execute("SELECT DISTINCT artist FROM tracks WHERE artist IS NOT NULL AND artist!='' AND artist!='UNKNOWN'"):
        KNOWN_ARTISTS.add(clean(r["artist"]).lower())

    rows=con.execute("""
      SELECT id, artist, title, audio_url, source_page
      FROM tracks
      WHERE artist='UNKNOWN' OR artist='' OR artist IS NULL
      ORDER BY id
    """).fetchall()

    fixed=0
    review=[]

    for r in rows:
        g=best_guess(r)

        if g and g["confidence"]=="high":
            con.execute("UPDATE tracks SET artist=?, title=? WHERE id=?",(g["artist"],g["title"],r["id"]))
            fixed+=1
            print(f"FIXED {r['id']}: {r['artist']} | {r['title']}  =>  {g['artist']} | {g['title']} [{g['reason']}]")
        else:
            review.append({
                "id":r["id"],
                "old_artist":r["artist"],
                "old_title":r["title"],
                "suggested_artist":g["artist"] if g else "",
                "suggested_title":g["title"] if g else "",
                "confidence":g["confidence"] if g else "none",
                "reason":g["reason"] if g else "no_guess",
                "audio_url":r["audio_url"],
                "source_page":r["source_page"],
            })

    con.commit()
    con.close()

    with open(REVIEW,"w",newline="",encoding="utf-8") as f:
        fields=["id","old_artist","old_title","suggested_artist","suggested_title","confidence","reason","audio_url","source_page"]
        w=csv.DictWriter(f,fieldnames=fields)
        w.writeheader()
        w.writerows(review)

    print("")
    print(f"Unknown rows checked: {len(rows)}")
    print(f"High-confidence fixes applied: {fixed}")
    print(f"Rows needing manual review: {len(review)}")
    print(f"Review CSV: {REVIEW}")

if __name__=="__main__":
    main()

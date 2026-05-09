#!/usr/bin/env python3
import csv, os, re, sqlite3
from datetime import datetime, timezone
from urllib.parse import urlparse, unquote

DB="/opt/radio-monitor/data/fingerprint-index/zambian_fingerprint_index.db"
REVIEW="/opt/radio-monitor/data/fingerprint-index/metadata_auto_review.csv"

EXTRA_KNOWN = """
yo maps
macky 2
chef 187
slapdee
slap dee
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
feligo de future
godelive
meddy
star boy
philezee chibeta
sido man
soka boy
sperk
zaubo
zosi power
d bwoy telem
daysotar
don t
guvna b
ghetto veterans
rich bizzy
msanene
daway
afflatus
moses sakala
angela nyirenda
sakala brothers
blood kid yvok
cimzie luniz
daxon ma africa
dear john
bigger waves
vinchenzo
vibro
vjeezy
mampi
digo
towela
kingston
boydoh
arthur bizzy
super boy sypee
t rayz
gass boy
ba nzovu
kelly ganster
vf cent
v cent
mr p
2 boys
kopala dangote
man chile
""".strip().splitlines()

GENERIC = [
    "top downloads", "ghana music", "gospel", "2026 all ngoni",
    "all ngoni", "ncwala songs", "zambian music", "latest",
    "download", "mp3", "official music video", "official audio"
]

def now():
    return datetime.now(timezone.utc).isoformat()

def clean(s):
    s = unquote(s or "")
    s = s.replace("_", " ").replace("+", " ")
    s = s.replace("“", "'").replace("”", "'").replace("‘", "'").replace("’", "'")
    s = re.sub(r"\.(mp3|wav|m4a|aac|ogg|flac)$", "", s, flags=re.I)
    s = re.sub(r"\?.*$", "", s)
    s = re.sub(r"\b(official music video|official video|official audio|music video|download|mp3|audio songforyou)\b", "", s, flags=re.I)
    s = re.sub(r"\bZHZ\b", "", s, flags=re.I)
    s = re.sub(r"-?ZHZ$", "", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -–—_|:•.'\"")

def nice(s):
    s = clean(s)
    out = []
    for w in s.split():
        lw = w.lower()
        if lw in ["ft", "feat", "featuring"]:
            out.append("ft")
        elif lw in ["dj", "dm"]:
            out.append(lw.upper())
        elif lw in ["x", "&"]:
            out.append(w)
        else:
            out.append(w[:1].upper() + w[1:])
    return " ".join(out)

def is_generic(s):
    low = clean(s).lower()
    return (not low) or any(g in low for g in GENERIC)

def filename(url):
    return os.path.basename(urlparse(url or "").path)

def filename_text(url):
    raw = unquote(filename(url))
    raw = re.sub(r"\.(mp3|wav|m4a|aac|ogg|flac)$", "", raw, flags=re.I)
    raw = re.sub(r"-?ZHZ$", "", raw, flags=re.I)
    raw = re.sub(r"[-_ ]?(prod|prod\.|produced)[-_ ]?(by)?[-_ ].*$", "", raw, flags=re.I)
    raw = raw.replace("---", " - ").replace("--", " - ")
    raw = raw.replace("_", "-")
    return raw.strip(" -_.")

def source_slug(url):
    path = urlparse(url or "").path.strip("/")
    slug = path.split("/")[-1] if path else ""
    slug = unquote(slug)
    slug = slug.replace("---", " - ").replace("--", " - ")
    slug = slug.replace("_", "-")
    return slug.strip(" -_.")

def slug_words(s):
    return clean((s or "").replace("-", " "))

def primary_artist(s):
    return re.split(r"\s+(ft|feat|featuring)\s+", clean(s), flags=re.I)[0].strip()

def add_cols(con):
    cols = [r[1] for r in con.execute("PRAGMA table_info(tracks)").fetchall()]
    for col in ["metadata_status", "metadata_confidence", "metadata_source", "metadata_notes", "metadata_cleaned_at"]:
        if col not in cols:
            con.execute(f"ALTER TABLE tracks ADD COLUMN {col} TEXT")
    con.commit()

def build_known(con):
    known = set(x.strip().lower() for x in EXTRA_KNOWN if x.strip())
    for r in con.execute("""
        SELECT DISTINCT artist FROM tracks
        WHERE artist IS NOT NULL AND artist!='' AND artist!='UNKNOWN'
    """):
        a = clean(r[0]).lower()
        if a:
            known.add(a)
            known.add(primary_artist(a).lower())
    return known

def known_prefix(raw, known):
    low = slug_words(raw).lower()
    for artist in sorted(known, key=len, reverse=True):
        if low.startswith(artist + " "):
            rest = clean(low[len(artist):])
            if rest:
                return artist, rest
    return None

def infer_article_style(text, source_name):
    raw = unquote(text or "")
    raw = raw.replace("“", "'").replace("”", "'").replace("‘", "'").replace("’", "'")
    raw = re.sub(r"[-_]+", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" -–—_|:•.\"")
    if not raw:
        return None

    # Artist drops 'Song' featuring Feature
    m = re.match(r"^(.+?)\s+drops\s+'?([^']+?)'?\s+featuring\s+(.+?)$", raw, flags=re.I)
    if m:
        artist = clean(m.group(1))
        title = clean(m.group(2))
        feature = clean(m.group(3))
        if artist and title and feature:
            return nice(f"{artist} ft {feature}"), nice(title), "high", f"{source_name}_article_drops_featuring"

    # Artist enlists Feature for 'Song'
    m = re.match(r"^(.+?)\s+enlists\s+(.+?)\s+for\s+'?([^']+?)'?$", raw, flags=re.I)
    if m:
        artist = clean(m.group(1))
        feature = clean(m.group(2))
        title = clean(m.group(3))
        if artist and title and feature:
            return nice(f"{artist} ft {feature}"), nice(title), "high", f"{source_name}_article_enlists_for"

    # Artist / Artists team up for 'Song'
    m = re.match(r"^(.+?)\s+team[s]?\s+up\s+for\s+'?([^']+?)'?$", raw, flags=re.I)
    if m:
        artist = clean(m.group(1))
        title = clean(m.group(2))
        if artist and title:
            return nice(artist), nice(title), "high", f"{source_name}_article_team_up_for"

    # Artists unite for love song 'Song'
    m = re.match(r"^(.+?)\s+unite\s+for\s+(?:love\s+song\s+)?'?([^']+?)'?$", raw, flags=re.I)
    if m:
        artist = clean(m.group(1))
        title = clean(m.group(2))
        if artist and title:
            return nice(artist), nice(title), "high", f"{source_name}_article_unite_for"

    return None

def infer_from_text(raw, known, source_name):
    raw = (raw or "").strip()
    if not raw:
        return None

    article = infer_article_style(raw, source_name)
    if article:
        return article

    # Strong separator: Artist - Title
    if " - " in raw:
        parts = [clean(p.replace("-", " ")) for p in raw.split(" - ") if clean(p)]
        if len(parts) >= 2:
            left, right = parts[0], " - ".join(parts[1:])
            if primary_artist(left).lower() in known:
                return nice(left), nice(right), "high", f"{source_name}_artist_title_separator"
            if primary_artist(right).lower() in known:
                return nice(right), nice(left), "high", f"{source_name}_title_artist_separator"

    # Known artist prefix
    kp = known_prefix(raw, known)
    if kp:
        artist, rest = kp
        m = re.search(r"^(.*?)\s+(ft|feat|featuring)\s+(.+)$", rest, flags=re.I)
        if m:
            song = clean(m.group(1))
            feature = clean(m.group(3))
            if song and feature:
                return nice(f"{artist} ft {feature}"), nice(song), "high", f"{source_name}_artist_song_ft_feature"
        return nice(artist), nice(rest), "high", f"{source_name}_known_artist_prefix"

    return None

def infer(row, known):
    old_artist = clean(row["artist"])
    old_title = clean(row["title"])

    candidates = [
        ("current_title", old_title),
        ("filename", filename_text(row["audio_url"] or "")),
        ("source_page", source_slug(row["source_page"] or "")),
    ]

    for source_name, raw in candidates:
        g = infer_from_text(raw, known, source_name)
        if g:
            return g

    if old_artist.upper() == "UNKNOWN" and old_title and not is_generic(old_title):
        return "UNKNOWN", nice(old_title), "title_only", "clean_title_artist_unknown"

    title_guess = slug_words(filename_text(row["audio_url"] or ""))
    if title_guess and len(title_guess.split()) <= 4:
        return "UNKNOWN", nice(title_guess), "title_only", "short_filename_title_only"

    return None

def main():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    add_cols(con)
    known = build_known(con)

    rows = con.execute("""
        SELECT id, artist, title, audio_url, source_page
        FROM tracks
        WHERE status='indexed'
        ORDER BY id
    """).fetchall()

    fixed = 0
    title_only = 0
    review = []

    for r in rows:
        old_artist = clean(r["artist"])
        old_title = clean(r["title"])

        needs = (
            old_artist.upper() == "UNKNOWN"
            or is_generic(old_artist)
            or is_generic(old_title)
        )

        if not needs:
            continue

        g = infer(r, known)

        if g and g[2] == "high" and g[0] != "UNKNOWN":
            artist, title, conf, reason = g
            con.execute("""
                UPDATE tracks
                SET artist=?, title=?,
                    metadata_status='auto_cleaned',
                    metadata_confidence='high',
                    metadata_source='auto_cleaner',
                    metadata_notes=?,
                    metadata_cleaned_at=?
                WHERE id=?
            """, (artist, title, reason, now(), r["id"]))
            fixed += 1
            print(f"AUTO FIXED {r['id']}: {artist} - {title} [{reason}]")

        elif g and g[2] == "title_only":
            artist, title, conf, reason = g
            con.execute("""
                UPDATE tracks
                SET title=?,
                    metadata_status='needs_artist_review',
                    metadata_confidence='title_only',
                    metadata_source='auto_cleaner',
                    metadata_notes=?,
                    metadata_cleaned_at=?
                WHERE id=?
            """, (title, reason, now(), r["id"]))
            title_only += 1
            review.append((r, g))

        else:
            con.execute("""
                UPDATE tracks
                SET metadata_status='needs_review',
                    metadata_confidence='low',
                    metadata_source='auto_cleaner',
                    metadata_notes='no_safe_guess',
                    metadata_cleaned_at=?
                WHERE id=?
            """, (now(), r["id"]))
            review.append((r, ("", "", "low", "no_safe_guess")))

    con.commit()

    with open(REVIEW, "w", newline="", encoding="utf-8") as f:
        fields = ["id","old_artist","old_title","suggested_artist","suggested_title","confidence","reason","file","audio_url","source_page"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r, g in review:
            w.writerow({
                "id": r["id"],
                "old_artist": r["artist"],
                "old_title": r["title"],
                "suggested_artist": g[0],
                "suggested_title": g[1],
                "confidence": g[2],
                "reason": g[3],
                "file": filename(r["audio_url"]),
                "audio_url": r["audio_url"],
                "source_page": r["source_page"],
            })

    con.close()

    print("")
    print(f"Auto high-confidence fixes applied: {fixed}")
    print(f"Title-only cleanups: {title_only}")
    print(f"Rows needing review: {len(review)}")
    print(f"Review CSV: {REVIEW}")

if __name__ == "__main__":
    main()

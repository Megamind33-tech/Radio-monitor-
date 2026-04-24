/**
 * music-content-filter.ts
 * -----------------------
 * Classifies a stream title as MUSIC or NON-MUSIC (show, news, ad, talk, jingle, etc.).
 *
 * Design goals:
 *  - Fast (regex / string ops only, no network).
 *  - Conservative: prefer false-positive music over false-positive non-music so we
 *    never silence a real song.
 *  - Returns a reason code so callers can log why content was skipped.
 *
 * Used by MonitorService to avoid logging shows/programs as songs and to avoid
 * wasting AcoustID quota on non-music content.
 */

export interface ContentFilterResult {
  isMusic: boolean;
  confidence: number; // 0 (definitely not music) … 1 (definitely music)
  reason: string;     // human-readable label for logging
}

// ---------------------------------------------------------------------------
// Non-music keyword patterns (all checked case-insensitively)
// ---------------------------------------------------------------------------

const NON_MUSIC_EXACT: Set<string> = new Set([
  "live", "on air", "news", "advertisement", "adverts", "ad break", "ads",
  "commercial", "commercials", "jingle", "jingles", "station id", "sign on",
  "sign off", "test tone", "prayer", "promo", "promotion", "break",
  "offline", "connecting", "buffering", "loading",
]);

/**
 * Phrase fragments that strongly indicate NON-music content.
 * Matched anywhere in the title (substring, case-insensitive).
 */
const NON_MUSIC_FRAGMENTS: readonly string[] = [
  // Show / programme labels
  "morning show", "afternoon show", "evening show", "breakfast show",
  "drive show", "drive time", "drivetime", "night show", "midnight show",
  "weekend show", "talk show", "chat show", "phone-in", "call-in",
  "on the couch", "studio session", "exclusive session",
  "triple threat", "playlist with dj", "dj online", "with dj ",
  // News / current affairs
  "news update", "news bulletin", "news hour", "headline", "weather report",
  "traffic update", "sports report", "sports update", "sports news",
  "match preview", "match report", "commentary",
  "number one for", "hits uninterrupted",
  "radio is our business", "now playing info", "goes here",
  // Religious / church
  "sunday service", "church service", "bible study", "devotion",
  "morning devotion", "daily devotion", "gospel hour",
  "corinthians", "all chapters", "confession final",
  // Greetings / openers
  "good morning", "good afternoon", "good evening", "welcome to",
  "join us", "stay tuned", "coming up", "after the break",
  "rise, learn, and shine",
  // Station IDs / branding phrases (> 3 words suggests branding, not a song)
  "radio zambia", "znbc radio", "radio phoenix", "radio mano",
  "hot fm", "muvi tv", "diamond tv",
  "sun fm tv", "kwacha radio",
  // Ads / sponsored content
  "sponsored by", "brought to you", "advertisement", "advert",
  "commercial break",
  "attacking the power source",
];

/**
 * Regex patterns that match non-music content.
 * Checked against the full lowercased title.
 */
const NON_MUSIC_PATTERNS: readonly RegExp[] = [
  // "Morning Show with DJ ..."  /  "Evening Drive with ..."
  /\b(morning|afternoon|evening|breakfast|drivetime|drive[- ]time|weekend|night)\s+(show|drive|edition|programme|program)\b/i,
  // "News at ..." / "5 O'Clock News"
  /\bnews\s+(at|at the|bulletin|hour|update|break)\b/i,
  /\b\d+\s*(o'?clock|pm|am)\s+(news|bulletin)\b/i,
  // "Talk with ..." / "In conversation with ..."
  /\b(talk|in conversation)\s+with\b/i,
  // Purely numeric/technical content
  /^[\d\s:./]+$/,
  // Only station name pattern: "ZNBC Radio 1" / "Hot 100.5 FM" (short, no separator, has FM/Radio)
  /^[\w\s.]+\b(fm|radio|am|tv|mhz|khz)\b[\w\s.]*$/i,
  // Empty or whitespace-only
  /^\s*$/,
  // Pure dash / ellipsis (no real content)
  /^[-–—.… ]+$/,
  // Dated show titles: "KHASU LAKALE 12TH NOVEMBER 2025"
  /\b\d{1,2}(st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4}\b/i,
  // Parenthetical chapter counts: "(8) 2 Corinthians"
  /^\(\s*\d+\s*\)\s*\d+\s+[A-Za-z]/,
  // Encoded / opaque IDs (e.g. FOTMUL709-e0d1aWQ)
  /[a-z]{4,}\d+[._-][a-z0-9]{8,}/i,
  /\b[a-z]{2,}\d{3,}-[a-f0-9]{8,}\b/i,
  // Football-style fixture lines (e.g. "NKANA FC vs KONKOLA BLADES FC")
  /\bfc\s+vs\s+/i,
  /\bvs\s+.{0,40}\bfc\b/i,
];

// ---------------------------------------------------------------------------
// Music positive signals
// ---------------------------------------------------------------------------

/**
 * Presence of any of these fragments strongly suggests the text IS a song entry.
 */
const MUSIC_SIGNALS: readonly string[] = [
  " ft. ", " ft.", " feat. ", " feat.", "(feat.", "(ft.",
  " x ", " vs ", " & ",  // collaboration patterns common in Zambian music
];

const MUSIC_PATTERNS: readonly RegExp[] = [
  // "Artist - Song Title"  or  "Artist / Song Title"
  /^.+\s[-–—/]\s.+$/,
  // "Song Title (feat. Artist)"
  /\(feat\./i,
  /\(ft\./i,
  // Year in parentheses — common in re-releases: "Song Title (2023)"
  /\(\d{4}\)/,
];

// ---------------------------------------------------------------------------
// Public classifier
// ---------------------------------------------------------------------------

export function classifyMusicContent(rawTitle: string | null | undefined): ContentFilterResult {
  const title = (rawTitle ?? "").trim();

  if (!title) {
    return { isMusic: false, confidence: 0, reason: "empty_title" };
  }

  if (title.length < 2) {
    return { isMusic: false, confidence: 0.05, reason: "too_short" };
  }

  const lower = title.toLowerCase();

  // 1. Exact match against known non-music tokens.
  if (NON_MUSIC_EXACT.has(lower)) {
    return { isMusic: false, confidence: 0.02, reason: "non_music_exact" };
  }

  // 2. Positive music signals — checked early so a song called "Morning Show Banger feat. X"
  //    isn't incorrectly rejected by the non-music fragment check below.
  for (const signal of MUSIC_SIGNALS) {
    if (lower.includes(signal.toLowerCase())) {
      return { isMusic: true, confidence: 0.9, reason: "music_signal_feat_collab" };
    }
  }
  for (const re of MUSIC_PATTERNS) {
    if (re.test(title)) {
      return { isMusic: true, confidence: 0.85, reason: "music_pattern_separator" };
    }
  }

  // 3. Non-music fragment scan.
  for (const frag of NON_MUSIC_FRAGMENTS) {
    if (lower.includes(frag)) {
      return { isMusic: false, confidence: 0.08, reason: `non_music_fragment:${frag}` };
    }
  }

  // 4. Non-music regex patterns.
  for (const re of NON_MUSIC_PATTERNS) {
    if (re.test(title)) {
      return { isMusic: false, confidence: 0.1, reason: `non_music_pattern:${re.source.slice(0, 40)}` };
    }
  }

  // 5. Default: assume music (conservative — we'd rather log an unknown programme
  //    than silently drop a real song).
  return { isMusic: true, confidence: 0.65, reason: "default_assume_music" };
}

/**
 * Quick boolean helper — drop-in replacement for the old `isProgramLikeTitle()`.
 */
export function isProgramContent(rawTitle: string | null | undefined): boolean {
  return !classifyMusicContent(rawTitle).isMusic;
}

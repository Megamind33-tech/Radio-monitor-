/**
 * Classify mount URLs into direct / CDN / relay / aggregator tiers for source preference.
 * Heuristic only — does not reject streams; scoring favors likely first-party endpoints.
 */

export function effectiveMountUrl(streamUrl: string, preferredStreamUrl?: string | null): string {
  const p = String(preferredStreamUrl ?? "").trim();
  return p || streamUrl;
}

export type StreamSourceTier = "direct" | "official_cdn" | "relay" | "aggregator" | "unknown";

export type StreamSourceClass = {
  tier: StreamSourceTier;
  qualityScore: number; // 0-100, higher = prefer for monitoring
  hints: string[];
};

const AGGREGATOR_HOSTS =
  /\b(tunein|streema|radio-browser|zeno\.fm|radiogarden|onlineradiobox|mytuner|streamtheworld|fastcast4u|cast\.fm|shoutcast|icecast\.org\/yp)\b/i;

const RELAY_HINTS =
  /\b(relay|restream|mirror|proxy\/|\/proxy\/|aggregator|listen\.|stream\.?port|allzic)\b/i;

const CDN_FIRST_PARTY =
  /\b(akamai|cloudfront|fastly|bunnycdn|keycdn|azureedge|googleusercontent|hwcdn)\b/i;

/** Official-ish mount paths (still heuristic). */
const DIRECT_MOUNT = /\.(mp3|aac|m4a|ogg)(\?|$)/i;

export function classifyStreamUrl(url: string, stationName?: string | null): StreamSourceClass {
  const raw = String(url || "").trim();
  const hints: string[] = [];
  if (!raw) {
    return { tier: "unknown", qualityScore: 30, hints: ["empty_url"] };
  }

  let lower = "";
  try {
    lower = new URL(raw).hostname.toLowerCase() + new URL(raw).pathname.toLowerCase();
  } catch {
    lower = raw.toLowerCase();
  }

  let score = 55;
  let tier: StreamSourceTier = "unknown";

  if (AGGREGATOR_HOSTS.test(lower)) {
    tier = "aggregator";
    score = 28;
    hints.push("aggregator_host");
  } else if (RELAY_HINTS.test(lower)) {
    tier = "relay";
    score = 42;
    hints.push("relay_path");
  } else if (CDN_FIRST_PARTY.test(lower)) {
    tier = "official_cdn";
    score = 72;
    hints.push("major_cdn");
  } else if (DIRECT_MOUNT.test(raw) || /\.(pls|m3u8?)(\?|$)/i.test(raw)) {
    tier = "direct";
    score = 88;
    hints.push("direct_file_or_playlist");
  } else {
    tier = "direct";
    score = 68;
    hints.push("default_treat_as_direct_mount");
  }

  if (tier === "direct" && RELAY_HINTS.test(lower)) {
    tier = "relay";
    score = Math.min(score, 48);
    hints.push("relay_override");
  }

  if (stationName) {
    const norm = stationName.toLowerCase().replace(/[^a-z0-9]+/g, "");
    const host = lower.split("/")[0] || "";
    if (norm.length >= 6 && host.replace(/[^a-z0-9]/g, "").includes(norm.slice(0, 8))) {
      score = Math.min(100, score + 8);
      hints.push("hostname_station_hint");
    }
  }

  return { tier, qualityScore: Math.max(0, Math.min(100, score)), hints };
}

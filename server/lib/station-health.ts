import { StationContentClassification, StationMonitorState, StreamHealthSnapshot } from "../types.js";

export function classifyContent(metadataText: string | null | undefined): StationContentClassification {
  const t = String(metadataText ?? "").trim().toLowerCase();
  if (!t) return "unknown";

  const adHints = [
    "advert",
    "sponsor",
    "commercial break",
    "ad break",
    "your ad here",
    "paid promotion",
  ];
  if (adHints.some((h) => t.includes(h))) return "ads";

  const speechHints = [
    "call in",
    "call-in",
    "phone in",
    "listener",
    "sermon",
    "prayer",
    "preaching",
    "mass ",
    "church",
    "live speech",
    "political",
    "debate",
    "election",
  ];
  const talkHints = [
    "talk",
    "news",
    "program",
    "discussion",
    "interview",
    "sports",
    "commentary",
    "banter",
    "morning show",
    "drive time",
  ];
  const musicHints = [" - ", " feat ", " remix", "album", "track", "dj ", "artist"];

  const speechHit = speechHints.some((h) => t.includes(h));
  const talkHit = talkHints.some((h) => t.includes(h)) || speechHit;
  const musicHit = musicHints.some((h) => t.includes(h));

  if (talkHit && musicHit) return "mixed";
  if (speechHit && !musicHit) return "unknown_speech";
  if (talkHit) return "talk";
  if (musicHit) return "music";
  return "unknown";
}

/**
 * Online/offline follows **transport + audio bytes only** — not decoder/ffprobe and not song ID.
 * INACTIVE only when the stream is unreachable or not delivering audio bytes, sustained.
 */
export function deriveMonitorState(input: {
  health: StreamHealthSnapshot;
  contentClassification: StationContentClassification;
  hasReliableMatch: boolean;
  consecutiveFailures: number;
  failureThreshold: number;
}): { state: StationMonitorState; reason: string } {
  const { health, contentClassification, hasReliableMatch, consecutiveFailures, failureThreshold } = input;

  const transportOk = health.reachable && health.audioFlowing;
  const transportFailed = !health.reachable || !health.audioFlowing;

  if (transportFailed) {
    if (consecutiveFailures >= failureThreshold) {
      return { state: "INACTIVE", reason: health.reason || "transport_failure_threshold" };
    }
    return { state: "DEGRADED", reason: health.reason || "transport_degraded" };
  }

  // Bytes flowing: stay online even if ffprobe/decode is flaky (fingerprint may be weaker only).
  if (!health.decoderOk || health.degraded) {
    return { state: "DEGRADED", reason: health.reason || "decode_degraded_audio_flowing" };
  }

  const talkLike =
    contentClassification === "talk" ||
    contentClassification === "ads" ||
    contentClassification === "unknown_speech" ||
    contentClassification === "mixed";

  if (talkLike) {
    return { state: "ACTIVE_TALK", reason: "healthy_non_music_content_window" };
  }

  if (hasReliableMatch) {
    return { state: "ACTIVE_MUSIC", reason: "healthy_with_match" };
  }

  return { state: "ACTIVE_NO_MATCH", reason: "healthy_without_match" };
}

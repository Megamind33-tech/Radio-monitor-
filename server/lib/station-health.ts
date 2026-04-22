import { StationContentClassification, StationMonitorState, StreamHealthSnapshot } from "../types.js";

export function classifyContent(metadataText: string | null | undefined): StationContentClassification {
  const t = String(metadataText ?? "").trim().toLowerCase();
  if (!t) return "unknown";

  const talkHints = [
    "talk",
    "news",
    "sermon",
    "preaching",
    "program",
    "discussion",
    "interview",
    "sports",
    "commentary",
  ];
  const musicHints = [" - ", " feat ", " remix", "album", "track", "dj ", "artist"];

  const talk = talkHints.some((h) => t.includes(h));
  const music = musicHints.some((h) => t.includes(h));

  if (talk && music) return "mixed";
  if (talk) return "talk";
  if (music) return "music";
  return "unknown";
}

export function deriveMonitorState(input: {
  health: StreamHealthSnapshot;
  contentClassification: StationContentClassification;
  hasReliableMatch: boolean;
  consecutiveFailures: number;
  failureThreshold: number;
}): { state: StationMonitorState; reason: string } {
  const { health, contentClassification, hasReliableMatch, consecutiveFailures, failureThreshold } = input;

  if (!health.reachable || !health.audioFlowing || !health.decoderOk) {
    if (consecutiveFailures >= failureThreshold) {
      return { state: "INACTIVE", reason: health.reason || "health_failure_threshold_exceeded" };
    }
    return { state: "DEGRADED", reason: health.reason || "health_partial_failure" };
  }

  if (health.degraded) {
    return { state: "DEGRADED", reason: health.reason || "health_degraded" };
  }

  if (contentClassification === "talk") {
    return { state: "ACTIVE_TALK", reason: "healthy_talk_content" };
  }

  if (hasReliableMatch) {
    return { state: "ACTIVE_MUSIC", reason: "healthy_with_match" };
  }

  return { state: "ACTIVE_NO_MATCH", reason: "healthy_without_match" };
}

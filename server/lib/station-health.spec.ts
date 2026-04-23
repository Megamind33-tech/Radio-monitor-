import { classifyContent, deriveMonitorState } from "./station-health.js";
import type { StreamHealthSnapshot } from "../types.js";

function healthySnapshot(): StreamHealthSnapshot {
  return {
    reachable: true,
    audioFlowing: true,
    decoderOk: true,
    degraded: false,
    reason: null,
    resolvedUrl: "https://stream.example/live",
    contentTypeHeader: "audio/mpeg",
  };
}

function assert(condition: boolean, message: string) {
  if (!condition) {
    throw new Error(message);
  }
}

function run() {
  // Content classification
  assert(classifyContent("Morning talk show with host") === "talk", "talk metadata should classify as talk");
  assert(classifyContent("Artist - Track Title") === "music", "artist/title metadata should classify as music");
  assert(classifyContent("") === "unknown", "empty metadata should classify as unknown");

  // Healthy + matched => ACTIVE_MUSIC
  let state = deriveMonitorState({
    health: healthySnapshot(),
    contentClassification: "music",
    hasReliableMatch: true,
    consecutiveFailures: 0,
    failureThreshold: 3,
  });
  assert(state.state === "ACTIVE_MUSIC", "healthy matched stream should be ACTIVE_MUSIC");

  // Healthy talk should stay active talk.
  state = deriveMonitorState({
    health: healthySnapshot(),
    contentClassification: "talk",
    hasReliableMatch: false,
    consecutiveFailures: 0,
    failureThreshold: 3,
  });
  assert(state.state === "ACTIVE_TALK", "healthy talk stream should be ACTIVE_TALK");

  // Healthy no-match remains active.
  state = deriveMonitorState({
    health: healthySnapshot(),
    contentClassification: "unknown",
    hasReliableMatch: false,
    consecutiveFailures: 0,
    failureThreshold: 3,
  });
  assert(state.state === "ACTIVE_NO_MATCH", "healthy no-match stream should be ACTIVE_NO_MATCH");

  // Temporary failure below threshold => DEGRADED
  state = deriveMonitorState({
    health: { ...healthySnapshot(), reachable: false, audioFlowing: false, decoderOk: false, reason: "request_exception" },
    contentClassification: "unknown",
    hasReliableMatch: false,
    consecutiveFailures: 1,
    failureThreshold: 3,
  });
  assert(state.state === "DEGRADED", "early failures should be DEGRADED");

  // Sustained failure beyond threshold => INACTIVE
  state = deriveMonitorState({
    health: { ...healthySnapshot(), reachable: false, audioFlowing: false, decoderOk: false, reason: "request_exception" },
    contentClassification: "unknown",
    hasReliableMatch: false,
    consecutiveFailures: 3,
    failureThreshold: 3,
  });
  assert(state.state === "INACTIVE", "failure threshold should flip to INACTIVE");

  // Transport OK + audio bytes but decoder/ffprobe flaky => DEGRADED, never INACTIVE from song ID
  state = deriveMonitorState({
    health: {
      ...healthySnapshot(),
      decoderOk: false,
      degraded: true,
      reason: "decoder_timeout",
    },
    contentClassification: "unknown",
    hasReliableMatch: false,
    consecutiveFailures: 99,
    failureThreshold: 3,
  });
  assert(state.state === "DEGRADED", "decode-only issues must not mark INACTIVE");

  console.log("station-health.spec: ok");
}

run();

import { EventEmitter } from "events";

export interface SongDetectedEvent {
  stationId: string;
  detectionLogId: string;
  observedAt: string;
  title: string | null;
  artist: string | null;
  playCount: number;
}

/** Emitted after every successful poll so UIs can refresh without waiting for a new DetectionLog row. */
export interface StationPollEvent {
  stationId: string;
  ts: string;
  detectionStatus: "matched" | "unresolved";
  /** Latest log id when known (same-spin repeats reuse previous id). */
  detectionLogId: string | null;
  /** What to show as “now playing”: identified title or raw ICY title. */
  displayTitle: string | null;
  displayArtist: string | null;
  streamText: string | null;
  /** True when a new DetectionLog row was written this tick. */
  newDetectionLog: boolean;
}

class MonitorEvents extends EventEmitter {
  constructor() {
    super();
    // SSE listeners are dynamic and can exceed Node's default 10 listener warning.
    this.setMaxListeners(0);
  }

  emitSongDetected(payload: SongDetectedEvent): void {
    this.emit("song-detected", payload);
  }

  emitStationPoll(payload: StationPollEvent): void {
    this.emit("station-poll", payload);
  }
}

export const monitorEvents = new MonitorEvents();

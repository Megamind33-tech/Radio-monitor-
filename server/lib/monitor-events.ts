import { EventEmitter } from "events";

export interface SongDetectedEvent {
  stationId: string;
  detectionLogId: string;
  observedAt: string;
  title: string | null;
  artist: string | null;
  playCount: number;
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
}

export const monitorEvents = new MonitorEvents();

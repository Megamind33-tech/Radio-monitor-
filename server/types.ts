export type DetectionMethod = 'stream_metadata' | 'fingerprint_acoustid' | 'catalog_lookup' | 'unresolved';
export type DetectionStatus = 'matched' | 'unresolved' | 'ignored';
export type StationMonitorState =
  | 'ACTIVE_MUSIC'
  | 'ACTIVE_NO_MATCH'
  | 'ACTIVE_TALK'
  | 'DEGRADED'
  | 'INACTIVE'
  | 'UNKNOWN';

export type StationContentClassification = 'music' | 'talk' | 'mixed' | 'unknown';

export interface NormalizedMetadata {
  rawTitle?: string;
  rawArtist?: string;
  combinedRaw?: string;
  splitRuleApplied?: string;
  splitConfidence?: number;
  sourceType: DetectionMethod;
}

export interface MatchResult {
  score: number;
  recordingId?: string;
  title?: string;
  artist?: string;
  releaseTitle?: string;
  releaseDate?: string;
  isrcs?: string[];
  genre?: string;
  /** Track length in ms (MusicBrainz / iTunes / AcoustID) — used to suppress duplicate logs during one long spin */
  durationMs?: number;
  sourceProvider?:
    | 'acoustid'
    | 'acoustid_open'
    | 'musicbrainz'
    | 'musicbrainz_search'
    | 'itunes_search'
    | 'deezer_search'
    | 'theaudiodb_search'
    | 'stream_metadata'
    | 'recovery_reprocess';
  confidence: number;
  reasonCode?: string;
}

export interface FingerprintResult {
  duration: number;
  fingerprint: string;
  backendUsed: string;
}

export interface StreamHealthSnapshot {
  reachable: boolean;
  audioFlowing: boolean;
  decoderOk: boolean;
  degraded: boolean;
  reason: string | null;
  resolvedUrl: string;
  contentTypeHeader?: string | null;
  codec?: string | null;
  bitrate?: number | null;
  latencyMs?: number | null;
}

export type DetectionMethod =
  | 'stream_metadata'
  | 'fingerprint_acoustid'
  | 'fingerprint_audd'
  | 'fingerprint_acrcloud'
  | 'fingerprint_local'
  | 'catalog_lookup'
  | 'unresolved';
export type DetectionStatus = 'matched' | 'unresolved' | 'ignored';
export type StationMonitorState =
  | 'ACTIVE_MUSIC'
  | 'ACTIVE_NO_MATCH'
  | 'ACTIVE_TALK'
  | 'DEGRADED'
  | 'INACTIVE'
  | 'UNKNOWN';

export type StationContentClassification =
  | 'music'
  | 'talk'
  | 'ads'
  | 'unknown_speech'
  | 'mixed'
  | 'unknown';

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
  /** AcoustID top-level track UUID (not the MusicBrainz MBID). */
  acoustidTrackId?: string;
  title?: string;
  artist?: string;
  releaseTitle?: string;
  releaseDate?: string;
  isrcs?: string[];
  genre?: string;
  /** Full artist credit line (e.g. "A feat. B") when resolved from MusicBrainz. */
  displayArtist?: string;
  /** Track title with featured segment removed for display/export. */
  titleWithoutFeat?: string;
  /** Featured / collaborating artists parsed from credits or title. */
  featuredArtists?: string[];
  /** Primary release label from MusicBrainz when available. */
  labelName?: string;
  /** ISO 3166-1 alpha-2 from MusicBrainz release country when available. */
  countryCode?: string;
  /** Track length in ms (MusicBrainz / iTunes / AcoustID) — used to suppress duplicate logs during one long spin */
  durationMs?: number;
  sourceProvider?:
    | 'acoustid'
    | 'acoustid_open'
    | 'audd'
    | 'acrcloud'
    | 'local_fingerprint'
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

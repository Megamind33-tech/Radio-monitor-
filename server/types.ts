export type DetectionMethod = 'stream_metadata' | 'fingerprint_acoustid' | 'catalog_lookup' | 'unresolved';
export type DetectionStatus = 'matched' | 'unresolved' | 'ignored';

export interface NormalizedMetadata {
  rawTitle?: string;
  rawArtist?: string;
  combinedRaw?: string;
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
    | 'musicbrainz'
    | 'musicbrainz_search'
    | 'itunes_search'
    | 'deezer_search'
    | 'stream_metadata';
  confidence: number;
  reasonCode?: string;
}

export interface FingerprintResult {
  duration: number;
  fingerprint: string;
  backendUsed: string;
}

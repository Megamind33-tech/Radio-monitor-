export type DetectionMethod = 'stream_metadata' | 'fingerprint_acoustid' | 'unresolved';
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
  confidence: number;
  reasonCode?: string;
}

export interface FingerprintResult {
  duration: number;
  fingerprint: string;
  backendUsed: string;
}

export interface Station {
  id: string;
  name: string;
  country: string;
  district?: string;
  province?: string;
  frequencyMhz?: string | null;
  icyQualification?: string | null;
  streamUrl: string;
  preferredStreamUrl?: string | null;
  pollIntervalSeconds?: number;
  isActive: boolean;
  lastPollAt?: string | null;
  lastPollStatus?: string | null;
  lastPollError?: string | null;
  monitorState?: 'ACTIVE_MUSIC' | 'ACTIVE_NO_MATCH' | 'ACTIVE_TALK' | 'DEGRADED' | 'INACTIVE' | 'UNKNOWN';
  monitorStateReason?: string | null;
  contentClassification?: 'music' | 'talk' | 'ads' | 'unknown_speech' | 'mixed' | 'unknown';
  streamSourceType?: string | null;
  streamSourceQualityScore?: number | null;
  streamOnlineLast?: number | null;
  audioDetectedLast?: number | null;
  metadataAvailableLast?: number | null;
  songIdentifiedLast?: number | null;
  decodeHealthEma?: number | null;
  fingerprintHitEma?: number | null;
  metadataPresentEma?: number | null;
  visibilityEnabled?: boolean;
  lastHealthyAt?: string | null;
  lastGoodAudioAt?: string | null;
  lastSongDetectedAt?: string | null;
  streamRefreshedAt?: string | null;
  currentNowPlaying?: {
    title: string;
    artist: string;
    album?: string;
    genre?: string;
    sourceProvider?: string;
    updatedAt: string;
  };
}

export interface DetectionLog {
  id: string;
  stationId: string;
  observedAt: string;
  detectionMethod: string;
  artistFinal?: string;
  titleFinal?: string;
  releaseFinal?: string;
  genreFinal?: string;
  sourceProvider?: string;
  status: string;
  acoustidScore?: number;
  station?: {
    id: string;
    name: string;
    country: string;
  };
}

export interface Metrics {
  total_detections: number;
  match_rate: number;
  match_rate_24h?: number;
  detections_24h?: number;
  music_match_rate?: number;
  music_match_rate_24h?: number;
  music_detections?: number;
  music_matched?: number;
  music_detections_24h?: number;
  music_matched_24h?: number;
  errors_count: number;
  matched_by_detection_method_24h?: Record<string, number>;
  all_detections_by_detection_method_24h?: Record<string, number>;
  match_rate_note?: string;
}

export interface StationSpinSummary {
  stationId: string;
  uniqueSongs: number;
  detectionCount: number;
}

export interface SongSpinRow {
  stationId: string;
  artist: string | null;
  title: string | null;
  album: string | null;
  playCount: number;
  lastPlayed: string;
  firstPlayed: string;
  mixRuleApplied?: string | null;
  mixSplitConfidence?: number | null;
  originalCombinedRaw?: string | null;
}

export interface DependencyStatus {
  ffmpeg: boolean;
  ffprobe: boolean;
  fpcalc: boolean;
  acoustidApiKeyConfigured: boolean;
  musicbrainzUserAgentConfigured: boolean;
  catalogLookupReady: boolean;
  freeApisEnabled: {
    acoustid: boolean;
    musicbrainz: boolean;
    itunesSearch: boolean;
    deezerSearch?: boolean;
  };
  fingerprintReady: boolean;
  missing: string[];
  paidApis?: {
    auddConfigured: boolean;
    acrcloudConfigured: boolean;
    paidFallbacksEnabled: boolean;
    paidLaneReady: boolean;
  };
  integrationNotes?: string[];
}

export interface UnknownStorageStationRow {
  stationId: string;
  stationName: string;
  sampleCount: number;
  audioBytes: number;
  purgeEligibleCount: number;
  reclaimableBytes: number;
}

export interface UnknownStorageSummary {
  totalUnknownSampleCount: number;
  countWithAudioFile: number;
  countMissingAudioFile: number;
  totalAudioBytes: number;
  reviewedCount: number;
  humanVerifiedCount: number;
  fingerprintedCount: number;
  fingerprintFailedCount: number;
  eligibleForPurgeCount: number;
  notEligibleForPurgeCount: number;
  estimatedBytesReclaimable: number;
  byStation: UnknownStorageStationRow[];
}

export type StationListFilter = 'all' | 'running' | 'degraded' | 'inactive' | 'unknown';

export interface AudioEditorSample {
  id: string;
  stationId: string;
  stationName: string | null;
  stationCountry: string | null;
  stationProvince: string | null;
  detectionLogId: string | null;
  createdAt: string;
  recoveryStatus: string;
  recoveryAttempts: number;
  lastRecoveryAt: string | null;
  recoveredAt: string | null;
  lastRecoveryError: string | null;
  hasAudioFile: boolean;
  detectedAt: string | null;
  rawStreamText: string | null;
  parsedArtist: string | null;
  parsedTitle: string | null;
  reasonCode: string | null;
  titleFinal: string | null;
  artistFinal: string | null;
  releaseFinal: string | null;
  genreFinal: string | null;
  manuallyTagged: boolean;
  manualTaggedAt: string | null;
}

export interface StationUnknownSample {
  id: string;
  stationId: string;
  stationName: string | null;
  capturedAt: string;
  playedAt: string | null;
  createdAt: string;
  duration: number | null;
  hasAudio: boolean;
  fileAvailable: boolean;
  matchStatus: string;
  reviewStatus: string;
  metadataSource: string | null;
  rawMetadataText: string | null;
  suggestedArtist: string | null;
  suggestedTitle: string | null;
  confidence: number | null;
  audioUrl: string;
  fingerprintStatus?: string;
  linkedTrackId?: string | null;
}

import React, { useState, useEffect } from 'react';
import { 
  Radio, 
  Activity, 
  Settings, 
  History, 
  ArrowLeft,
  Plus, 
  RefreshCw, 
  ExternalLink,
  Search,
  Copy,
  CheckCircle2,
  AlertCircle,
  BarChart3,
  Download,
  Eye,
  MoreHorizontal,
  Globe,
  Music,
  LineChart
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';

// --- Types ---
interface Station {
  id: string;
  name: string;
  country: string;
  district?: string;
  province?: string;
  frequencyMhz?: string | null;
  icyQualification?: string | null;
  streamUrl: string;
  pollIntervalSeconds?: number;
  isActive: boolean;
  lastPollAt?: string | null;
  lastPollStatus?: string | null;
  lastPollError?: string | null;
  monitorState?: 'ACTIVE_MUSIC' | 'ACTIVE_NO_MATCH' | 'ACTIVE_TALK' | 'DEGRADED' | 'INACTIVE' | 'UNKNOWN';
  monitorStateReason?: string | null;
  contentClassification?: 'music' | 'talk' | 'mixed' | 'unknown';
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

interface DetectionLog {
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

interface Metrics {
  total_detections: number;
  match_rate: number;
  match_rate_24h?: number;
  detections_24h?: number;
  /** Matched ÷ detections excluding talk/program ICY noise (closer to “song ID” success). */
  music_match_rate?: number;
  music_match_rate_24h?: number;
  music_detections?: number;
  music_matched?: number;
  music_detections_24h?: number;
  music_matched_24h?: number;
  errors_count: number;
}

interface StationSpinSummary {
  stationId: string;
  uniqueSongs: number;
  detectionCount: number;
}

interface SongSpinRow {
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

interface DependencyStatus {
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
}

type StationListFilter = 'all' | 'running' | 'degraded' | 'inactive' | 'unknown';

const REQUESTED_STATION_PRIORITY: string[] = [
  'znbc radio 1',
  'znbc radio 2',
  'znbc radio 4',
  'hot fm',
  'hone fm',
  'yar fm',
  'chikuni',
  'phoenix',
  'faith radio',
  'chongwe radio',
  'ichengelo',
  'oblate radio',
  'unza',
  'zamcom',
  'kwithu',
  'rock fm',
  'money fm',
  'pan african radio',
  'komboni radio',
  'chimwemwe radio',
  'rooster fm',
  'power fm',
];

function normalizeStationName(value: string): string {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function formatMethod(method: string) {
  if (method === 'stream_metadata') return 'Metadata';
  if (method === 'fingerprint_acoustid') return 'AcoustID';
  if (method === 'fingerprint_local') return 'Local library';
  if (method === 'catalog_lookup') return 'Catalog';
  return method;
}

function parseStationHash(): string | null {
  if (typeof window === 'undefined') return null;
  const match = window.location.hash.match(/^#\/stations\/([^?]+)/);
  return match ? decodeURIComponent(match[1]) : null;
}

function setStationHash(stationId?: string) {
  if (typeof window === 'undefined') return;
  window.location.hash = stationId ? `#/stations/${encodeURIComponent(stationId)}` : '#/stations';
}

function stationGroup(station: Station): Exclude<StationListFilter, 'all'> {
  if (!station.isActive || station.monitorState === 'INACTIVE') return 'inactive';
  if (station.monitorState === 'DEGRADED') return 'degraded';
  if (!station.monitorState || station.monitorState === 'UNKNOWN') return 'unknown';
  return 'running';
}

// --- Components ---

export default function App() {
  const STATION_REFRESH_MS = 30_000;
  const HISTORY_REFRESH_MS = 120_000;
  const REALTIME_FALLBACK_REFRESH_MS = 15_000;
  const [stations, setStations] = useState<Station[]>([]);
  const [logs, setLogs] = useState<DetectionLog[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [dependencies, setDependencies] = useState<DependencyStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [selectedStationId, setSelectedStationId] = useState<string>('all');
  const [activeTab, setActiveTab] = useState<'stations' | 'history' | 'analytics' | 'settings'>('stations');
  const [isAddingStation, setIsAddingStation] = useState(false);
  const [addSubmitting, setAddSubmitting] = useState(false);
  const [addError, setAddError] = useState<string | null>(null);
  const [spinSummaries, setSpinSummaries] = useState<StationSpinSummary[]>([]);
  const [songSpins, setSongSpins] = useState<SongSpinRow[]>([]);
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [stationSearch, setStationSearch] = useState('');
  const [stationFilter, setStationFilter] = useState<StationListFilter>('all');
  const [stationPageId, setStationPageId] = useState<string | null>(() => parseStationHash());
  const [stationPageLogs, setStationPageLogs] = useState<DetectionLog[]>([]);
  const [stationPageSongSpins, setStationPageSongSpins] = useState<SongSpinRow[]>([]);
  const [stationPageLoading, setStationPageLoading] = useState(false);
  const [stationPageError, setStationPageError] = useState<string | null>(null);
  const [includeHiddenStations, setIncludeHiddenStations] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.localStorage.getItem('rm_include_hidden_stations') === '1';
  });

  const fetchData = React.useCallback(async () => {
    try {
      const stationsUrl = includeHiddenStations ? '/api/stations?visibility=all' : '/api/stations';
      const [stRes, metRes, spinRes] = await Promise.all([
        fetch(stationsUrl),
        fetch('/api/metrics/summary'),
        fetch('/api/analytics/station-summaries'),
      ]);
      const stData = await stRes.json();
      const metData = await metRes.json();
      const spinData = await spinRes.json();
      setStations(stData);
      setMetrics(metData);
      setSpinSummaries(Array.isArray(spinData) ? spinData : []);
    } catch (error) {
      console.error("Failed to fetch data", error);
    } finally {
      setLoading(false);
    }
  }, [includeHiddenStations]);

  const fetchSongAnalytics = async () => {
    setAnalyticsLoading(true);
    try {
      const res = await fetch('/api/analytics/songs?limit=800');
      const data = await res.json();
      setSongSpins(Array.isArray(data) ? data : []);
    } catch (e) {
      console.error(e);
    } finally {
      setAnalyticsLoading(false);
    }
  };

  const fetchLogs = async (stationId: string = selectedStationId) => {
    setHistoryLoading(true);
    try {
      const query = stationId === 'all' ? '' : `?stationId=${encodeURIComponent(stationId)}`;
      const res = await fetch(`/api/logs${query}`);
      const data = await res.json();
      setLogs(data);
    } catch (error) {
      console.error("Failed to fetch logs", error);
    } finally {
      setHistoryLoading(false);
    }
  };

  const fetchDependencies = async () => {
    try {
      const res = await fetch('/api/system/dependencies');
      const data = await res.json();
      setDependencies(data);
    } catch (error) {
      console.error("Failed to fetch dependency status", error);
    }
  };

  const fetchStationPageData = React.useCallback(async (stationId: string) => {
    setStationPageLoading(true);
    setStationPageError(null);
    try {
      const [logsRes, songsRes] = await Promise.all([
        fetch(`/api/stations/${encodeURIComponent(stationId)}/logs?take=300`),
        fetch(`/api/analytics/songs?stationId=${encodeURIComponent(stationId)}&limit=1200`),
      ]);
      if (!logsRes.ok) throw new Error(`Failed loading station logs (HTTP ${logsRes.status})`);
      if (!songsRes.ok) throw new Error(`Failed loading station songs (HTTP ${songsRes.status})`);
      const logsData = await logsRes.json();
      const songsData = await songsRes.json();
      setStationPageLogs(Array.isArray(logsData) ? logsData : []);
      setStationPageSongSpins(Array.isArray(songsData) ? songsData : []);
    } catch (error) {
      setStationPageError(error instanceof Error ? error.message : 'Failed loading station page data');
    } finally {
      setStationPageLoading(false);
    }
  }, []);

  useEffect(() => {
    const syncFromHash = () => {
      setStationPageId(parseStationHash());
    };
    if (typeof window !== 'undefined') {
      if (!window.location.hash) {
        setStationHash();
      } else {
        syncFromHash();
      }
      window.addEventListener('hashchange', syncFromHash);
      return () => {
        window.removeEventListener('hashchange', syncFromHash);
      };
    }
    return () => undefined;
  }, []);

  useEffect(() => {
    fetchData();
    fetchDependencies();
    fetchLogs('all');
    const interval = setInterval(fetchData, STATION_REFRESH_MS);
    return () => clearInterval(interval);
  }, [fetchData]);

  useEffect(() => {
    if (activeTab === 'analytics') {
      fetchSongAnalytics();
    }
  }, [activeTab]);

  useEffect(() => {
    if (!stationPageId || activeTab !== 'stations') return;
    fetchStationPageData(stationPageId);
    const interval = setInterval(() => {
      fetchStationPageData(stationPageId);
    }, HISTORY_REFRESH_MS);
    return () => clearInterval(interval);
  }, [stationPageId, activeTab, fetchStationPageData]);

  useEffect(() => {
    if (!stationPageId) {
      setStationPageLogs([]);
      setStationPageSongSpins([]);
      setStationPageError(null);
    }
  }, [stationPageId]);

  useEffect(() => {
    let cancelled = false;
    let es: EventSource | null = null;
    let fallbackTimer: ReturnType<typeof setInterval> | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;

    const refreshFromRealtime = (stationId?: string) => {
      if (cancelled) return;
      void fetchData();
      if (activeTab === 'history') {
        const targetStation = selectedStationId === 'all' ? undefined : selectedStationId;
        if (!targetStation || !stationId || targetStation === stationId) {
          void fetchLogs(selectedStationId);
        }
      }
      if (activeTab === 'analytics') {
        void fetchSongAnalytics();
      }
      if (activeTab === 'stations' && stationPageId && (!stationId || stationId === stationPageId)) {
        void fetchStationPageData(stationPageId);
      }
    };

    const connect = () => {
      if (cancelled) return;
      es = new EventSource('/api/events/monitoring');
      es.addEventListener('song_detected', (event) => {
        try {
          const payload = JSON.parse((event as MessageEvent).data) as { stationId?: string };
          refreshFromRealtime(payload.stationId);
        } catch {
          refreshFromRealtime();
        }
      });
      es.addEventListener('error', () => {
        es?.close();
        es = null;
        if (!fallbackTimer) {
          fallbackTimer = setInterval(() => {
            refreshFromRealtime();
          }, REALTIME_FALLBACK_REFRESH_MS);
        }
        if (!retryTimer) {
          retryTimer = setTimeout(() => {
            retryTimer = null;
            connect();
          }, 5000);
        }
      });
      es.addEventListener('open', () => {
        if (fallbackTimer) {
          clearInterval(fallbackTimer);
          fallbackTimer = null;
        }
      });
    };

    connect();
    return () => {
      cancelled = true;
      es?.close();
      if (fallbackTimer) clearInterval(fallbackTimer);
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [activeTab, selectedStationId, stationPageId, fetchStationPageData, fetchData]);

  useEffect(() => {
    if (activeTab !== 'history') return;
    fetchLogs(selectedStationId);
    const interval = setInterval(() => {
      fetchLogs(selectedStationId);
    }, HISTORY_REFRESH_MS);
    return () => clearInterval(interval);
  }, [activeTab, selectedStationId]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem('rm_include_hidden_stations', includeHiddenStations ? '1' : '0');
  }, [includeHiddenStations]);

  const stationNameById = new Map(stations.map((station) => [station.id, station.name]));
  const spinByStation = new Map(spinSummaries.map((s) => [s.stationId, s]));
  const monitoredCount = stations.filter((s) => s.isActive).length;
  const orderedStations = React.useMemo(() => {
    const rank = (station: Station): number => {
      const nameNorm = normalizeStationName(station.name || '');
      const idx = REQUESTED_STATION_PRIORITY.findIndex((token) =>
        nameNorm.includes(token)
      );
      return idx === -1 ? Number.MAX_SAFE_INTEGER : idx;
    };
    return [...stations].sort((a, b) => {
      const ra = rank(a);
      const rb = rank(b);
      if (ra !== rb) return ra - rb;
      return a.name.localeCompare(b.name);
    });
  }, [stations]);

  const stationById = new Map(orderedStations.map((station) => [station.id, station]));

  const stateCounts = React.useMemo(() => {
    const counts: Record<StationListFilter, number> = {
      all: orderedStations.length,
      running: 0,
      degraded: 0,
      inactive: 0,
      unknown: 0,
    };
    for (const station of orderedStations) {
      counts[stationGroup(station)] += 1;
    }
    return counts;
  }, [orderedStations]);

  const filteredStations = React.useMemo(() => {
    const searchQuery = stationSearch.trim().toLowerCase();
    return orderedStations.filter((station) => {
      if (stationFilter !== 'all' && stationGroup(station) !== stationFilter) {
        return false;
      }
      if (!searchQuery) return true;

      const haystack = [
        station.id,
        station.name,
        station.country,
        station.province || '',
        station.district || '',
        station.frequencyMhz || '',
      ]
        .join(' ')
        .toLowerCase();
      return haystack.includes(searchQuery);
    });
  }, [orderedStations, stationFilter, stationSearch]);

  const activeStation = stationPageId ? stationById.get(stationPageId) || null : null;

  return (
    <div className="min-h-screen bg-brand-bg text-gray-100 selection:bg-brand-cyan/30">
      {/* Sidebar / Nav */}
      <nav className="fixed left-0 top-0 h-full w-20 border-r border-white/5 bg-black/40 backdrop-blur-xl flex flex-col items-center py-8 gap-10 z-50">
        <div className="p-3 bg-brand-cyan/20 rounded-2xl border border-brand-cyan/30 shadow-[0_0_20px_rgba(0,242,255,0.2)]">
          <Radio className="w-8 h-8 text-brand-cyan" />
        </div>
        
        <div className="flex flex-col gap-6 flex-1">
          <NavIcon icon={<Activity className="w-6 h-6" />} active={activeTab === 'stations'} onClick={() => setActiveTab('stations')} label="Monitor" />
          <NavIcon icon={<History className="w-6 h-6" />} active={activeTab === 'history'} onClick={() => setActiveTab('history')} label="History" />
          <NavIcon icon={<LineChart className="w-6 h-6" />} active={activeTab === 'analytics'} onClick={() => setActiveTab('analytics')} label="Song spins" />
          <NavIcon icon={<Settings className="w-6 h-6" />} active={activeTab === 'settings'} onClick={() => setActiveTab('settings')} label="Settings" />
        </div>

        <button
          type="button"
          className="p-3 text-gray-500 hover:text-white transition-colors"
          onClick={() => setIsAddingStation(true)}
          aria-label="Add station"
        >
          <Plus className="w-6 h-6" />
        </button>
      </nav>

      {/* Main Content */}
      <main className="pl-28 pr-6 py-10 max-w-[1650px] mx-auto">
        {/* Header */}
        <header className="mb-8 flex flex-wrap justify-between items-end gap-6">
          <div>
            <h1 className="text-4xl font-bold tracking-tight mb-2 flex items-center gap-3">
              Radio Pulse <span className="text-brand-cyan">Monitor</span>
            </h1>
            <p className="text-gray-400">
              {activeTab === 'stations' && stationPageId
                ? 'Station profile, logs, and per-station export.'
                : 'Table-driven station operations and reliable song detection.'}
            </p>
          </div>
          
          <div className="flex gap-4">
            <MetricCard 
              label="Song match rate" 
              value={
                metrics
                  ? `${(
                      (metrics.music_match_rate_24h ?? metrics.music_match_rate ?? metrics.match_rate_24h ?? metrics.match_rate) * 100
                    ).toFixed(1)}%`
                  : '--'
              } 
              sub={
                metrics?.music_detections_24h != null && metrics.music_matched_24h != null
                  ? `Last 24h: ${metrics.music_matched_24h} matched / ${metrics.music_detections_24h} song attempts (talk/program ICY excluded)`
                  : metrics?.detections_24h
                    ? `Last 24h all logs: ${((metrics.match_rate_24h ?? metrics.match_rate) * 100).toFixed(1)}% (${metrics.detections_24h} rows)`
                    : 'Last 24h'
              }
            />
            <MetricCard 
              label="Monitoring" 
              value={monitoredCount.toString()} 
              sub="Active Zambia stations"
            />
          </div>
        </header>

        {activeTab === 'stations' && stationPageId && activeStation && (
          <StationDetailPage
            station={activeStation}
            spin={spinByStation.get(activeStation.id)}
            logs={stationPageLogs}
            songSpins={stationPageSongSpins}
            loading={stationPageLoading}
            error={stationPageError}
            onBack={() => setStationHash()}
            onRefreshAll={fetchData}
            onRefreshStation={() => fetchStationPageData(activeStation.id)}
          />
        )}

        {activeTab === 'stations' && stationPageId && !activeStation && (
          <div className="bg-white/5 border border-white/10 rounded-3xl p-10 text-center">
            <h2 className="text-xl font-semibold">Station page not found</h2>
            <p className="text-sm text-gray-400 mt-2">This station may have been removed or hidden.</p>
            <button
              type="button"
              onClick={() => setStationHash()}
              className="mt-5 px-4 py-2 rounded-xl border border-white/10 bg-black/30 hover:bg-black/50"
            >
              Back to station list
            </button>
          </div>
        )}

        {activeTab === 'stations' && !stationPageId && (
          <StationsManagementTable
            loading={loading}
            stations={filteredStations}
            spinByStation={spinByStation}
            stateCounts={stateCounts}
            stationSearch={stationSearch}
            stationFilter={stationFilter}
            onSearchChange={setStationSearch}
            onFilterChange={setStationFilter}
            onOpenStation={(id) => setStationHash(id)}
            onRefreshAll={fetchData}
          />
        )}

        {activeTab === 'analytics' && (
          <div className="bg-white/5 border border-white/10 rounded-3xl p-8 backdrop-blur-sm space-y-6">
            <div className="flex flex-wrap justify-between items-center gap-4">
              <div>
                <h2 className="text-xl font-semibold flex items-center gap-2">
                  <BarChart3 className="w-5 h-5 text-brand-cyan" />
                  Song spins (all stations)
                </h2>
                <p className="text-sm text-gray-500 mt-1">
                  Per-station CSV exports now live on each station page so song extraction is not one massive file.
                </p>
              </div>
              <button
                type="button"
                onClick={() => fetchSongAnalytics()}
                className="px-4 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50"
              >
                Refresh
              </button>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm table-fixed min-w-[980px]">
                <thead>
                  <tr className="border-b border-white/5 text-gray-500">
                    <th className="pb-3 font-medium w-[20%]">Station</th>
                    <th className="pb-3 font-medium w-[13%]">Province</th>
                    <th className="pb-3 font-medium w-[13%]">District</th>
                    <th className="pb-3 font-medium w-[20%]">Title</th>
                    <th className="pb-3 font-medium w-[16%]">Artist</th>
                    <th className="pb-3 font-medium w-[12%]">Album</th>
                    <th className="pb-3 font-medium text-right w-[6%]">Plays</th>
                  </tr>
                </thead>
                <tbody>
                  {analyticsLoading && songSpins.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="py-8 text-center text-gray-500">
                        Loading analytics…
                      </td>
                    </tr>
                  ) : (
                    songSpins.map((row, i) => {
                      const st = stations.find((s) => s.id === row.stationId);
                      return (
                        <tr key={`${row.stationId}-${row.title}-${i}`} className="border-b border-white/5 hover:bg-white/5">
                          <td className="py-3 font-medium truncate pr-2" title={st?.name ?? row.stationId}>{st?.name ?? row.stationId}</td>
                          <td className="py-3 text-gray-400 truncate pr-2" title={st?.province || '—'}>{st?.province || '—'}</td>
                          <td className="py-3 text-gray-400 truncate pr-2" title={st?.district || '—'}>{st?.district || '—'}</td>
                          <td className="py-3 truncate pr-2" title={row.title || '—'}>{row.title || '—'}</td>
                          <td className="py-3 text-gray-400 truncate pr-2" title={row.artist || '—'}>{row.artist || '—'}</td>
                          <td className="py-3 text-gray-500 truncate pr-2" title={row.album || '—'}>{row.album || '—'}</td>
                          <td className="py-3 text-right font-mono">{row.playCount}</td>
                        </tr>
                      );
                    })
                  )}
                  {!analyticsLoading && songSpins.length === 0 && (
                    <tr>
                      <td colSpan={7} className="py-8 text-center text-gray-500">
                        No matched detections yet. Leave the monitor running — spins appear as tracks are logged.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'history' && (
          <div className="bg-white/5 border border-white/10 rounded-3xl p-8 backdrop-blur-sm">
             <div className="flex justify-between items-center mb-6">
                <h2 className="text-xl font-semibold flex items-center gap-2">
                  <History className="w-5 h-5 text-brand-purple" />
                  Station Airplay Timeline
                </h2>
                <div className="flex gap-2">
                   <select
                      value={selectedStationId}
                      onChange={(event) => setSelectedStationId(event.target.value)}
                      className="bg-black/40 border border-white/10 rounded-lg px-3 py-1 text-sm outline-none"
                    >
                      <option value="all">All Stations</option>
                      {orderedStations.map((station) => <option key={station.id} value={station.id}>{station.name}</option>)}
                   </select>
                   <button
                    onClick={() => fetchLogs(selectedStationId)}
                    className="px-3 py-1 text-xs rounded-lg border border-white/10 bg-black/30 hover:bg-black/50 transition-colors"
                   >
                    Refresh
                   </button>
                </div>
             </div>
             
             <div className="overflow-x-auto">
                <table className="w-full text-left table-fixed min-w-[980px]">
                  <thead>
                    <tr className="border-b border-white/5 text-gray-500 text-sm">
                      <th className="pb-4 font-medium w-[18%]">Time</th>
                      <th className="pb-4 font-medium w-[20%]">Station</th>
                      <th className="pb-4 font-medium w-[42%]">Track Info</th>
                      <th className="pb-4 font-medium w-[12%]">Method</th>
                      <th className="pb-4 font-medium w-[8%]">Status</th>
                    </tr>
                  </thead>
                  <tbody className="text-sm">
                    {logs.map((log) => (
                      <tr key={log.id} className="border-b border-white/5 hover:bg-white/5 transition-colors group">
                        <td className="py-4 text-gray-400 whitespace-nowrap">{new Date(log.observedAt).toLocaleString()}</td>
                        <td className="py-4 font-medium truncate pr-2" title={log.station?.name || stationNameById.get(log.stationId) || "Unknown station"}>{log.station?.name || stationNameById.get(log.stationId) || "Unknown station"}</td>
                        <td className="py-4">
                          <div className="flex flex-col min-w-0">
                            <span className="font-semibold group-hover:text-brand-cyan transition-colors truncate" title={log.titleFinal || "Unknown track"}>{log.titleFinal || "Unknown track"}</span>
                            <span className="text-xs text-gray-500 truncate" title={log.artistFinal || "Unknown artist"}>{log.artistFinal || "Unknown artist"}</span>
                            <span className="text-xs text-gray-600 truncate">
                              {log.genreFinal ? `Genre: ${log.genreFinal}` : ''}
                              {log.sourceProvider ? `${log.genreFinal ? ' • ' : ''}Source: ${log.sourceProvider}` : ''}
                            </span>
                          </div>
                        </td>
                        <td className="py-4">
                          <span className="px-2 py-0.5 bg-brand-cyan/10 text-brand-cyan rounded-full text-[10px] uppercase font-bold">{formatMethod(log.detectionMethod)}</span>
                        </td>
                        <td className="py-4">
                          {log.status === 'matched' ? <CheckCircle2 className="w-4 h-4 text-green-500" /> : <AlertCircle className="w-4 h-4 text-yellow-500" />}
                        </td>
                      </tr>
                    ))}
                    {!historyLoading && logs.length === 0 && (
                      <tr>
                        <td colSpan={5} className="py-8 text-center text-gray-500">
                          No airplay detections yet. Probe a station to create logs.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
             </div>
          </div>
        )}

        {activeTab === 'settings' && (
          <div className="max-w-2xl bg-white/5 border border-white/10 rounded-3xl p-10">
            <h2 className="text-xl font-semibold mb-8 flex items-center gap-2">
              <Settings className="w-5 h-5 text-gray-400" />
              Environment & Fingerprint Readiness
            </h2>
            
            <div className="space-y-6">
              <div className="bg-black/30 border border-white/10 rounded-2xl p-5 space-y-4">
                <div className="flex items-center justify-between">
                  <span className="font-semibold">Fingerprint pipeline</span>
                  <span className={`px-2 py-1 rounded-lg text-xs font-bold ${dependencies?.fingerprintReady ? 'bg-green-500/20 text-green-400' : 'bg-yellow-500/20 text-yellow-300'}`}>
                    {dependencies?.fingerprintReady ? 'READY' : 'NEEDS SETUP'}
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-2 text-sm text-gray-300">
                  <div>ffmpeg: {dependencies?.ffmpeg ? 'OK' : 'Missing'}</div>
                  <div>ffprobe: {dependencies?.ffprobe ? 'OK' : 'Missing'}</div>
                  <div>fpcalc: {dependencies?.fpcalc ? 'OK' : 'Missing'}</div>
                  <div>AcoustID key: {dependencies?.acoustidApiKeyConfigured ? 'Configured' : 'Missing'}</div>
                </div>
                <div className="grid grid-cols-1 gap-2 text-xs text-gray-400 border-t border-white/5 pt-3">
                  <div>
                    Free APIs active: AcoustID ({dependencies?.freeApisEnabled?.acoustid ? 'on' : 'off'}), MusicBrainz (
                    {dependencies?.freeApisEnabled?.musicbrainz ? 'on' : 'off'}), iTunes Search (
                    {dependencies?.freeApisEnabled?.itunesSearch ? 'on' : 'off'}), Deezer Search (
                    {dependencies?.freeApisEnabled?.deezerSearch !== false ? 'on' : 'off'})
                  </div>
                  <div>Catalog lookup fallback: {dependencies?.catalogLookupReady ? 'ready' : 'needs MusicBrainz user-agent'}</div>
                </div>
                {dependencies && dependencies.missing.length > 0 && (
                  <p className="text-xs text-yellow-300">
                    Missing: {dependencies.missing.join(', ')}
                  </p>
                )}
              </div>

              <label className="flex items-start gap-3 cursor-pointer select-none">
                <input
                  type="checkbox"
                  className="mt-1 rounded border-white/20 bg-black/40"
                  checked={includeHiddenStations}
                  onChange={(e) => setIncludeHiddenStations(e.target.checked)}
                />
                <span className="text-sm text-gray-300">
                  <span className="font-semibold text-white">Show all stations in the database</span>
                  <span className="block text-xs text-gray-500 mt-1">
                    When enabled, the dashboard loads every station row (including those with visibility turned off). Use this to
                    audit the full catalog on production, then re-enable visibility per station if needed.
                  </span>
                </span>
              </label>

              <div className="pt-4 border-t border-white/5 flex gap-4">
                <button
                  onClick={fetchDependencies}
                  className="flex-1 bg-brand-purple hover:bg-brand-purple/80 text-white font-semibold py-3 rounded-xl transition-all"
                >
                  Re-check Dependencies
                </button>
                <button
                  onClick={fetchData}
                  className="flex-1 bg-white/5 hover:bg-white/10 border border-white/10 text-white font-semibold py-3 rounded-xl transition-all"
                >
                  Refresh Station Status
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

      {/* Modals */}
      <AnimatePresence>
        {isAddingStation && (
          <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 bg-black/80 backdrop-blur-md">
            <motion.div 
              initial={{ opacity: 0, scale: 0.95, y: 20 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: 20 }}
              className="bg-zinc-900 border border-white/10 w-full max-w-lg rounded-[2rem] overflow-hidden shadow-2xl"
            >
              <div className="px-8 py-10">
                <h3 className="text-2xl font-bold mb-2">Register Station</h3>
                <p className="text-gray-400 mb-8 text-sm">Add a new radio stream to monitor.</p>
                
                <form
                  className="space-y-6"
                  onSubmit={async (e) => {
                    e.preventDefault();
                    setAddError(null);
                    const form = e.currentTarget;
                    const fd = new FormData(form);
                    const name = String(fd.get('name') || '').trim();
                    const streamUrl = String(fd.get('streamUrl') || '').trim();
                    const country = String(fd.get('country') || '').trim();
                    const pollRaw = Number(fd.get('pollIntervalSeconds'));
                    const pollIntervalSeconds = Number.isFinite(pollRaw) ? Math.min(3600, Math.max(5, Math.trunc(pollRaw))) : 60;
                    if (!name || !streamUrl || !country) {
                      setAddError('Name, stream URL, and country are required.');
                      return;
                    }
                    setAddSubmitting(true);
                    try {
                      const res = await fetch('/api/stations', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                          name,
                          streamUrl,
                          country,
                          pollIntervalSeconds,
                          isActive: true,
                          metadataPriorityEnabled: true,
                          fingerprintFallbackEnabled: true,
                          archiveSongSamples: true,
                        }),
                      });
                      if (!res.ok) {
                        const j = await res.json().catch(() => ({}));
                        setAddError(typeof j.error === 'string' ? j.error : `HTTP ${res.status}`);
                        return;
                      }
                      setIsAddingStation(false);
                      form.reset();
                      await fetchData();
                    } catch (err) {
                      setAddError(err instanceof Error ? err.message : 'Request failed');
                    } finally {
                      setAddSubmitting(false);
                    }
                  }}
                >
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-gray-500">Station Name</label>
                    <input required name="name" type="text" placeholder="e.g. Worldwide FM" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none focus:border-brand-cyan transition-colors" />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-gray-500">Stream URL (Direct MP3/AAC/M3U)</label>
                    <input required name="streamUrl" type="url" placeholder="https://icecast.example.com/stream" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none focus:border-brand-cyan transition-colors" />
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-500">Country</label>
                      <input required name="country" type="text" placeholder="USA" className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none" />
                    </div>
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-500">Poll (secs)</label>
                      <input required name="pollIntervalSeconds" type="number" defaultValue="60" min={5} max={3600} className="w-full bg-white/5 border border-white/10 rounded-xl px-4 py-3 outline-none" />
                    </div>
                  </div>
                  {addError ? <p className="text-sm text-amber-300">{addError}</p> : null}
                  <div className="flex gap-4 pt-4">
                    <button type="button" onClick={() => { setAddError(null); setIsAddingStation(false); }} className="flex-1 py-4 text-gray-400 font-medium hover:text-white">Cancel</button>
                    <button type="submit" disabled={addSubmitting} className="flex-1 bg-brand-cyan text-black font-bold py-4 rounded-2xl hover:brightness-110 shadow-[0_0_20px_rgba(0,242,255,0.3)] disabled:opacity-50">
                      {addSubmitting ? 'Adding…' : 'Add Station'}
                    </button>
                  </div>
                </form>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}


function StationsManagementTable({
  loading,
  stations,
  spinByStation,
  stateCounts,
  stationSearch,
  stationFilter,
  onSearchChange,
  onFilterChange,
  onOpenStation,
  onRefreshAll,
}: {
  loading: boolean;
  stations: Station[];
  spinByStation: Map<string, StationSpinSummary>;
  stateCounts: Record<StationListFilter, number>;
  stationSearch: string;
  stationFilter: StationListFilter;
  onSearchChange: (value: string) => void;
  onFilterChange: (value: StationListFilter) => void;
  onOpenStation: (stationId: string) => void;
  onRefreshAll: () => void;
}) {
  const filters: Array<{ key: StationListFilter; label: string }> = [
    { key: 'all', label: 'All-State' },
    { key: 'running', label: 'Running' },
    { key: 'degraded', label: 'Degraded' },
    { key: 'inactive', label: 'Paused' },
    { key: 'unknown', label: 'Other' },
  ];

  return (
    <div className="bg-white/5 border border-white/10 rounded-3xl p-6 lg:p-8 backdrop-blur-sm space-y-5">
      <div className="flex flex-wrap gap-2">
        {filters.map((filter) => {
          const active = stationFilter === filter.key;
          return (
            <button
              key={filter.key}
              type="button"
              onClick={() => onFilterChange(filter.key)}
              className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-xl text-xs border transition-colors ${
                active
                  ? 'bg-brand-cyan/20 border-brand-cyan/40 text-brand-cyan'
                  : 'bg-black/30 border-white/10 text-gray-300 hover:bg-black/50'
              }`}
            >
              <span>{filter.label}</span>
              <span className="px-1.5 py-0.5 rounded-md bg-white/10 text-[10px]">{stateCounts[filter.key]}</span>
            </button>
          );
        })}
      </div>

      <div className="flex flex-wrap items-center gap-3">
        <div className="relative flex-1 min-w-[250px]">
          <Search className="w-4 h-4 text-gray-500 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            value={stationSearch}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="Search station name, country, province, district, frequency, or ID"
            className="w-full bg-black/40 border border-white/10 rounded-xl pl-9 pr-3 py-2.5 text-sm outline-none focus:border-brand-cyan/40"
          />
        </div>
        <button
          type="button"
          onClick={onRefreshAll}
          className="px-4 py-2.5 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50"
        >
          Refresh
        </button>
      </div>

      <div className="overflow-x-auto rounded-2xl border border-white/5">
        <table className="w-full text-sm min-w-[1200px] table-fixed">
          <thead>
            <tr className="border-b border-white/10 text-left text-gray-400">
              <th className="py-3 px-3 font-medium w-[12%]">Station ID</th>
              <th className="py-3 px-3 font-medium w-[20%]">Name</th>
              <th className="py-3 px-3 font-medium w-[10%]">Country</th>
              <th className="py-3 px-3 font-medium w-[14%]">Location</th>
              <th className="py-3 px-3 font-medium w-[22%]">Now playing</th>
              <th className="py-3 px-3 font-medium w-[10%]">State</th>
              <th className="py-3 px-3 font-medium w-[6%]">Plays</th>
              <th className="py-3 px-3 font-medium text-right w-[6%]">Actions</th>
            </tr>
          </thead>
          <tbody>
            {stations.map((station) => (
              <StationTableRow
                key={station.id}
                station={station}
                spin={spinByStation.get(station.id)}
                onOpenStation={onOpenStation}
                onRefreshAll={onRefreshAll}
              />
            ))}
            {!loading && stations.length === 0 && (
              <tr>
                <td colSpan={8} className="py-10 text-center text-gray-500">
                  No stations found for the current filter/search.
                </td>
              </tr>
            )}
            {loading && (
              <tr>
                <td colSpan={8} className="py-10 text-center text-gray-500">
                  Loading stations…
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function StationTableRow({
  station,
  spin,
  onOpenStation,
  onRefreshAll,
}: {
  station: Station;
  spin?: StationSpinSummary;
  onOpenStation: (stationId: string) => void;
  onRefreshAll: () => void;
}) {
  const [probing, setProbing] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);

  const lastPoll = station.lastPollAt ? new Date(station.lastPollAt) : null;
  const pollOk = station.lastPollStatus === 'ok';
  const pollErr = station.lastPollStatus === 'error';
  const pollMs = (station.pollIntervalSeconds || 60) * 1000;
  const stalePoll =
    lastPoll != null && Number.isFinite(lastPoll.getTime()) && Date.now() - lastPoll.getTime() > Math.max(120_000, pollMs * 4);
  const badge = monitorBadge(station, pollErr, stalePoll, pollOk, lastPoll);

  const handleProbe = async () => {
    setProbing(true);
    try {
      await fetch(`/api/stations/${station.id}/probe`, { method: 'POST' });
      onRefreshAll();
    } finally {
      setTimeout(() => setProbing(false), 600);
    }
  };

  const handleToggle = async () => {
    setToggling(true);
    try {
      await fetch(`/api/stations/${station.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ isActive: !station.isActive }),
      });
      onRefreshAll();
    } finally {
      setToggling(false);
    }
  };

  useEffect(() => {
    setMenuOpen(false);
  }, [station.id]);

  return (
    <tr className="border-b border-white/5 hover:bg-white/5">
      <td className="py-3 px-3 text-xs text-gray-400 font-mono whitespace-nowrap truncate" title={station.id}>{station.id}</td>
      <td className="py-3 px-3 min-w-0">
        <div className="font-medium">{station.name}</div>
        <div className="text-[11px] text-gray-500 mt-0.5 truncate">{station.frequencyMhz ? `${station.frequencyMhz} MHz` : 'Frequency not set'}</div>
      </td>
      <td className="py-3 px-3 text-gray-300 truncate" title={station.country}>{station.country}</td>
      <td className="py-3 px-3 text-gray-400 truncate" title={[station.province, station.district].filter(Boolean).join(' / ') || '—'}>
        {[station.province, station.district].filter(Boolean).join(' / ') || '—'}
      </td>
      <td className="py-3 px-3 min-w-0">
        <div className="font-medium text-gray-200 truncate" title={station.currentNowPlaying?.title || '—'}>{station.currentNowPlaying?.title || '—'}</div>
        <div className="text-xs text-gray-500 truncate" title={station.currentNowPlaying?.artist || 'No current track'}>{station.currentNowPlaying?.artist || 'No current track'}</div>
      </td>
      <td className="py-3 px-3">
        <span className={`text-[11px] font-semibold uppercase tracking-wide px-2 py-1 rounded-lg border ${badge.className}`}>
          {badge.text}
        </span>
      </td>
      <td className="py-3 px-3 text-gray-300 font-mono whitespace-nowrap">{spin?.detectionCount ?? 0}</td>
      <td className="py-3 px-3">
        <div className="flex justify-end items-center gap-2">
          <button
            type="button"
            onClick={() => onOpenStation(station.id)}
            className="px-2.5 py-1.5 rounded-lg border border-brand-cyan/30 text-brand-cyan hover:bg-brand-cyan/10 text-xs inline-flex items-center gap-1"
          >
            <Eye className="w-3.5 h-3.5" />
            View
          </button>
          <div className="relative">
            <button
              type="button"
              aria-label="More actions"
              onClick={() => setMenuOpen((open) => !open)}
              className="px-2.5 py-1.5 rounded-lg border border-white/10 bg-black/30 hover:bg-black/50 text-xs inline-flex items-center gap-1"
            >
              <MoreHorizontal className="w-3.5 h-3.5" />
              More
            </button>
            {menuOpen ? (
              <div className="absolute right-0 mt-1 w-44 bg-zinc-900 border border-white/10 rounded-xl shadow-xl z-20 p-1">
                <button
                  type="button"
                  disabled={probing || !station.isActive}
                  onClick={async () => {
                    setMenuOpen(false);
                    await handleProbe();
                  }}
                  className="w-full text-left px-3 py-2 rounded-lg text-xs hover:bg-white/10 disabled:opacity-40"
                >
                  {probing ? 'Probing…' : 'Probe now'}
                </button>
                <button
                  type="button"
                  disabled={toggling}
                  onClick={async () => {
                    setMenuOpen(false);
                    await handleToggle();
                  }}
                  className="w-full text-left px-3 py-2 rounded-lg text-xs hover:bg-white/10 disabled:opacity-40"
                >
                  {station.isActive ? 'Pause monitoring' : 'Resume monitoring'}
                </button>
                <a
                  href={station.streamUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={() => setMenuOpen(false)}
                  className="block px-3 py-2 rounded-lg text-xs hover:bg-white/10 text-gray-200"
                >
                  Open stream URL
                </a>
              </div>
            ) : null}
          </div>
        </div>
      </td>
    </tr>
  );
}

function StationDetailPage({
  station,
  spin,
  logs,
  songSpins,
  loading,
  error,
  onBack,
  onRefreshAll,
  onRefreshStation,
}: {
  station: Station;
  spin?: StationSpinSummary;
  logs: DetectionLog[];
  songSpins: SongSpinRow[];
  loading: boolean;
  error: string | null;
  onBack: () => void;
  onRefreshAll: () => void;
  onRefreshStation: () => void;
}) {
  const [probing, setProbing] = useState(false);
  const [copied, setCopied] = useState(false);
  const [streamEdit, setStreamEdit] = useState(station.streamUrl);
  const [savingUrl, setSavingUrl] = useState(false);
  const [refreshingUrl, setRefreshingUrl] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [songSearch, setSongSearch] = useState('');
  const [songFilter, setSongFilter] = useState<'all' | 'withArtist' | 'titleOnly' | 'mixedSplit'>('all');

  useEffect(() => {
    setStreamEdit(station.streamUrl);
  }, [station.id, station.streamUrl]);

  const lastPoll = station.lastPollAt ? new Date(station.lastPollAt) : null;
  const pollOk = station.lastPollStatus === 'ok';
  const pollErr = station.lastPollStatus === 'error';
  const pollMs = (station.pollIntervalSeconds || 60) * 1000;
  const stalePoll =
    lastPoll != null && Number.isFinite(lastPoll.getTime()) && Date.now() - lastPoll.getTime() > Math.max(120_000, pollMs * 4);
  const badge = monitorBadge(station, pollErr, stalePoll, pollOk, lastPoll);

  const handleProbe = async () => {
    setProbing(true);
    setPageError(null);
    try {
      const res = await fetch(`/api/stations/${station.id}/probe`, { method: 'POST' });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        setPageError(typeof body.error === 'string' ? body.error : `Probe failed (${res.status})`);
      }
      onRefreshAll();
      onRefreshStation();
    } catch (err) {
      setPageError(err instanceof Error ? err.message : 'Probe failed');
    } finally {
      setTimeout(() => setProbing(false), 700);
    }
  };

  const copyToClipboard = () => {
    navigator.clipboard.writeText(streamEdit);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const filteredSongSpins = React.useMemo(() => {
    const query = songSearch.trim().toLowerCase();
    return songSpins.filter((row) => {
      if (songFilter === 'withArtist' && !row.artist) return false;
      if (songFilter === 'titleOnly' && !!row.artist) return false;
      if (songFilter === 'mixedSplit' && !row.mixRuleApplied) return false;

      if (!query) return true;
      const haystack = [
        row.title || '',
        row.artist || '',
        row.album || '',
        row.originalCombinedRaw || '',
      ]
        .join(' ')
        .toLowerCase();
      return haystack.includes(query);
    });
  }, [songSpins, songSearch, songFilter]);

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <button
          type="button"
          onClick={onBack}
          className="inline-flex items-center gap-2 px-3 py-2 rounded-xl border border-white/10 bg-black/30 hover:bg-black/50 text-sm"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to stations
        </button>

        <div className="flex flex-wrap items-center gap-2">
          <a
            href={`/api/export/logs.xlsx?stationId=${encodeURIComponent(station.id)}&limit=50000`}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-brand-cyan text-black font-semibold text-sm hover:brightness-110"
          >
            <Download className="w-4 h-4" />
            Export {station.name} logs (XLSX template)
          </a>
          <button
            type="button"
            onClick={onRefreshStation}
            className="px-3 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50"
          >
            Refresh page
          </button>
        </div>
      </div>

      <div className="bg-white/5 border border-white/10 rounded-3xl p-6 md:p-8 space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-3xl font-bold">{station.name}</h2>
            <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-gray-400">
              <span className="px-2 py-1 rounded-lg border border-white/10 bg-black/30">{station.country}</span>
              {station.province ? <span>{station.province}</span> : null}
              {station.district ? <span>/ {station.district}</span> : null}
              {station.frequencyMhz ? <span>• {station.frequencyMhz} MHz</span> : null}
            </div>
          </div>

          <div className="text-right space-y-1 text-xs text-gray-500">
            <div><span className={`px-2 py-1 rounded-lg border font-semibold uppercase tracking-wide ${badge.className}`}>{badge.text}</span></div>
            <div>Last check: {station.lastPollAt ? new Date(station.lastPollAt).toLocaleString() : '—'}</div>
            <div>Last song: {station.lastSongDetectedAt ? new Date(station.lastSongDetectedAt).toLocaleString() : '—'}</div>
            <div>Matched plays: {spin?.detectionCount ?? 0}</div>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={handleProbe}
            disabled={probing || !station.isActive}
            className="px-3 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50 disabled:opacity-40"
          >
            {probing ? 'Probing…' : 'Probe now'}
          </button>
          <button
            type="button"
            disabled={toggling}
            onClick={async () => {
              setToggling(true);
              setPageError(null);
              try {
                const res = await fetch(`/api/stations/${station.id}`, {
                  method: 'PATCH',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ isActive: !station.isActive }),
                });
                if (!res.ok) {
                  const body = await res.json().catch(() => ({}));
                  setPageError(typeof body.error === 'string' ? body.error : `HTTP ${res.status}`);
                  return;
                }
                onRefreshAll();
              } catch (apiError) {
                setPageError(apiError instanceof Error ? apiError.message : 'Update failed');
              } finally {
                setToggling(false);
              }
            }}
            className="px-3 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50 disabled:opacity-40"
          >
            {station.isActive ? 'Pause monitoring' : 'Resume monitoring'}
          </button>
          <a
            href={station.streamUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="px-3 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50 inline-flex items-center gap-2"
          >
            Open stream <ExternalLink className="w-3.5 h-3.5" />
          </a>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          <div className="bg-black/30 rounded-2xl p-5 border border-white/10">
            <div className="text-[11px] uppercase tracking-wide text-gray-500 font-semibold mb-3">Current now playing</div>
            {station.currentNowPlaying ? (
              <div className="flex items-start gap-3">
                <div className="w-12 h-12 rounded-xl border border-white/10 bg-gradient-to-br from-brand-cyan/20 to-brand-purple/20 flex items-center justify-center">
                  <Music className="w-6 h-6 text-brand-cyan" />
                </div>
                <div>
                  <p className="font-semibold">{station.currentNowPlaying.title}</p>
                  <p className="text-sm text-gray-400">{station.currentNowPlaying.artist}</p>
                  <p className="text-xs text-gray-500 mt-1">Detected {new Date(station.currentNowPlaying.updatedAt).toLocaleString()}</p>
                </div>
              </div>
            ) : (
              <p className="text-sm text-gray-500">No current metadata track. Probe to refresh.</p>
            )}
          </div>

          <div className="bg-black/30 rounded-2xl p-5 border border-white/10 space-y-3">
            <div className="text-[11px] uppercase tracking-wide text-gray-500 font-semibold">Station stream URL</div>
            <textarea
              rows={3}
              value={streamEdit}
              onChange={(event) => setStreamEdit(event.target.value)}
              className="w-full text-xs font-mono bg-black/40 border border-white/10 rounded-xl px-3 py-2 text-gray-300 outline-none focus:border-brand-cyan/50 resize-y"
              spellCheck={false}
            />
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={copyToClipboard}
                className={`p-2 rounded-lg text-xs flex items-center gap-1 ${copied ? 'bg-green-500/20 text-green-500' : 'bg-white/5 text-gray-500 hover:text-white'}`}
              >
                {copied ? <CheckCircle2 className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
                Copy
              </button>
              <button
                type="button"
                disabled={savingUrl || streamEdit.trim() === station.streamUrl}
                onClick={async () => {
                  setSavingUrl(true);
                  setPageError(null);
                  try {
                    const res = await fetch(`/api/stations/${station.id}`, {
                      method: 'PATCH',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ streamUrl: streamEdit.trim() }),
                    });
                    const body = await res.json().catch(() => ({}));
                    if (!res.ok) {
                      setPageError(typeof body.error === 'string' ? body.error : 'Invalid stream URL');
                      return;
                    }
                    onRefreshAll();
                  } catch (saveError) {
                    setPageError(saveError instanceof Error ? saveError.message : 'Save failed');
                  } finally {
                    setSavingUrl(false);
                  }
                }}
                className="px-3 py-2 rounded-lg text-xs font-semibold bg-brand-purple/30 border border-brand-purple/40 hover:bg-brand-purple/50 disabled:opacity-40"
              >
                {savingUrl ? 'Saving…' : 'Save URL'}
              </button>
              <button
                type="button"
                disabled={refreshingUrl}
                onClick={async () => {
                  setRefreshingUrl(true);
                  setPageError(null);
                  try {
                    const res = await fetch(`/api/stations/${station.id}/refresh-stream`, { method: 'POST' });
                    const body = await res.json().catch(() => ({}));
                    if (!res.ok) {
                      setPageError(typeof body.error === 'string' ? body.error : `HTTP ${res.status}`);
                      return;
                    }
                    if (body.streamUrl) setStreamEdit(String(body.streamUrl));
                    if (!body.updated) {
                      setPageError(typeof body.message === 'string' ? body.message : 'No replacement stream found from source hints');
                    }
                    onRefreshAll();
                  } catch (refreshError) {
                    setPageError(refreshError instanceof Error ? refreshError.message : 'Refresh failed');
                  } finally {
                    setRefreshingUrl(false);
                  }
                }}
                className="px-3 py-2 rounded-lg text-xs font-semibold border border-white/10 bg-black/30 hover:bg-black/50 disabled:opacity-40"
              >
                {refreshingUrl ? 'Refreshing…' : 'Auto-refresh from source'}
              </button>
            </div>
          </div>
        </div>

        {(pageError || error || station.monitorStateReason || station.lastPollError) && (
          <div className="space-y-1 text-sm">
            {pageError ? <p className="text-amber-300">{pageError}</p> : null}
            {error ? <p className="text-amber-300">{error}</p> : null}
            {station.monitorStateReason ? <p className="text-amber-200">Reason: {station.monitorStateReason}</p> : null}
            {station.lastPollError ? <p className="text-red-300">Last poll error: {station.lastPollError}</p> : null}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
        <div className="bg-white/5 border border-white/10 rounded-3xl p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold">Logged songs for {station.name}</h3>
            <span className="text-xs text-gray-500">{filteredSongSpins.length} / {songSpins.length} rows</span>
          </div>
          <div className="mb-4 flex flex-wrap gap-2 items-center">
            <input
              value={songSearch}
              onChange={(event) => setSongSearch(event.target.value)}
              placeholder="Search title / artist / album / raw metadata"
              className="flex-1 min-w-[220px] bg-black/40 border border-white/10 rounded-xl px-3 py-2 text-sm outline-none focus:border-brand-cyan/50"
            />
            <select
              value={songFilter}
              onChange={(event) => setSongFilter(event.target.value as 'all' | 'withArtist' | 'titleOnly' | 'mixedSplit')}
              className="bg-black/40 border border-white/10 rounded-xl px-3 py-2 text-sm outline-none focus:border-brand-cyan/50"
            >
              <option value="all">All songs</option>
              <option value="withArtist">With artist</option>
              <option value="titleOnly">Title only</option>
              <option value="mixedSplit">Mixed-title splits</option>
            </select>
          </div>
          <div className="overflow-x-auto max-h-[480px]">
            <table className="w-full text-sm min-w-[760px] table-fixed">
              <thead>
                <tr className="border-b border-white/10 text-gray-400">
                  <th className="py-2.5 text-left font-medium w-[28%]">Title</th>
                  <th className="py-2.5 text-left font-medium w-[22%]">Artist</th>
                  <th className="py-2.5 text-left font-medium w-[20%]">Album</th>
                  <th className="py-2.5 text-left font-medium w-[22%]">Normalization</th>
                  <th className="py-2.5 text-right font-medium w-[8%]">Plays</th>
                </tr>
              </thead>
              <tbody>
                {filteredSongSpins.map((row, idx) => (
                  <tr key={`${row.stationId}-${row.title}-${idx}`} className="border-b border-white/5 hover:bg-white/5">
                    <td className="py-2.5 truncate pr-2" title={row.title || '—'}>{row.title || '—'}</td>
                    <td className="py-2.5 text-gray-400 truncate pr-2" title={row.artist || '—'}>{row.artist || '—'}</td>
                    <td className="py-2.5 text-gray-500 truncate pr-2" title={row.album || '—'}>{row.album || '—'}</td>
                    <td className="py-2.5 text-xs text-gray-500 truncate pr-2">
                      {row.mixRuleApplied ? (
                        <span className="inline-flex items-center gap-1">
                          <span className="px-1.5 py-0.5 rounded bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">
                            {row.mixRuleApplied}
                          </span>
                          {row.mixSplitConfidence != null ? (
                            <span>{Math.round(row.mixSplitConfidence * 100)}%</span>
                          ) : null}
                        </span>
                      ) : (
                        '—'
                      )}
                    </td>
                    <td className="py-2.5 text-right font-mono">{row.playCount}</td>
                  </tr>
                ))}
                {!loading && filteredSongSpins.length === 0 && (
                  <tr>
                    <td colSpan={5} className="py-8 text-center text-gray-500">No songs match the current search/filter.</td>
                  </tr>
                )}
                {loading && (
                  <tr>
                    <td colSpan={5} className="py-8 text-center text-gray-500">Loading station songs…</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="bg-white/5 border border-white/10 rounded-3xl p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold">Recent detection logs</h3>
            <span className="text-xs text-gray-500">{logs.length} rows</span>
          </div>
          <div className="overflow-x-auto max-h-[480px]">
            <table className="w-full text-sm min-w-[560px] table-fixed">
              <thead>
                <tr className="border-b border-white/10 text-gray-400">
                  <th className="py-2.5 text-left font-medium w-[24%]">Time</th>
                  <th className="py-2.5 text-left font-medium w-[44%]">Track</th>
                  <th className="py-2.5 text-left font-medium w-[18%]">Method</th>
                  <th className="py-2.5 text-left font-medium w-[14%]">Status</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log) => (
                  <tr key={log.id} className="border-b border-white/5 hover:bg-white/5">
                    <td className="py-2.5 text-gray-400 whitespace-nowrap">{new Date(log.observedAt).toLocaleString()}</td>
                    <td className="py-2.5">
                      <div className="truncate" title={log.titleFinal || 'Unknown track'}>{log.titleFinal || 'Unknown track'}</div>
                      <div className="text-xs text-gray-500 truncate" title={log.artistFinal || 'Unknown artist'}>{log.artistFinal || 'Unknown artist'}</div>
                    </td>
                    <td className="py-2.5 text-xs text-gray-400">{formatMethod(log.detectionMethod)}</td>
                    <td className="py-2.5">
                      {log.status === 'matched' ? (
                        <span className="text-green-400 inline-flex items-center gap-1"><CheckCircle2 className="w-3.5 h-3.5" />Matched</span>
                      ) : (
                        <span className="text-yellow-300 inline-flex items-center gap-1"><AlertCircle className="w-3.5 h-3.5" />{log.status}</span>
                      )}
                    </td>
                  </tr>
                ))}
                {!loading && logs.length === 0 && (
                  <tr>
                    <td colSpan={4} className="py-8 text-center text-gray-500">No logs yet for this station.</td>
                  </tr>
                )}
                {loading && (
                  <tr>
                    <td colSpan={4} className="py-8 text-center text-gray-500">Loading station logs…</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}


function NavIcon({ icon, active, onClick, label }: any) {
  return (
    <button 
      onClick={onClick}
      className={`group relative p-3 rounded-xl transition-all duration-300 ${active ? 'bg-brand-purple/20 text-brand-purple' : 'text-gray-500 hover:text-gray-300 hover:bg-white/5'}`}
    >
      {icon}
      <span className="absolute left-20 bg-black border border-white/10 text-[10px] text-white px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none">
        {label}
      </span>
      {active && <motion.div layoutId="nav-active" className="absolute -left-5 top-1/4 w-1 h-1/2 bg-brand-purple rounded-full shadow-[0_0_10px_rgba(112,0,255,0.8)]" />}
    </button>
  );
}

function MetricCard({ label, value, sub }: any) {
  return (
    <div className="bg-white/5 border border-white/10 rounded-2xl p-4 flex flex-col items-center min-w-[120px] backdrop-blur-sm">
      <span className="text-[10px] uppercase font-bold text-gray-500 mb-1">{label}</span>
      <span className="text-2xl font-bold text-white">{value}</span>
      <span className="text-[10px] text-gray-600 mt-1">{sub}</span>
    </div>
  );
}

function monitorBadge(station: Station, pollErr: boolean, stalePoll: boolean, pollOk: boolean, lastPoll: Date | null) {
  const state = station.monitorState || 'UNKNOWN';
  if (!station.isActive) {
    return { text: 'Offline', className: 'border-white/10 text-gray-500' };
  }
  if (state === 'INACTIVE') {
    return { text: 'Offline', className: 'border-red-500/40 text-red-300' };
  }
  if (state === 'DEGRADED') {
    return { text: 'Degraded', className: 'border-amber-500/40 text-amber-200' };
  }
  if (state === 'ACTIVE_TALK') {
    return { text: 'Talk show', className: 'border-purple-500/40 text-purple-200' };
  }
  if (state === 'ACTIVE_MUSIC') {
    return { text: 'Music detected', className: 'border-green-500/30 text-green-300' };
  }
  if (state === 'ACTIVE_NO_MATCH') {
    return { text: 'No current song match', className: 'border-cyan-500/40 text-cyan-200' };
  }
  if (pollErr) return { text: 'Stream error', className: 'border-red-500/40 text-red-300' };
  if (stalePoll) return { text: 'No recent poll', className: 'border-amber-500/40 text-amber-200' };
  if (pollOk || lastPoll) return { text: 'Active', className: 'border-green-500/30 text-green-300' };
  return { text: 'Starting…', className: 'border-white/10 text-gray-500' };
}

import React, { useState, useEffect } from 'react';
import { Radio, Plus } from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import type {
  Station,
  DetectionLog,
  Metrics,
  StationSpinSummary,
  SongSpinRow,
  DependencyStatus,
  UnknownStorageSummary,
  StationListFilter,
  AudioEditorSample,
} from './types/dashboard';
import {
  REQUESTED_STATION_PRIORITY,
  normalizeStationName,
  parseStationHash,
  setStationHash,
  stationGroup,
} from './lib/dashboard-format';
import { StationsManagementTable, StationDetailPage, AudioEditorTab } from './pages/dashboardStationAndAudio';
import { AppSidebar, type AppPageId } from './components/shell/AppSidebar';
import { AppTopBar } from './components/shell/AppTopBar';
import { RightRail } from './components/shell/RightRail';
import { MetricStrip } from './components/shell/MetricStrip';
import { ActivityPage } from './pages/ActivityPage';
import { HistoryPage } from './pages/HistoryPage';
import { AnalyticsPage } from './pages/AnalyticsPage';
import { LearningPage } from './pages/LearningPage';
import { SettingsPage } from './pages/SettingsPage';
import { HelpModal } from './components/shell/HelpModal';
import { NotificationsPanel, type NotificationItem } from './components/shell/NotificationsPanel';
import {
  filterDetectionLogs,
  filterSongSpins,
  railTasksFromDashboard,
  activitySpectrumFromLogs,
} from './lib/dashboard-rail';

// --- Components ---

export default function App() {
  const STATION_REFRESH_MS = 12_000;
  const HISTORY_REFRESH_MS = 15_000;
  const REALTIME_FALLBACK_REFRESH_MS = 12_000;
  const [stations, setStations] = useState<Station[]>([]);
  const [logs, setLogs] = useState<DetectionLog[]>([]);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [dependencies, setDependencies] = useState<DependencyStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [selectedStationId, setSelectedStationId] = useState<string>('all');
  const [activeTab, setActiveTab] = useState<AppPageId>('stations');
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

  // Audio Editor state
  const [audioSamples, setAudioSamples] = useState<AudioEditorSample[]>([]);
  const [audioEditorLoading, setAudioEditorLoading] = useState(false);
  const [audioEditorFilter, setAudioEditorFilter] = useState<'untagged' | 'tagged' | 'all'>('untagged');
  const [audioEditorStationFilter, setAudioEditorStationFilter] = useState<string>('all');
  const [unknownStorage, setUnknownStorage] = useState<UnknownStorageSummary | null>(null);
  const [storageLoading, setStorageLoading] = useState(false);
  const [storageError, setStorageError] = useState<string | null>(null);
  const [storageDryRun, setStorageDryRun] = useState<any | null>(null);
  const [hashBackfillResult, setHashBackfillResult] = useState<any | null>(null);
  const [crawlerStatus, setCrawlerStatus] = useState<any | null>(null);
  const [rematchSummary, setRematchSummary] = useState<any | null>(null);
  const [logQuery, setLogQuery] = useState('');
  const [analyticsQuery, setAnalyticsQuery] = useState('');
  const [helpOpen, setHelpOpen] = useState(false);
  const [notifOpen, setNotifOpen] = useState(false);

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

  const fetchAudioEditorSamples = React.useCallback(async (filter = audioEditorFilter, stationId = audioEditorStationFilter) => {
    setAudioEditorLoading(true);
    try {
      const params = new URLSearchParams({ status: filter, take: '200' });
      if (stationId !== 'all') params.set('stationId', stationId);
      const res = await fetch(`/api/audio-editor/samples?${params}`);
      const data = await res.json();
      setAudioSamples(Array.isArray(data.items) ? data.items : []);
    } catch (e) {
      console.error('Failed to fetch audio editor samples', e);
    } finally {
      setAudioEditorLoading(false);
    }
  }, [audioEditorFilter, audioEditorStationFilter]);
  const fetchUnknownStorage = React.useCallback(async () => {
    setStorageLoading(true);
    setStorageError(null);
    try {
      const res = await fetch('/api/admin/storage/unknown-samples');
      const body = await res.json();
      if (!res.ok) throw new Error(body?.error || `Storage summary failed (${res.status})`);
      setUnknownStorage(body);
    } catch (e) {
      setStorageError(e instanceof Error ? e.message : 'Storage summary failed');
    } finally {
      setStorageLoading(false);
    }
  }, []);
  const fetchCrawlerStatus = React.useCallback(async () => {
    try {
      const res = await fetch('/api/admin/crawler/status');
      const body = await res.json();
      if (!res.ok) throw new Error(body?.error || `Crawler status failed (${res.status})`);
      setCrawlerStatus(body);
    } catch (e) {
      setStorageError(e instanceof Error ? e.message : 'Crawler status failed');
    }
  }, []);

  const fetchRematchSummary = React.useCallback(async () => {
    try {
      const res = await fetch('/api/admin/rematch/summary');
      const body = await res.json();
      if (!res.ok) throw new Error(body?.error || `Rematch summary failed (${res.status})`);
      setRematchSummary(body);
    } catch (e) {
      setStorageError(e instanceof Error ? e.message : 'Rematch summary failed');
    }
  }, []);

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
    if (activeTab === 'audio') {
      fetchAudioEditorSamples();
    }
    if (activeTab === 'settings') {
      fetchUnknownStorage();
      fetchCrawlerStatus();
    }
  }, [activeTab, fetchAudioEditorSamples, fetchUnknownStorage, fetchCrawlerStatus]);

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
      if (activeTab === 'history' || activeTab === 'activity') {
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
      es.addEventListener('station_poll', (event) => {
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
    if (activeTab !== 'history' && activeTab !== 'activity') return;
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

  useEffect(() => {
    if (activeTab !== 'history' && activeTab !== 'activity') setLogQuery('');
  }, [activeTab]);

  useEffect(() => {
    if (activeTab !== 'analytics') setAnalyticsQuery('');
  }, [activeTab]);

  const stationNameById = React.useMemo(
    () => new Map(stations.map((station) => [station.id, station.name])),
    [stations],
  );
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

  const filteredLogsUi = React.useMemo(
    () => filterDetectionLogs(logs, logQuery, stationNameById),
    [logs, logQuery, stationNameById],
  );

  const filteredSongSpins = React.useMemo(
    () => filterSongSpins(songSpins, analyticsQuery, stations),
    [songSpins, analyticsQuery, stations],
  );

  const degradedStations = React.useMemo(
    () => stations.filter((s) => s.monitorState === 'DEGRADED').map((s) => ({ id: s.id, name: s.name })),
    [stations],
  );

  const railTasks = React.useMemo(
    () =>
      railTasksFromDashboard({
        unknownTotal: unknownStorage?.totalUnknownSampleCount ?? 0,
        degradedCount: degradedStations.length,
        rematchPending: rematchSummary?.pending,
        untaggedAudio: audioSamples.filter((s) => !s.manuallyTagged).length,
      }),
    [unknownStorage, degradedStations.length, rematchSummary, audioSamples],
  );

  const activityBars = React.useMemo(() => activitySpectrumFromLogs(logs, 48), [logs]);

  const notificationItems = React.useMemo((): NotificationItem[] => {
    const list: NotificationItem[] = [];
    const unk = unknownStorage?.totalUnknownSampleCount ?? 0;
    if (unk > 0) {
      list.push({
        id: 'unk',
        title: `${unk} unknown samples`,
        detail: 'Review in Audio Library',
        actionLabel: 'Open Audio Library',
        onAction: () => setActiveTab('audio'),
      });
    }
    for (const s of degradedStations.slice(0, 5)) {
      list.push({
        id: `deg-${s.id}`,
        title: 'Degraded stream',
        detail: s.name,
        tone: 'warn',
        actionLabel: 'Open station',
        onAction: () => {
          setActiveTab('stations');
          setStationHash(s.id);
        },
      });
    }
    return list;
  }, [unknownStorage, degradedStations]);

  const headerSearch = React.useMemo(() => {
    if (activeTab === 'stations' && !stationPageId) {
      return {
        show: true as const,
        value: stationSearch,
        onChange: setStationSearch,
        placeholder: 'Search stations…',
      };
    }
    if (activeTab === 'history' || activeTab === 'activity') {
      return {
        show: true as const,
        value: logQuery,
        onChange: setLogQuery,
        placeholder: 'Filter logs (station, title, method…)…',
      };
    }
    if (activeTab === 'analytics') {
      return {
        show: true as const,
        value: analyticsQuery,
        onChange: setAnalyticsQuery,
        placeholder: 'Filter spins (station, title, province…)…',
      };
    }
    return {
      show: false as const,
      value: '',
      onChange: (_: string) => {},
      placeholder: '',
    };
  }, [activeTab, stationPageId, stationSearch, logQuery, analyticsQuery]);

  const handleNavigate = React.useCallback((page: AppPageId) => {
    setHelpOpen(false);
    setNotifOpen(false);
    setActiveTab(page);
  }, []);

  const activeStation = stationPageId ? stationById.get(stationPageId) || null : null;
  useEffect(() => {
    if (typeof document !== 'undefined') {
      document.title = 'Radio Monitor — Royalty Logs & Fingerprint Intelligence';
    }
  }, []);


  return (
    <div className="min-h-screen bg-rm-canvas text-rm-text selection:bg-rm-indigo/25">
      <AppSidebar
        active={activeTab}
        onNavigate={handleNavigate}
        onAddStation={() => setIsAddingStation(true)}
      />

      <div className="flex pl-64 min-h-screen">
        <div className="flex-1 min-w-0 max-w-[1760px] mx-auto w-full py-8 px-6 lg:px-10 rm-hero-gradient">
          <header className="mb-2">
            <AppTopBar
              title={
                activeTab === 'stations' && stationPageId
                  ? 'Station Profile'
                  : activeTab === 'stations'
                    ? 'Stations'
                    : activeTab === 'activity'
                      ? 'Live Activity'
                      : activeTab === 'history'
                        ? 'Airplay History'
                        : activeTab === 'analytics'
                          ? 'Analytics'
                          : activeTab === 'learning'
                            ? 'Intelligence'
                            : activeTab === 'audio'
                              ? 'Audio Library'
                              : activeTab === 'settings'
                                ? 'Settings'
                                : 'Dashboard'
              }
              subtitle={
                activeTab === 'stations' && stationPageId
                  ? 'Station profile, logs, and per-station export.'
                  : activeTab === 'learning'
                    ? 'Self-learned Chromaprint library, pipeline load, and recognition stack status.'
                    : activeTab === 'activity'
                      ? 'A condensed live feed of the most recent detections.'
                      : 'Fingerprint matching, unknown review, catalog growth, and royalty-ready logs.'
              }
              searchValue={headerSearch.value}
              onSearchChange={headerSearch.onChange}
              searchPlaceholder={headerSearch.placeholder}
              showSearch={headerSearch.show}
              onHelpClick={() => {
                setNotifOpen(false);
                setHelpOpen(true);
              }}
              onNotificationsClick={() => {
                setHelpOpen(false);
                setNotifOpen((o) => !o);
              }}
              notificationCount={notificationItems.length}
            />
          </header>

          <MetricStrip
            metrics={metrics}
            monitoredCount={monitoredCount}
            unknownStorage={unknownStorage}
            crawlerStatus={crawlerStatus}
          />

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
            <div className="rm-card p-12 text-center">
              <div className="w-12 h-12 rounded-full bg-slate-100 border border-slate-200 flex items-center justify-center mx-auto mb-4">
                <Radio className="w-6 h-6 text-slate-400" />
              </div>
              <h2 className="text-lg font-semibold text-slate-800">Station not found</h2>
              <p className="text-sm text-slate-500 mt-2">This station may have been removed or hidden.</p>
              <button
                type="button"
                onClick={() => setStationHash()}
                className="btn-ghost mt-5 px-4 py-2 text-sm text-slate-700"
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
              hideSearchInput={activeTab === 'stations' && !stationPageId}
            />
          )}

          {activeTab === 'activity' && (
            <ActivityPage
              logs={filteredLogsUi}
              totalLogCount={logs.length}
              loading={historyLoading}
              stationNameById={stationNameById}
              selectedStationId={selectedStationId}
              orderedStations={orderedStations}
              onStationChange={setSelectedStationId}
              onRefresh={() => fetchLogs(selectedStationId)}
              barHeights={activityBars}
            />
          )}

          {activeTab === 'learning' && <LearningPage />}

          {activeTab === 'analytics' && (
            <AnalyticsPage
              songSpins={filteredSongSpins}
              totalSpinCount={songSpins.length}
              stations={stations}
              analyticsLoading={analyticsLoading}
              onRefresh={() => fetchSongAnalytics()}
            />
          )}

          {activeTab === 'history' && (
            <HistoryPage
              logs={filteredLogsUi}
              totalLogCount={logs.length}
              historyLoading={historyLoading}
              selectedStationId={selectedStationId}
              orderedStations={orderedStations}
              stationNameById={stationNameById}
              onStationChange={setSelectedStationId}
              onRefresh={() => fetchLogs(selectedStationId)}
            />
          )}

          {activeTab === 'audio' && (
            <AudioEditorTab
              samples={audioSamples}
              loading={audioEditorLoading}
              filter={audioEditorFilter}
              stationFilter={audioEditorStationFilter}
              stations={stations}
              onFilterChange={(f) => {
                setAudioEditorFilter(f);
                fetchAudioEditorSamples(f, audioEditorStationFilter);
              }}
              onStationFilterChange={(s) => {
                setAudioEditorStationFilter(s);
                fetchAudioEditorSamples(audioEditorFilter, s);
              }}
              onRefresh={() => fetchAudioEditorSamples()}
              onSampleUpdated={(updated) => {
                setAudioSamples((prev) =>
                  prev.map((s) => (s.id === updated.id ? { ...s, ...updated } : s))
                );
              }}
            />
          )}

          {activeTab === 'settings' && (
            <SettingsPage
              dependencies={dependencies}
              crawlerStatus={crawlerStatus}
              rematchSummary={rematchSummary}
              unknownStorage={unknownStorage}
              storageLoading={storageLoading}
              storageError={storageError}
              storageDryRun={storageDryRun}
              hashBackfillResult={hashBackfillResult}
              includeHiddenStations={includeHiddenStations}
              onRefreshCrawler={fetchCrawlerStatus}
              onRefreshRematch={fetchRematchSummary}
              onStorageError={setStorageError}
              onHashBackfillResult={setHashBackfillResult}
              onStorageDryRun={setStorageDryRun}
              onRefetchUnknownStorage={fetchUnknownStorage}
              onIncludeHiddenChange={setIncludeHiddenStations}
              onRecheckDependencies={fetchDependencies}
              onRefreshStations={fetchData}
            />
          )}
        </div>

        <RightRail
          metrics={metrics}
          monitoredCount={monitoredCount}
          stations={stations}
          logs={logs}
          tasks={railTasks}
        />
      </div>

      <HelpModal open={helpOpen} onClose={() => setHelpOpen(false)} />
      <NotificationsPanel open={notifOpen} onClose={() => setNotifOpen(false)} items={notificationItems} />
      {/* Modals */}
      <AnimatePresence>
        {isAddingStation && (
          <div className="fixed inset-0 z-[100] flex items-center justify-center p-6 bg-rm-navy/40 backdrop-blur-md">
            <motion.div
              initial={{ opacity: 0, scale: 0.96, y: 16 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.96, y: 16 }}
              className="bg-white border border-slate-200 w-full max-w-lg rounded-3xl overflow-hidden shadow-2xl"
            >
              <div className="px-8 py-8">
                <div className="flex items-center gap-3 mb-1">
                  <div className="p-1.5 rounded-lg bg-rm-indigo-soft border border-indigo-200">
                    <Plus className="w-4 h-4 text-rm-indigo" />
                  </div>
                  <h3 className="text-xl font-serif font-semibold text-slate-900">Register Station</h3>
                </div>
                <p className="text-slate-500 mb-6 text-sm ml-10">Add a new radio stream to monitor.</p>

                <form
                  className="space-y-5"
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
                  <div className="space-y-1.5">
                    <label className="rm-section-label">Station Name</label>
                    <input required name="name" type="text" placeholder="e.g. Radio Phoenix" className="rm-input w-full px-4 py-2.5 text-sm" />
                  </div>
                  <div className="space-y-1.5">
                    <label className="rm-section-label">Stream URL (MP3 / AAC / M3U)</label>
                    <input required name="streamUrl" type="url" placeholder="https://icecast.example.com/stream" className="rm-input w-full px-4 py-2.5 text-sm" />
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-1.5">
                      <label className="rm-section-label">Country</label>
                      <input required name="country" type="text" placeholder="Zambia" className="rm-input w-full px-4 py-2.5 text-sm" />
                    </div>
                    <div className="space-y-1.5">
                      <label className="rm-section-label">Poll Interval (sec)</label>
                      <input required name="pollIntervalSeconds" type="number" defaultValue="60" min={5} max={3600} className="rm-input w-full px-4 py-2.5 text-sm" />
                    </div>
                  </div>
                  {addError ? <p className="text-sm text-amber-900 bg-amber-50 border border-amber-200 rounded-xl px-3 py-2">{addError}</p> : null}
                  <div className="flex gap-3 pt-2">
                    <button type="button" onClick={() => { setAddError(null); setIsAddingStation(false); }} className="flex-1 py-3 text-slate-600 font-medium hover:text-slate-900 text-sm transition-colors">Cancel</button>
                    <button type="submit" disabled={addSubmitting} className="flex-1 btn-primary py-3 text-sm">
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



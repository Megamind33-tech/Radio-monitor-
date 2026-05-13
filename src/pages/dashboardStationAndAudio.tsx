import React, { useState, useEffect, useRef } from 'react';
import {
  ArrowLeft,
  RefreshCw,
  ExternalLink,
  Search,
  Copy,
  CheckCircle2,
  AlertCircle,
  Download,
  Eye,
  MoreHorizontal,
  Music,
  Play,
  Tag,
  Save,
  Pencil,
  Sparkles,
  ChevronDown,
  Loader2,
  Activity,
  Headphones,
} from 'lucide-react';
import type {
  Station,
  StationSpinSummary,
  StationListFilter,
  DetectionLog,
  SongSpinRow,
  AudioEditorSample,
  StationUnknownSample,
} from '../types/dashboard';
import { formatMethod } from '../lib/dashboard-format';

function streamSignalsTitle(station: Station): string {
  const on = (v: number | null | undefined) => (v === 1 ? 'yes' : v === 0 ? 'no' : '—');
  return [
    `Stream online: ${on(station.streamOnlineLast)}`,
    `Audio bytes: ${on(station.audioDetectedLast)}`,
    `Metadata: ${on(station.metadataAvailableLast)}`,
    `Song identified: ${on(station.songIdentifiedLast)}`,
    station.contentClassification ? `Content: ${station.contentClassification}` : '',
  ]
    .filter(Boolean)
    .join(' · ');
}

function monitorBadge(station: Station, pollErr: boolean, stalePoll: boolean, pollOk: boolean, lastPoll: Date | null) {
  const state = station.monitorState || 'UNKNOWN';
  const signals = streamSignalsTitle(station);
  if (!station.isActive) {
    return { text: 'Disabled', className: 'border-slate-200 text-slate-500', title: signals };
  }
  if (state === 'INACTIVE') {
    return { text: 'Stream offline', className: 'border-red-200 bg-red-50 text-red-800', title: signals };
  }
  if (state === 'DEGRADED') {
    const transportOk = station.streamOnlineLast === 1 && station.audioDetectedLast === 1;
    return {
      text: transportOk ? 'Online · weak decode' : 'Degraded',
      className: 'border-amber-200 bg-amber-50 text-amber-900',
      title: signals,
    };
  }
  if (state === 'ACTIVE_TALK') {
    return { text: 'Online · non-music', className: 'border-violet-200 bg-violet-50 text-violet-900', title: signals };
  }
  if (state === 'ACTIVE_MUSIC') {
    return { text: 'Online · song ID', className: 'border-emerald-200 bg-emerald-50 text-emerald-900', title: signals };
  }
  if (state === 'ACTIVE_NO_MATCH') {
    return { text: 'Online · no song ID', className: 'border-indigo-200 bg-indigo-50 text-indigo-900', title: signals };
  }
  if (pollErr) return { text: 'Poll error', className: 'border-red-200 bg-red-50 text-red-800', title: signals };
  if (stalePoll) return { text: 'No recent poll', className: 'border-amber-200 bg-amber-50 text-amber-900', title: signals };
  if (pollOk || lastPoll) return { text: 'Online', className: 'border-emerald-200 bg-emerald-50 text-emerald-900', title: signals };
  return { text: 'Starting…', className: 'border-slate-200 text-slate-500', title: signals };
}
export function StationsManagementTable({
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
  hideSearchInput,
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
  hideSearchInput?: boolean;
}) {
  const filters: Array<{ key: StationListFilter; label: string }> = [
    { key: 'all', label: 'All-State' },
    { key: 'running', label: 'Running' },
    { key: 'degraded', label: 'Degraded' },
    { key: 'inactive', label: 'Paused' },
    { key: 'unknown', label: 'Other' },
  ];

  const FILTER_ACCENT: Record<StationListFilter, string> = {
    all: 'bg-slate-900 text-white border-slate-900 shadow-sm',
    running: 'bg-emerald-600 text-white border-emerald-600 shadow-sm',
    degraded: 'bg-amber-500 text-white border-amber-500 shadow-sm',
    inactive: 'bg-slate-500 text-white border-slate-500 shadow-sm',
    unknown: 'bg-sky-600 text-white border-sky-600 shadow-sm',
  };

  return (
    <div className="rm-card p-5 lg:p-7 space-y-5">
      {/* Filter + search row */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex flex-wrap gap-1.5">
          {filters.map((f) => {
            const active = stationFilter === f.key;
            return (
              <button
                key={f.key}
                type="button"
                onClick={() => onFilterChange(f.key)}
                className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-lg text-xs font-medium border transition-all ${
                  active
                    ? FILTER_ACCENT[f.key]
                    : 'bg-transparent border-slate-200 text-slate-500 hover:text-slate-700 hover:border-slate-300'
                }`}
              >
                {f.label}
                <span className={`px-1.5 py-0.5 rounded text-[10px] font-mono ${active ? 'bg-white/20' : 'bg-slate-100'}`}>
                  {stateCounts[f.key]}
                </span>
              </button>
            );
          })}
        </div>

        <div className={`flex-1 flex items-center gap-2 min-w-[240px] ${hideSearchInput ? 'justify-end' : ''}`}>
          {!hideSearchInput ? (
            <div className="relative flex-1">
              <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input
                value={stationSearch}
                onChange={(event) => onSearchChange(event.target.value)}
                placeholder="Search by name, country, province, frequency…"
                className="rm-input w-full pl-8 pr-3 py-2 text-sm"
              />
            </div>
          ) : null}
          <button
            type="button"
            onClick={onRefreshAll}
            className="btn-ghost px-3 py-2 text-sm flex items-center gap-1.5 text-slate-600"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Refresh
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-2xl border border-slate-200">
        <table className="w-full text-sm min-w-[1100px] table-fixed">
          <thead>
            <tr className="border-b border-slate-200 bg-slate-50">
              <th className="py-3 px-4 text-left rm-section-label w-[11%]">Station ID</th>
              <th className="py-3 px-4 text-left rm-section-label w-[19%]">Name</th>
              <th className="py-3 px-4 text-left rm-section-label w-[9%]">Country</th>
              <th className="py-3 px-4 text-left rm-section-label w-[13%]">Location</th>
              <th className="py-3 px-4 text-left rm-section-label w-[22%]">Now Playing</th>
              <th className="py-3 px-4 text-left rm-section-label w-[12%]">State</th>
              <th
                className="py-3 px-4 text-left rm-section-label w-[9%]"
                title="Unique songs / total matched plays"
              >
                Songs
              </th>
              <th className="py-3 px-4 text-right rm-section-label w-[7%]">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
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
                <td colSpan={8} className="py-14 text-center text-slate-500 text-sm">
                  No stations match the current filter or search.
                </td>
              </tr>
            )}
            {loading && (
              <tr>
                <td colSpan={8} className="py-14 text-center text-slate-500 text-sm">
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
    <tr className="hover:bg-slate-50 transition-colors group">
      <td className="py-3.5 px-4 text-[11px] text-slate-500 font-mono truncate" title={station.id}>{station.id}</td>
      <td className="py-3.5 px-4 min-w-0">
        <div className="font-semibold text-slate-900 truncate">{station.name}</div>
        <div className="text-[11px] text-slate-400 mt-0.5">{station.frequencyMhz ? `${station.frequencyMhz} MHz` : '—'}</div>
      </td>
      <td className="py-3.5 px-4 text-slate-600 text-sm truncate" title={station.country}>{station.country}</td>
      <td className="py-3.5 px-4 text-slate-500 text-xs truncate" title={[station.province, station.district].filter(Boolean).join(' / ') || '—'}>
        {[station.province, station.district].filter(Boolean).join(' / ') || '—'}
      </td>
      <td className="py-3.5 px-4 min-w-0">
        {station.currentNowPlaying ? (
          <>
            <div className="text-slate-800 text-sm truncate font-medium" title={station.currentNowPlaying.title}>{station.currentNowPlaying.title}</div>
            <div className="text-[11px] text-slate-500 truncate" title={station.currentNowPlaying.artist}>{station.currentNowPlaying.artist}</div>
          </>
        ) : (
          <span className="text-slate-400 text-xs italic">No current track</span>
        )}
      </td>
      <td className="py-3.5 px-4">
        <span title={badge.title} className={`rm-badge ${badge.className}`}>
          {badge.text}
        </span>
      </td>
      <td className="py-3.5 px-4 font-mono text-xs" title="Unique songs / total plays">
        {spin ? (
          <span>
            <span className="text-slate-800">{spin.uniqueSongs}</span>
            <span className="text-slate-400"> / </span>
            <span className="text-slate-600">{spin.detectionCount}</span>
          </span>
        ) : (
          <span className="text-slate-400">—</span>
        )}
      </td>
      <td className="py-3.5 px-4">
        <div className="flex justify-end items-center gap-1.5">
          <button
            type="button"
            onClick={() => onOpenStation(station.id)}
            className="px-2.5 py-1.5 rounded-lg border border-indigo-300 text-rm-indigo hover:bg-rm-indigo-soft text-xs inline-flex items-center gap-1 transition-all"
          >
            <Eye className="w-3 h-3" />
            View
          </button>
          <div className="relative">
            <button
              type="button"
              aria-label="More actions"
              onClick={() => setMenuOpen((open) => !open)}
              className="p-1.5 rounded-lg border border-slate-200 bg-slate-50 hover:bg-slate-200 text-slate-500 hover:text-slate-700 transition-all"
            >
              <MoreHorizontal className="w-3.5 h-3.5" />
            </button>
            {menuOpen ? (
              <div className="absolute right-0 mt-1 w-44 bg-white border border-slate-200 rounded-xl shadow-xl border border-slate-200 z-20 p-1">
                <button
                  type="button"
                  disabled={probing || !station.isActive}
                  onClick={async () => { setMenuOpen(false); await handleProbe(); }}
                  className="w-full text-left px-3 py-2 rounded-lg text-xs text-slate-700 hover:bg-slate-100 hover:text-slate-900 disabled:opacity-40 transition-colors"
                >
                  {probing ? 'Probing…' : 'Probe now'}
                </button>
                <button
                  type="button"
                  disabled={toggling}
                  onClick={async () => { setMenuOpen(false); await handleToggle(); }}
                  className="w-full text-left px-3 py-2 rounded-lg text-xs text-slate-700 hover:bg-slate-100 hover:text-slate-900 disabled:opacity-40 transition-colors"
                >
                  {station.isActive ? 'Pause monitoring' : 'Resume monitoring'}
                </button>
                <a
                  href={station.streamUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={() => setMenuOpen(false)}
                  className="block px-3 py-2 rounded-lg text-xs text-slate-700 hover:bg-slate-100 hover:text-slate-900 transition-colors"
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

export function StationDetailPage({
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
  const [preferredEdit, setPreferredEdit] = useState(station.preferredStreamUrl || '');
  const [savingUrl, setSavingUrl] = useState(false);
  const [savingPreferred, setSavingPreferred] = useState(false);
  const [refreshingUrl, setRefreshingUrl] = useState(false);
  const [toggling, setToggling] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [songSearch, setSongSearch] = useState('');
  const [songFilter, setSongFilter] = useState<'all' | 'withArtist' | 'titleOnly' | 'mixedSplit'>('all');
  const [discovering, setDiscovering] = useState(false);
  const [applyingStream, setApplyingStream] = useState(false);
  const [discoveryResult, setDiscoveryResult] = useState<{
    candidates: Array<{
      streamUrl: string;
      name?: string;
      source: string;
      tier: string;
      qualityScore: number;
      nameMatch: number;
      detail?: string;
    }>;
    queryUsed: string[];
    serversTried: string[];
    errors: string[];
  } | null>(null);
  const [unknownSamples, setUnknownSamples] = useState<StationUnknownSample[]>([]);
  const [unknownLoading, setUnknownLoading] = useState(false);
  const [unknownError, setUnknownError] = useState<string | null>(null);
  const [editingSample, setEditingSample] = useState<StationUnknownSample | null>(null);
  const [reviewSaving, setReviewSaving] = useState(false);
  const [reviewForm, setReviewForm] = useState({
    artist: '',
    title: '',
    album: '',
    label: '',
    isrc: '',
    iswc: '',
    composerWriter: '',
    publisher: '',
    country: '',
    sourceSociety: '',
    notes: '',
  });

  useEffect(() => {
    setStreamEdit(station.streamUrl);
    setPreferredEdit(station.preferredStreamUrl || '');
  }, [station.id, station.streamUrl, station.preferredStreamUrl]);

  const fetchUnknownSamples = React.useCallback(async () => {
    setUnknownLoading(true);
    setUnknownError(null);
    try {
      const res = await fetch(`/api/stations/${station.id}/unknown-samples`);
      const body = await res.json();
      if (!res.ok) {
        throw new Error(body?.error || `Unknown samples request failed (${res.status})`);
      }
      setUnknownSamples(Array.isArray(body?.items) ? body.items : []);
    } catch (e) {
      setUnknownError(e instanceof Error ? e.message : 'Failed to load unknown samples');
      setUnknownSamples([]);
    } finally {
      setUnknownLoading(false);
    }
  }, [station.id]);

  useEffect(() => {
    fetchUnknownSamples();
  }, [fetchUnknownSamples]);

  const openReviewEditor = (item: StationUnknownSample) => {
    setEditingSample(item);
    setReviewForm({
      artist: item.suggestedArtist || '',
      title: item.suggestedTitle || '',
      album: '',
      label: '',
      isrc: '',
      iswc: '',
      composerWriter: '',
      publisher: '',
      country: '',
      sourceSociety: '',
      notes: '',
    });
  };

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
          className="btn-ghost inline-flex items-center gap-2 px-3 py-2 text-sm text-slate-600"
        >
          <ArrowLeft className="w-4 h-4" />
          All stations
        </button>

        <div className="flex flex-wrap items-center gap-2">
          <a
            href={`/api/export/logs.xlsx?stationId=${encodeURIComponent(station.id)}&limit=50000`}
            className="btn-primary inline-flex items-center gap-2 px-4 py-2 text-sm"
          >
            <Download className="w-4 h-4" />
            Export XLSX
          </a>
          <button
            type="button"
            onClick={onRefreshStation}
            className="btn-ghost px-3 py-2 text-sm text-slate-600 flex items-center gap-1.5"
          >
            <RefreshCw className="w-3.5 h-3.5" />
            Refresh
          </button>
        </div>
      </div>

      <div className="rm-card p-6 md:p-8 space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div>
            <h2 className="text-2xl sm:text-3xl font-serif font-semibold text-slate-900">{station.name}</h2>
            <div className="mt-2 flex flex-wrap items-center gap-1.5 text-sm">
              <span className="px-2 py-0.5 rounded-md text-xs border border-slate-200 bg-slate-100 text-slate-700">{station.country}</span>
              {station.province ? <span className="text-slate-500">{station.province}</span> : null}
              {station.district ? <span className="text-slate-400">/ {station.district}</span> : null}
              {station.frequencyMhz ? <span className="text-slate-500 font-mono text-xs">· {station.frequencyMhz} MHz</span> : null}
            </div>
          </div>

          <div className="text-right space-y-1.5 text-xs text-slate-500">
            <div>
              <span title={badge.title} className={`rm-badge ${badge.className}`}>{badge.text}</span>
            </div>
            <div>Last check: {station.lastPollAt ? new Date(station.lastPollAt).toLocaleString() : '—'}</div>
            <div>Last song: {station.lastSongDetectedAt ? new Date(station.lastSongDetectedAt).toLocaleString() : '—'}</div>
            <div title="Unique songs / total plays">
              <span className="text-slate-700">{spin?.uniqueSongs ?? 0}</span>
              <span className="text-slate-400"> unique · </span>
              <span className="text-slate-600">{spin?.detectionCount ?? 0}</span>
              <span className="text-slate-400"> plays</span>
            </div>
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={handleProbe}
            disabled={probing || !station.isActive}
            className="btn-ghost px-3 py-2 text-sm text-slate-700 flex items-center gap-1.5 disabled:opacity-40"
          >
            <Activity className="w-3.5 h-3.5" />
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
            className="btn-ghost px-3 py-2 text-sm text-slate-700 disabled:opacity-40"
          >
            {station.isActive ? 'Pause monitoring' : 'Resume monitoring'}
          </button>
          <a
            href={(station.preferredStreamUrl || '').trim() || station.streamUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="btn-ghost px-3 py-2 text-sm text-slate-700 inline-flex items-center gap-1.5"
          >
            Open stream <ExternalLink className="w-3.5 h-3.5" />
          </a>
        </div>

        <div className="rounded-2xl border border-amber-500/15 bg-amber-500/[0.04] px-4 py-4 space-y-2">
          <div className="rm-section-label text-amber-400/80">
            Multi-server stream discovery
          </div>
          <p className="text-[11px] text-slate-500 leading-relaxed">
            Searches Radio-Browser mirrors (by country + by name), TuneIn OPML, and harvest hints — no station website required.
            Use the best-ranked direct URL as the preferred mount.
          </p>
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              disabled={discovering}
              onClick={async () => {
                setDiscovering(true);
                setPageError(null);
                try {
                  const res = await fetch(`/api/stations/${station.id}/discover-streams`);
                  const body = await res.json().catch(() => ({}));
                  if (!res.ok) {
                    setPageError(typeof body.error === 'string' ? body.error : `Discovery failed (${res.status})`);
                    setDiscoveryResult(null);
                    return;
                  }
                  setDiscoveryResult(body);
                } catch (e) {
                  setPageError(e instanceof Error ? e.message : 'Discovery failed');
                  setDiscoveryResult(null);
                } finally {
                  setDiscovering(false);
                }
              }}
              className="px-3 py-2 rounded-lg text-xs font-semibold border border-amber-500/40 bg-amber-500/10 text-amber-100 hover:bg-amber-500/20 disabled:opacity-40"
            >
              {discovering ? 'Searching servers…' : 'Search all sources'}
            </button>
            <button
              type="button"
              disabled={applyingStream || !discoveryResult?.candidates?.length}
              onClick={async () => {
                setApplyingStream(true);
                setPageError(null);
                try {
                  const res = await fetch(`/api/stations/${station.id}/discover-streams/apply`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ preferredOnly: true }),
                  });
                  const body = await res.json().catch(() => ({}));
                  if (!res.ok) {
                    setPageError(typeof body.error === 'string' ? body.error : `Apply failed (${res.status})`);
                    return;
                  }
                  if (body.preferredStreamUrl) setPreferredEdit(String(body.preferredStreamUrl));
                  onRefreshAll();
                  onRefreshStation();
                } catch (e) {
                  setPageError(e instanceof Error ? e.message : 'Apply failed');
                } finally {
                  setApplyingStream(false);
                }
              }}
              className="px-3 py-2 rounded-lg text-xs font-semibold border border-slate-200 bg-slate-100 hover:bg-slate-200 disabled:opacity-40"
            >
              {applyingStream ? 'Applying…' : 'Apply best as preferred URL'}
            </button>
          </div>
          {discoveryResult && (
            <div className="mt-2 space-y-2 max-h-64 overflow-y-auto">
              <p className="text-[10px] text-slate-400">
                Queries: {discoveryResult.queryUsed.join(' · ')} · {discoveryResult.candidates.length} URL(s)
              </p>
              {discoveryResult.errors?.length ? (
                <p className="text-[10px] text-amber-800">
                  Partial errors (mirrors may be down): {discoveryResult.errors.slice(0, 3).join(' | ')}
                </p>
              ) : null}
              <table className="w-full text-[10px] text-left border-collapse">
                <thead>
                  <tr className="text-slate-500 border-b border-slate-200">
                    <th className="py-1 pr-2">Score</th>
                    <th className="py-1 pr-2">Tier</th>
                    <th className="py-1 pr-2">Source</th>
                    <th className="py-1">URL</th>
                  </tr>
                </thead>
                <tbody>
                  {discoveryResult.candidates.slice(0, 25).map((c) => (
                    <tr key={c.streamUrl} className="border-b border-slate-100 align-top">
                      <td className="py-1 pr-2 text-slate-700 whitespace-nowrap">{c.qualityScore}</td>
                      <td className="py-1 pr-2 text-slate-600 whitespace-nowrap">{c.tier}</td>
                      <td className="py-1 pr-2 text-slate-600 whitespace-nowrap">{c.source}</td>
                      <td className="py-1 font-mono text-[9px] break-all text-cyan-200/90">
                        <button
                          type="button"
                          className="text-left hover:underline"
                          title="Set as preferred stream"
                          onClick={async () => {
                            setApplyingStream(true);
                            setPageError(null);
                            try {
                              const res = await fetch(`/api/stations/${station.id}/discover-streams/apply`, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ streamUrl: c.streamUrl, preferredOnly: true }),
                              });
                              const body = await res.json().catch(() => ({}));
                              if (!res.ok) {
                                setPageError(typeof body.error === 'string' ? body.error : `Apply failed (${res.status})`);
                                return;
                              }
                              setPreferredEdit(c.streamUrl);
                              onRefreshAll();
                              onRefreshStation();
                            } catch (e) {
                              setPageError(e instanceof Error ? e.message : 'Apply failed');
                            } finally {
                              setApplyingStream(false);
                            }
                          }}
                        >
                          {c.name ? `${c.name} - ` : ''}
                          {c.streamUrl}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div
          className="rm-card-inner px-4 py-3 text-xs text-slate-600 space-y-1"
          title={badge.title}
        >
          <div className="rm-section-label">Stream vs Song ID</div>
          <p className="text-slate-700">{badge.title}</p>
          <p>
            Source tier:{' '}
            <span className="text-slate-800">{station.streamSourceType || 'unknown'}</span>
            {station.streamSourceQualityScore != null ? (
              <span className="text-slate-500"> · quality {station.streamSourceQualityScore}</span>
            ) : null}
          </p>
          {(station.decodeHealthEma != null ||
            station.fingerprintHitEma != null ||
            station.metadataPresentEma != null) && (
            <p className="text-slate-500">
              EMA decode {((station.decodeHealthEma ?? 0) * 100).toFixed(0)}% · fp hit{' '}
              {((station.fingerprintHitEma ?? 0) * 100).toFixed(0)}% · metadata present{' '}
              {((station.metadataPresentEma ?? 0) * 100).toFixed(0)}%
            </p>
          )}
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
          <div className="rm-card-inner p-5">
            <div className="rm-section-label mb-3">Now Playing</div>
            {station.currentNowPlaying ? (
              <div className="flex items-start gap-3">
                <div className="w-11 h-11 rounded-xl border border-indigo-200 bg-gradient-to-br from-indigo-100 to-violet-100 flex items-center justify-center shrink-0">
                  <Music className="w-5 h-5 text-rm-indigo" />
                </div>
                <div>
                  <p className="font-semibold text-slate-900">{station.currentNowPlaying.title}</p>
                  <p className="text-sm text-slate-600">{station.currentNowPlaying.artist}</p>
                  <p className="text-xs text-slate-400 mt-1">Detected {new Date(station.currentNowPlaying.updatedAt).toLocaleString()}</p>
                </div>
              </div>
            ) : (
              <p className="text-sm text-slate-500 italic">No current track. Probe to refresh.</p>
            )}
          </div>

          <div className="rm-card-inner p-5 space-y-3">
            <div className="rm-section-label">Station stream URL</div>
            <textarea
              rows={3}
              value={streamEdit}
              onChange={(event) => setStreamEdit(event.target.value)}
              className="w-full text-xs font-mono rm-input px-3 py-2 text-slate-700 resize-y"
              spellCheck={false}
            />
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={copyToClipboard}
                className={`p-2 rounded-lg text-xs flex items-center gap-1 ${copied ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-500 hover:text-rm-indigo'}`}
              >
                {copied ? <CheckCircle2 className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
                Copy
              </button>
              <button
                type="button"
                disabled={
                  savingUrl ||
                  (streamEdit.trim() === station.streamUrl &&
                    (preferredEdit || '').trim() === (station.preferredStreamUrl || '').trim())
                }
                onClick={async () => {
                  setSavingUrl(true);
                  setPageError(null);
                  try {
                    const res = await fetch(`/api/stations/${station.id}`, {
                      method: 'PATCH',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                        streamUrl: streamEdit.trim(),
                        preferredStreamUrl: (preferredEdit || '').trim() || null,
                      }),
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
                className="px-3 py-2 rounded-lg text-xs font-semibold bg-violet-100 border border-violet-200 hover:bg-violet-200 disabled:opacity-40"
              >
                {savingUrl ? 'Saving…' : 'Save URLs'}
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
                className="px-3 py-2 rounded-lg text-xs font-semibold border border-slate-200 bg-slate-100 hover:bg-slate-200 disabled:opacity-40"
              >
                {refreshingUrl ? 'Refreshing…' : 'Auto-refresh from source'}
              </button>
            </div>
            <div className="rm-section-label pt-2">Preferred mount (optional)</div>
            <p className="text-[11px] text-slate-500">
              When set, monitoring uses this URL instead of the catalog URL. Leave empty to use station URL only.
            </p>
            <textarea
              rows={2}
              value={preferredEdit}
              onChange={(event) => setPreferredEdit(event.target.value)}
              className="w-full text-xs font-mono rm-input px-3 py-2 text-slate-700 resize-y"
              spellCheck={false}
              placeholder="https://… direct mp3/aac/hls mount"
            />
            <button
              type="button"
              disabled={
                savingPreferred ||
                (preferredEdit || '').trim() === (station.preferredStreamUrl || '').trim()
              }
              onClick={async () => {
                setSavingPreferred(true);
                setPageError(null);
                try {
                  const res = await fetch(`/api/stations/${station.id}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                      preferredStreamUrl: (preferredEdit || '').trim() || null,
                    }),
                  });
                  const body = await res.json().catch(() => ({}));
                  if (!res.ok) {
                    setPageError(typeof body.error === 'string' ? body.error : 'Invalid preferred URL');
                    return;
                  }
                  onRefreshAll();
                } catch (e) {
                  setPageError(e instanceof Error ? e.message : 'Save failed');
                } finally {
                  setSavingPreferred(false);
                }
              }}
              className="px-3 py-2 rounded-lg text-xs font-semibold border border-slate-200 bg-slate-100 hover:bg-slate-200 disabled:opacity-40"
            >
              {savingPreferred ? 'Saving…' : 'Save preferred only'}
            </button>
          </div>
        </div>

        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between gap-3">
            <div>
              <h3 className="font-semibold text-slate-900">Unknown Review</h3>
              <p className="text-xs text-slate-500 mt-0.5">Unmatched/unresolved samples for this station.</p>
            </div>
            <button
              type="button"
              onClick={fetchUnknownSamples}
              disabled={unknownLoading}
              className="btn-ghost-sm px-3 py-1.5 text-slate-600 disabled:opacity-50"
            >
              {unknownLoading ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>

          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            <div className="rounded-xl border border-slate-200 bg-black/25 p-3">
              <div className="text-[11px] text-slate-500 uppercase">Total unknowns</div>
              <div className="text-xl font-semibold mt-1">{unknownSamples.length}</div>
            </div>
            <div className="rounded-xl border border-slate-200 bg-black/25 p-3">
              <div className="text-[11px] text-slate-500 uppercase">With audio</div>
              <div className="text-xl font-semibold mt-1">{unknownSamples.filter((s) => s.hasAudio).length}</div>
            </div>
            <div className="rounded-xl border border-slate-200 bg-black/25 p-3">
              <div className="text-[11px] text-slate-500 uppercase">Missing audio</div>
              <div className="text-xl font-semibold mt-1">{unknownSamples.filter((s) => !s.hasAudio).length}</div>
            </div>
            <div className="rounded-xl border border-slate-200 bg-black/25 p-3">
              <div className="text-[11px] text-slate-500 uppercase">Latest unknown</div>
              <div className="text-xs text-slate-700 mt-2">
                {unknownSamples[0]?.capturedAt ? new Date(unknownSamples[0].capturedAt).toLocaleString() : '—'}
              </div>
            </div>
          </div>

          {unknownError ? <div className="text-sm text-red-700">{unknownError}</div> : null}
          {unknownLoading ? <div className="text-sm text-slate-600">Loading unknown samples…</div> : null}
          {!unknownLoading && !unknownError && unknownSamples.length === 0 ? (
            <div className="text-sm text-slate-600 border border-dashed border-slate-200 rounded-xl p-4">No unknown samples found for this station.</div>
          ) : null}

          {!unknownLoading && !unknownError && unknownSamples.length > 0 ? (
            <div className="overflow-x-auto border border-slate-200 rounded-xl">
              <table className="w-full min-w-[760px] text-sm">
                <thead className="bg-slate-100 text-slate-600">
                  <tr>
                    <th className="text-left p-3">Captured</th>
                    <th className="text-left p-3">Metadata</th>
                    <th className="text-left p-3">Status</th>
                    <th className="text-left p-3">Audio</th>
                    <th className="text-left p-3">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {unknownSamples.map((item) => (
                    <tr key={item.id} className="border-t border-slate-200 align-top">
                      <td className="p-3 whitespace-nowrap text-xs text-slate-700">{new Date(item.capturedAt).toLocaleString()}</td>
                      <td className="p-3">
                        <div className="font-medium text-slate-900">
                          {item.suggestedArtist || 'Unknown artist'} {item.suggestedTitle ? `— ${item.suggestedTitle}` : ''}
                        </div>
                        <div className="text-xs text-slate-500 mt-1 line-clamp-2">{item.rawMetadataText || 'No ICY/metadata text captured.'}</div>
                      </td>
                      <td className="p-3">
                        <div className="flex flex-col gap-1 text-xs">
                          <span className="px-2 py-1 rounded-lg border border-slate-300 bg-slate-100 w-fit">{item.matchStatus || 'unmatched'}</span>
                          <span className="px-2 py-1 rounded-lg border border-slate-300 bg-slate-100 w-fit">{item.reviewStatus || 'unreviewed'}</span>
                          <span className="px-2 py-1 rounded-lg border border-slate-300 bg-slate-100 w-fit">
                            {item.fingerprintStatus || 'not_started'}
                          </span>
                          {item.linkedTrackId ? <span className="text-[11px] text-rm-indigo">Track: {item.linkedTrackId.slice(0, 8)}</span> : null}
                        </div>
                      </td>
                      <td className="p-3 text-xs">
                        {item.fileAvailable ? (
                          <audio controls preload="none" className="w-56">
                            <source src={item.audioUrl} />
                          </audio>
                        ) : (
                          <span className="text-amber-800">Missing audio</span>
                        )}
                      </td>
                      <td className="p-3">
                        <div className="flex flex-col gap-1">
                          <button type="button" onClick={() => openReviewEditor(item)} className="text-xs px-2 py-1 rounded border border-slate-200 text-left hover:bg-slate-100">Edit Metadata</button>
                          <button type="button" disabled className="text-xs px-2 py-1 rounded border border-slate-200 opacity-50 text-left">Auto Identify (Phase 4)</button>
                          <button
                            type="button"
                            onClick={async () => {
                              openReviewEditor(item);
                            }}
                            className="text-xs px-2 py-1 rounded border border-slate-200 text-left hover:bg-slate-100"
                          >
                            Save + Fingerprint
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
        {editingSample && (
          <div className="fixed inset-0 z-50 bg-black/70 backdrop-blur-sm flex items-center justify-center p-4">
            <div className="w-full max-w-3xl rounded-2xl border border-slate-200 bg-slate-950 p-5 space-y-4">
              <div className="flex items-center justify-between">
                <h4 className="text-lg font-semibold">Review Unknown Sample</h4>
                <button type="button" onClick={() => setEditingSample(null)} className="text-sm text-slate-600">Close</button>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
                {Object.entries(reviewForm).map(([key, value]) => (
                  <label key={key} className="space-y-1">
                    <span className="text-xs text-slate-600">{key}</span>
                    <input
                      value={value}
                      onChange={(e) => setReviewForm((prev) => ({ ...prev, [key]: e.target.value }))}
                      className="w-full px-3 py-2 rounded-lg border border-slate-200 bg-slate-100"
                    />
                  </label>
                ))}
              </div>
              {editingSample.fileAvailable ? (
                <audio controls preload="none" className="w-full">
                  <source src={editingSample.audioUrl} />
                </audio>
              ) : <div className="text-amber-800 text-sm">Audio file is missing for this sample.</div>}
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  disabled={reviewSaving}
                  onClick={async () => {
                    setReviewSaving(true);
                    try {
                      const res = await fetch(`/api/unknown-samples/${editingSample.id}/review`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ ...reviewForm, verificationStatus: 'human_verified' }),
                      });
                      if (!res.ok) throw new Error(`Review save failed (${res.status})`);
                      await fetchUnknownSamples();
                      setEditingSample(null);
                    } catch (e) {
                      setUnknownError(e instanceof Error ? e.message : 'Review save failed');
                    } finally {
                      setReviewSaving(false);
                    }
                  }}
                  className="px-3 py-2 rounded-lg bg-rm-indigo text-white text-sm font-semibold"
                >
                  Save Review
                </button>
                <button
                  type="button"
                  disabled={reviewSaving}
                  onClick={async () => {
                    setReviewSaving(true);
                    try {
                      const saveRes = await fetch(`/api/unknown-samples/${editingSample.id}/review`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ ...reviewForm, verificationStatus: 'human_verified' }),
                      });
                      if (!saveRes.ok) throw new Error(`Review save failed (${saveRes.status})`);
                      const fpRes = await fetch(`/api/unknown-samples/${editingSample.id}/save-fingerprint`, { method: 'POST' });
                      if (!fpRes.ok) throw new Error(`Fingerprint save failed (${fpRes.status})`);
                      await fetchUnknownSamples();
                      setEditingSample(null);
                    } catch (e) {
                      setUnknownError(e instanceof Error ? e.message : 'Save + fingerprint failed');
                    } finally {
                      setReviewSaving(false);
                    }
                  }}
                  className="px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-sm"
                >
                  Save + Fingerprint
                </button>
              </div>
            </div>
          </div>
        )}

        {(pageError || error || station.monitorStateReason || station.lastPollError) && (
          <div className="space-y-1 text-sm">
            {pageError ? <p className="text-amber-800">{pageError}</p> : null}
            {error ? <p className="text-amber-800">{error}</p> : null}
            {station.monitorStateReason ? <p className="text-amber-200">Reason: {station.monitorStateReason}</p> : null}
            {station.lastPollError ? <p className="text-red-700">Last poll error: {station.lastPollError}</p> : null}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-2 gap-5">
        <div className="rm-card p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-semibold text-slate-900">Logged songs — {station.name}</h3>
            <span className="text-xs text-slate-500 text-right max-w-[min(100%,22rem)] leading-snug">
              {filteredSongSpins.length} of {songSpins.length} rows shown
              {spin != null ? (
                <>
                  {' '}
                  · {spin.uniqueSongs} unique · {spin.detectionCount} total plays
                </>
              ) : null}
              <span className="block text-[10px] text-slate-400 mt-0.5">
                One row per distinct title key; high play counts are repeats (e.g. promos).
              </span>
            </span>
          </div>
          <div className="mb-4 flex flex-wrap gap-2 items-center">
            <input
              value={songSearch}
              onChange={(event) => setSongSearch(event.target.value)}
              placeholder="Search title / artist / album…"
              className="rm-input flex-1 min-w-[180px] px-3 py-2 text-sm"
            />
            <select
              value={songFilter}
              onChange={(event) => setSongFilter(event.target.value as 'all' | 'withArtist' | 'titleOnly' | 'mixedSplit')}
              className="rm-input bg-slate-100 px-3 py-2 text-sm"
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
                <tr className="border-b border-slate-200 text-slate-600">
                  <th className="py-2.5 text-left font-medium w-[28%]">Title</th>
                  <th className="py-2.5 text-left font-medium w-[22%]">Artist</th>
                  <th className="py-2.5 text-left font-medium w-[20%]">Album</th>
                  <th className="py-2.5 text-left font-medium w-[22%]">Normalization</th>
                  <th className="py-2.5 text-right font-medium w-[8%]">Plays</th>
                </tr>
              </thead>
              <tbody>
                {filteredSongSpins.map((row, idx) => (
                  <tr key={`${row.stationId}-${row.title}-${idx}`} className="border-b border-slate-100 hover:bg-slate-100">
                    <td className="py-2.5 truncate pr-2" title={row.title || '—'}>{row.title || '—'}</td>
                    <td className="py-2.5 text-slate-600 truncate pr-2" title={row.artist || '—'}>{row.artist || '—'}</td>
                    <td className="py-2.5 text-slate-500 truncate pr-2" title={row.album || '—'}>{row.album || '—'}</td>
                    <td className="py-2.5 text-xs text-slate-500 truncate pr-2">
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
                    <td colSpan={5} className="py-8 text-center text-slate-500">No songs match the current search/filter.</td>
                  </tr>
                )}
                {loading && (
                  <tr>
                    <td colSpan={5} className="py-8 text-center text-slate-500">Loading station songs…</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="rm-card p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-semibold text-slate-900">Recent Detection Logs</h3>
            <span className="text-xs text-slate-400">{logs.length} rows</span>
          </div>
          <div className="overflow-x-auto max-h-[480px]">
            <table className="w-full text-sm min-w-[560px] table-fixed">
              <thead>
                <tr className="border-b border-slate-200 text-slate-600">
                  <th className="py-2.5 text-left font-medium w-[24%]">Time</th>
                  <th className="py-2.5 text-left font-medium w-[44%]">Track</th>
                  <th className="py-2.5 text-left font-medium w-[18%]">Method</th>
                  <th className="py-2.5 text-left font-medium w-[14%]">Status</th>
                </tr>
              </thead>
              <tbody>
                {logs.map((log) => (
                  <tr key={log.id} className="border-b border-slate-100 hover:bg-slate-100">
                    <td className="py-2.5 text-slate-600 whitespace-nowrap">{new Date(log.observedAt).toLocaleString()}</td>
                    <td className="py-2.5">
                      <div className="truncate" title={log.titleFinal || 'Unknown track'}>{log.titleFinal || 'Unknown track'}</div>
                      <div className="text-xs text-slate-500 truncate" title={log.artistFinal || 'Unknown artist'}>{log.artistFinal || 'Unknown artist'}</div>
                    </td>
                    <td className="py-2.5 text-xs text-slate-600">{formatMethod(log.detectionMethod)}</td>
                    <td className="py-2.5">
                      {log.status === 'matched' ? (
                        <span className="text-emerald-700 inline-flex items-center gap-1"><CheckCircle2 className="w-3.5 h-3.5" />Matched</span>
                      ) : (
                        <span className="text-amber-700 inline-flex items-center gap-1"><AlertCircle className="w-3.5 h-3.5" />{log.status}</span>
                      )}
                    </td>
                  </tr>
                ))}
                {!loading && logs.length === 0 && (
                  <tr>
                    <td colSpan={4} className="py-8 text-center text-slate-500">No logs yet for this station.</td>
                  </tr>
                )}
                {loading && (
                  <tr>
                    <td colSpan={4} className="py-8 text-center text-slate-500">Loading station logs…</td>
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
interface AudioEditorCardProps {
  sample: AudioEditorSample;
  onSaved: (updated: Partial<AudioEditorSample> & { id: string }) => void;
}

function AudioEditorCard({ sample, onSaved }: AudioEditorCardProps) {
  const [title, setTitle] = React.useState(sample.titleFinal || sample.parsedTitle || '');
  const [artist, setArtist] = React.useState(sample.artistFinal || sample.parsedArtist || '');
  const [album, setAlbum] = React.useState(sample.releaseFinal || '');
  const [genre, setGenre] = React.useState(sample.genreFinal || '');
  const [saving, setSaving] = React.useState(false);
  const [saveMsg, setSaveMsg] = React.useState<{ ok: boolean; text: string } | null>(null);
  const [manuallyTagged, setManuallyTagged] = React.useState(sample.manuallyTagged);
  const [identifying, setIdentifying] = React.useState(false);
  const [identifyMsg, setIdentifyMsg] = React.useState<{ ok: boolean; text: string; score?: number | null } | null>(null);
  const [showIdentifyMenu, setShowIdentifyMenu] = React.useState(false);
  const identifyMenuRef = React.useRef<HTMLDivElement>(null);
  const audioRef = useRef<HTMLAudioElement>(null);

  React.useEffect(() => {
    if (!showIdentifyMenu) return;
    const onClick = (e: MouseEvent) => {
      if (identifyMenuRef.current && !identifyMenuRef.current.contains(e.target as Node)) {
        setShowIdentifyMenu(false);
      }
    };
    document.addEventListener('mousedown', onClick);
    return () => document.removeEventListener('mousedown', onClick);
  }, [showIdentifyMenu]);

  const displayTime = sample.detectedAt || sample.createdAt;

  const statusBadge = () => {
    if (manuallyTagged) {
      return <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-emerald-100 text-emerald-800 flex items-center gap-1"><Tag className="w-3 h-3" />Manually Tagged</span>;
    }
    if (sample.recoveryStatus === 'no_match') {
      return <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-amber-100 text-amber-900">No Match</span>;
    }
    if (sample.recoveryStatus === 'pending') {
      return <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-blue-500/20 text-blue-300">Pending</span>;
    }
    if (sample.recoveryStatus === 'error') {
      return <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-red-100 text-red-800">Error</span>;
    }
    return <span className="px-2 py-0.5 rounded-full text-[10px] font-bold bg-slate-100 text-slate-600">{sample.recoveryStatus}</span>;
  };

  const handleIdentify = async (provider: 'auto' | 'acoustid' | 'audd') => {
    setShowIdentifyMenu(false);
    setIdentifying(true);
    setIdentifyMsg(null);
    setSaveMsg(null);
    try {
      const res = await fetch(`/api/audio-editor/samples/${sample.id}/identify`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ provider }),
      });
      const data = await res.json();
      if (!data.ok) {
        const detail = data.detail || data.tried?.join(', ') || data.error || 'No match found';
        setIdentifyMsg({ ok: false, text: detail });
        return;
      }
      if (data.title) setTitle(data.title);
      if (data.artist) setArtist(data.artist);
      if (data.album) setAlbum(data.album);
      if (data.genre) setGenre(data.genre);
      const providerLabel = data.provider === 'acoustid' ? 'AcoustID' : data.provider === 'audd' ? 'AudD' : data.provider;
      const scoreStr = data.score != null ? ` (confidence ${Math.round(data.score * 100)}%)` : '';
      setIdentifyMsg({ ok: true, text: `Identified via ${providerLabel}${scoreStr} — review and save.`, score: data.score });
    } catch (e) {
      setIdentifyMsg({ ok: false, text: e instanceof Error ? e.message : 'Request failed' });
    } finally {
      setIdentifying(false);
    }
  };

  const handleSave = async () => {
    setSaving(true);
    setSaveMsg(null);
    try {
      const res = await fetch(`/api/audio-editor/samples/${sample.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: title.trim(), artist: artist.trim(), album: album.trim(), genre: genre.trim() }),
      });
      const data = await res.json();
      if (!res.ok) {
        setSaveMsg({ ok: false, text: data.error || `HTTP ${res.status}` });
        return;
      }
      setManuallyTagged(true);
      setIdentifyMsg(null);
      setSaveMsg({ ok: true, text: 'Metadata saved & embedded in audio file.' });
      onSaved({
        id: sample.id,
        titleFinal: title.trim() || null,
        artistFinal: artist.trim() || null,
        releaseFinal: album.trim() || null,
        genreFinal: genre.trim() || null,
        manuallyTagged: true,
        manualTaggedAt: new Date().toISOString(),
        recoveryStatus: 'recovered',
      });
    } catch (e) {
      setSaveMsg({ ok: false, text: e instanceof Error ? e.message : 'Request failed' });
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className={`bg-slate-100 border rounded-2xl p-5 space-y-4 transition-all ${manuallyTagged ? 'border-green-500/30' : 'border-slate-200'}`}>
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-semibold text-slate-900 truncate">{sample.stationName || sample.stationId}</span>
            {sample.stationCountry && (
              <span className="text-xs text-slate-500">{sample.stationCountry}{sample.stationProvince ? ` · ${sample.stationProvince}` : ''}</span>
            )}
            {statusBadge()}
          </div>
          <div className="text-xs text-slate-500 mt-0.5">
            {new Date(displayTime).toLocaleString()} · {sample.recoveryAttempts} recovery attempt{sample.recoveryAttempts !== 1 ? 's' : ''}
          </div>
          {sample.rawStreamText && (
            <div className="text-xs text-slate-600 mt-1 italic truncate" title={sample.rawStreamText}>
              Stream text: "{sample.rawStreamText}"
            </div>
          )}
        </div>
        {manuallyTagged && (
          <div className="shrink-0">
            <CheckCircle2 className="w-5 h-5 text-emerald-600" />
          </div>
        )}
      </div>

      {/* Audio Player */}
      {sample.hasAudioFile ? (
        <div className="bg-slate-100 rounded-xl p-3">
          <div className="flex items-center justify-between gap-2 mb-2">
            <div className="flex items-center gap-2">
              <Play className="w-3.5 h-3.5 text-rm-indigo shrink-0" />
              <span className="text-xs text-slate-600">Recorded audio sample</span>
            </div>
            {/* Identify button — only shown when audio file exists */}
            <div className="relative" ref={identifyMenuRef}>
              <button
                onClick={() => setShowIdentifyMenu((v) => !v)}
                disabled={identifying}
                title="Send audio to AcoustID or AudD for automatic identification"
                className="flex items-center gap-1.5 bg-purple-600/20 hover:bg-purple-600/30 border border-purple-500/30 text-purple-300 text-[11px] font-semibold px-2.5 py-1 rounded-lg transition-all disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {identifying
                  ? <Loader2 className="w-3 h-3 animate-spin" />
                  : <Sparkles className="w-3 h-3" />}
                {identifying ? 'Identifying…' : 'Identify'}
                {!identifying && <ChevronDown className="w-3 h-3 opacity-60" />}
              </button>
              {showIdentifyMenu && (
                <div className="absolute right-0 top-full mt-1 z-50 bg-[#141414] border border-slate-200 rounded-xl shadow-2xl py-1 min-w-[160px]">
                  <button
                    onClick={() => handleIdentify('auto')}
                    className="w-full text-left px-3 py-2 text-xs text-slate-800 hover:bg-slate-100 transition-colors"
                  >
                    <span className="font-semibold">Auto</span>
                    <span className="text-slate-500 ml-1">AcoustID → AudD</span>
                  </button>
                  <button
                    onClick={() => handleIdentify('acoustid')}
                    className="w-full text-left px-3 py-2 text-xs text-slate-800 hover:bg-slate-100 transition-colors"
                  >
                    AcoustID only
                  </button>
                  <button
                    onClick={() => handleIdentify('audd')}
                    className="w-full text-left px-3 py-2 text-xs text-slate-800 hover:bg-slate-100 transition-colors"
                  >
                    AudD only
                  </button>
                </div>
              )}
            </div>
          </div>
          <audio
            ref={audioRef}
            controls
            className="w-full h-9"
            style={{ colorScheme: 'dark' }}
            preload="metadata"
            src={`/api/audio-editor/samples/${sample.id}/audio`}
          />
        </div>
      ) : (
        <div className="bg-slate-50 border border-slate-100 rounded-xl p-3 text-xs text-slate-500 flex items-center gap-2">
          <AlertCircle className="w-3.5 h-3.5 shrink-0" />
          Audio file not available on disk
        </div>
      )}

      {/* Identify result banner */}
      {identifyMsg && (
        <div className={`rounded-xl px-3 py-2 text-xs flex items-start gap-2 ${identifyMsg.ok ? 'bg-violet-50 border border-violet-200 text-violet-900' : 'bg-red-50 border border-red-200 text-red-800'}`}>
          {identifyMsg.ok
            ? <Sparkles className="w-3.5 h-3.5 shrink-0 mt-0.5 text-violet-600" />
            : <AlertCircle className="w-3.5 h-3.5 shrink-0 mt-0.5" />}
          <span>{identifyMsg.text}</span>
        </div>
      )}

      {/* Metadata Edit Form */}
      <div className="space-y-3">
        <div className="flex items-center gap-2 text-xs font-medium text-slate-600 border-t border-slate-100 pt-3">
          <Pencil className="w-3.5 h-3.5" />
          {manuallyTagged ? 'Edit metadata' : identifyMsg?.ok ? 'Review identified metadata' : 'Enter metadata manually'}
        </div>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <div className="space-y-1">
            <label className="text-xs text-slate-500">Title <span className="text-rm-indigo">*</span></label>
            <input
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              placeholder="Song title"
              className="w-full bg-slate-100 border border-slate-200 rounded-xl px-3 py-2 text-sm outline-none focus:border-rm-indigo transition-colors"
            />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-slate-500">Artist</label>
            <input
              type="text"
              value={artist}
              onChange={(e) => setArtist(e.target.value)}
              placeholder="Artist name"
              className="w-full bg-slate-100 border border-slate-200 rounded-xl px-3 py-2 text-sm outline-none focus:border-rm-indigo transition-colors"
            />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-slate-500">Album / Release</label>
            <input
              type="text"
              value={album}
              onChange={(e) => setAlbum(e.target.value)}
              placeholder="Album name"
              className="w-full bg-slate-100 border border-slate-200 rounded-xl px-3 py-2 text-sm outline-none focus:border-rm-indigo transition-colors"
            />
          </div>
          <div className="space-y-1">
            <label className="text-xs text-slate-500">Genre</label>
            <input
              type="text"
              value={genre}
              onChange={(e) => setGenre(e.target.value)}
              placeholder="e.g. Afrobeats"
              className="w-full bg-slate-100 border border-slate-200 rounded-xl px-3 py-2 text-sm outline-none focus:border-rm-indigo transition-colors"
            />
          </div>
        </div>

        <div className="flex items-center justify-between gap-3 pt-1">
          {saveMsg ? (
            <p className={`text-xs ${saveMsg.ok ? 'text-emerald-700' : 'text-red-700'} flex items-center gap-1`}>
              {saveMsg.ok ? <CheckCircle2 className="w-3.5 h-3.5" /> : <AlertCircle className="w-3.5 h-3.5" />}
              {saveMsg.text}
            </p>
          ) : (
            <span className="text-xs text-slate-400">Changes are saved to database and embedded in the audio file.</span>
          )}
          <button
            onClick={handleSave}
            disabled={saving || !title.trim()}
            className="shrink-0 flex items-center gap-2 bg-rm-indigo text-white font-bold px-4 py-2 rounded-xl text-sm hover:brightness-110 transition-all disabled:opacity-40 disabled:cursor-not-allowed shadow-[0_4px_16px_rgba(99,102,241,0.35)]"
          >
            <Save className="w-3.5 h-3.5" />
            {saving ? 'Saving…' : manuallyTagged ? 'Update' : 'Save Metadata'}
          </button>
        </div>
      </div>
    </div>
  );
}

export function AudioEditorTab({
  samples,
  loading,
  filter,
  stationFilter,
  stations,
  onFilterChange,
  onStationFilterChange,
  onRefresh,
  onSampleUpdated,
}: {
  samples: AudioEditorSample[];
  loading: boolean;
  filter: 'untagged' | 'tagged' | 'all';
  stationFilter: string;
  stations: Station[];
  onFilterChange: (f: 'untagged' | 'tagged' | 'all') => void;
  onStationFilterChange: (s: string) => void;
  onRefresh: () => void;
  onSampleUpdated: (updated: Partial<AudioEditorSample> & { id: string }) => void;
}) {
  const withAudio = samples.filter((s) => s.hasAudioFile);
  const withoutAudio = samples.filter((s) => !s.hasAudioFile);
  const untaggedCount = samples.filter((s) => !s.manuallyTagged).length;

  const filterLabels: Record<typeof filter, string> = {
    untagged: 'Needs Tagging',
    tagged: 'Manually Tagged',
    all: 'All Samples',
  };

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-semibold flex items-center gap-2">
            <Headphones className="w-5 h-5 text-rm-indigo" />
            Audio Metadata Editor
          </h2>
          <p className="text-sm text-slate-600 mt-1">
            Listen to unidentified recordings and manually set their title, artist, and album.
            Metadata is written to the audio file and saved system-wide.
          </p>
        </div>
        <button
          onClick={onRefresh}
          disabled={loading}
          className="flex items-center gap-2 bg-slate-100 hover:bg-slate-100 border border-slate-200 text-sm font-medium px-4 py-2 rounded-xl transition-all disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Filter Bar */}
      <div className="flex flex-wrap gap-3 items-center">
        <div className="flex bg-slate-100 border border-slate-200 rounded-xl p-1 gap-1">
          {(['untagged', 'tagged', 'all'] as const).map((f) => (
            <button
              key={f}
              onClick={() => onFilterChange(f)}
              className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all ${
                filter === f
                  ? 'bg-rm-indigo text-white shadow-md shadow-indigo-200'
                  : 'text-slate-600 hover:text-rm-indigo'
              }`}
            >
              {filterLabels[f]}
              {f === 'untagged' && untaggedCount > 0 && filter !== 'untagged' && (
                <span className="ml-1.5 bg-amber-100 text-amber-900 rounded-full px-1.5 text-[10px]">{untaggedCount}</span>
              )}
            </button>
          ))}
        </div>

        <select
          value={stationFilter}
          onChange={(e) => onStationFilterChange(e.target.value)}
          className="bg-slate-100 border border-slate-200 rounded-xl px-3 py-2 text-xs text-slate-700 outline-none focus:border-rm-indigo transition-colors"
        >
          <option value="all">All stations</option>
          {[...new Map(stations.map((s) => [s.id, s])).values()].map((s) => (
            <option key={s.id} value={s.id}>{s.name}</option>
          ))}
        </select>

        <span className="text-xs text-slate-500">
          {loading ? 'Loading…' : `${samples.length} sample${samples.length !== 1 ? 's' : ''}`}
          {withoutAudio.length > 0 && ` · ${withoutAudio.length} without audio file`}
        </span>
      </div>

      {/* Empty State */}
      {!loading && samples.length === 0 && (
        <div className="bg-slate-100 border border-slate-200 rounded-3xl p-16 text-center">
          <Headphones className="w-12 h-12 text-slate-400 mx-auto mb-4" />
          <p className="text-slate-600 font-medium">
            {filter === 'tagged'
              ? 'No manually tagged samples yet.'
              : filter === 'untagged'
              ? 'No unidentified audio samples in the queue.'
              : 'No audio samples found.'}
          </p>
          <p className="text-xs text-slate-400 mt-2">
            {filter === 'untagged' && 'Unresolved recordings appear here when the system cannot identify a playing song.'}
          </p>
        </div>
      )}

      {/* Loading skeleton */}
      {loading && (
        <div className="space-y-4">
          {[1, 2, 3].map((i) => (
            <div key={i} className="bg-slate-100 border border-slate-200 rounded-2xl p-5 animate-pulse">
              <div className="h-4 bg-slate-100 rounded w-1/3 mb-3" />
              <div className="h-9 bg-slate-100 rounded-xl mb-3" />
              <div className="grid grid-cols-2 gap-3">
                <div className="h-10 bg-slate-100 rounded-xl" />
                <div className="h-10 bg-slate-100 rounded-xl" />
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Sample Cards */}
      {!loading && withAudio.length > 0 && (
        <div className="space-y-4">
          {withAudio.map((sample) => (
            <AudioEditorCard key={sample.id} sample={sample} onSaved={onSampleUpdated} />
          ))}
        </div>
      )}

      {/* No-audio samples (collapsed list) */}
      {!loading && withoutAudio.length > 0 && (
        <div className="bg-slate-100 border border-slate-200 rounded-2xl p-4">
          <p className="text-xs text-slate-500 mb-2 flex items-center gap-1.5">
            <AlertCircle className="w-3.5 h-3.5" />
            {withoutAudio.length} sample{withoutAudio.length !== 1 ? 's' : ''} without audio file on disk (metadata editing still possible)
          </p>
          <div className="space-y-2 max-h-64 overflow-y-auto">
            {withoutAudio.map((sample) => (
              <AudioEditorCard key={sample.id} sample={sample} onSaved={onSampleUpdated} />
            ))}
          </div>
        </div>
      )}

      {/* Info box */}
      {!loading && samples.length > 0 && (
        <div className="bg-rm-indigo-soft border border-indigo-100 rounded-2xl p-4 text-xs text-slate-600 space-y-1">
          <p className="font-semibold text-rm-indigo">How manual tagging works</p>
          <ul className="list-disc pl-4 space-y-0.5">
            <li>Title is required. Artist, Album, and Genre are optional but recommended.</li>
            <li>Saving updates the detection log and marks the song as <strong className="text-rm-indigo">matched</strong> across the whole system.</li>
            <li>The song will appear in Song Spins analytics and History.</li>
            <li>Metadata is embedded directly into the WAV recording file (ID3 tags via ffmpeg).</li>
            <li>If a Chromaprint fingerprint exists, the song is added to the local fingerprint library so future plays are auto-identified.</li>
          </ul>
        </div>
      )}
    </div>
  );
}

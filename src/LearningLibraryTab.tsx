import React, { useCallback, useEffect, useState } from 'react';
import { Brain, RefreshCw, Download, Activity, CheckCircle2, XCircle, Radio } from 'lucide-react';

type PipelineGate = { active: number; maxConcurrent: number; minGapMs: number };

type LibraryStats = {
  totalRecordings: number;
  totalMatches: number;
  sumPlayCountTotal: number;
  latestLearnedAt: string | null;
  latestMatchedAt: string | null;
  bySource: Record<string, number>;
  learningEnabled: boolean;
};

type DashboardPayload = {
  library: LibraryStats;
  pipelineGate: PipelineGate;
  services: {
    acoustid: boolean;
    acoustidOpen: boolean;
    musicbrainz: boolean;
    audd: boolean;
    acrcloud: boolean;
    paidFallbacksEnabled: boolean;
    localLearningEnabled: boolean;
  };
  pipelineEnv: { minGapMs: number; maxConcurrent: number };
};

type LocalFpRow = {
  id: string;
  title: string | null;
  artist: string | null;
  displayArtist: string | null;
  titleWithoutFeat: string | null;
  featuredArtistsJson: string | null;
  releaseTitle: string | null;
  genre: string | null;
  labelName: string | null;
  countryCode: string | null;
  durationSec: number;
  durationMs: number | null;
  playCountTotal: number;
  source: string;
  confidence: number;
  timesMatched: number;
  firstLearnedAt: string;
  lastMatchedAt: string;
  recordingMbid: string | null;
  acoustidTrackId: string | null;
};

const POLL_MS = 4000;

function parseFeatured(json: string | null): string[] {
  if (!json) return [];
  try {
    const a = JSON.parse(json);
    return Array.isArray(a) ? a.filter((x): x is string => typeof x === 'string') : [];
  } catch {
    return [];
  }
}

function sourceLabel(s: string): string {
  if (s === 'acoustid') return 'AcoustID';
  if (s === 'stream_metadata') return 'ICY + audio';
  if (s === 'manual') return 'Manual / AudD / ACR';
  return s;
}

function ServicePill({ ok, label }: { ok: boolean; label: string }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-xs font-semibold border ${
        ok ? 'border-green-500/40 bg-green-500/10 text-green-300' : 'border-white/10 bg-black/30 text-gray-500'
      }`}
    >
      {ok ? <CheckCircle2 className="w-3.5 h-3.5" /> : <XCircle className="w-3.5 h-3.5" />}
      {label}
    </span>
  );
}

export function LearningLibraryTab() {
  const [dash, setDash] = useState<DashboardPayload | null>(null);
  const [rows, setRows] = useState<LocalFpRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [take] = useState(80);

  const fetchAll = useCallback(async () => {
    try {
      const [dRes, rRes] = await Promise.all([
        fetch('/api/learning/dashboard'),
        fetch(`/api/fingerprints/local?take=${take}`),
      ]);
      if (!dRes.ok) throw new Error(`Dashboard HTTP ${dRes.status}`);
      if (!rRes.ok) throw new Error(`Library HTTP ${rRes.status}`);
      const dJson = (await dRes.json()) as DashboardPayload;
      const rJson = (await rRes.json()) as LocalFpRow[];
      setDash(dJson);
      setRows(Array.isArray(rJson) ? rJson : []);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load');
    } finally {
      setLoading(false);
    }
  }, [take]);

  useEffect(() => {
    void fetchAll();
    const t = setInterval(() => void fetchAll(), POLL_MS);
    return () => clearInterval(t);
  }, [fetchAll]);

  useEffect(() => {
    const es = new EventSource('/api/events/monitoring');
    const bump = () => void fetchAll();
    es.addEventListener('song_detected', bump);
    es.onerror = () => {};
    return () => es.close();
  }, [fetchAll]);

  const gate = dash?.pipelineGate;
  const lib = dash?.library;
  const cap = gate?.maxConcurrent ?? 1;
  const active = gate?.active ?? 0;
  const loadPct = cap > 0 ? Math.min(100, Math.round((active / cap) * 100)) : 0;

  const bySourceEntries = lib?.bySource ? Object.entries(lib.bySource).sort((a, b) => b[1] - a[1]) : [];

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap justify-between items-start gap-4">
        <div>
          <h2 className="text-xl font-semibold flex items-center gap-2">
            <Brain className="w-6 h-6 text-brand-purple" />
            Learning library
          </h2>
          <p className="text-sm text-gray-500 mt-1 max-w-2xl">
            Chromaprint fingerprints learned from successful matches. Local hits skip AcoustID on repeat plays.
            Pipeline load shows how many stations are capturing audio right now (global gate).
          </p>
        </div>
        <div className="flex gap-2">
          <a
            href="/api/fingerprints/local/export?format=csv"
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50"
            download
          >
            <Download className="w-4 h-4" />
            Export quality CSV
          </a>
          <button
            type="button"
            onClick={() => void fetchAll()}
            className="inline-flex items-center gap-2 px-4 py-2 rounded-xl border border-white/10 bg-black/30 text-sm hover:bg-black/50"
          >
            <RefreshCw className="w-4 h-4" />
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-2xl border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">{error}</div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-white/5 border border-white/10 rounded-3xl p-6 space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
              <Activity className="w-4 h-4 text-brand-cyan" />
              Fingerprint pipeline (live)
            </h3>
            <span className="text-[10px] text-gray-600 uppercase">Polls every {POLL_MS / 1000}s</span>
          </div>
          <div className="flex items-end gap-4">
            <div className="flex-1">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>Concurrent captures</span>
                <span>
                  {active} / {cap}
                </span>
              </div>
              <div className="h-3 rounded-full bg-black/50 overflow-hidden border border-white/10">
                <div
                  className="h-full rounded-full bg-gradient-to-r from-brand-cyan to-brand-purple transition-all duration-500"
                  style={{ width: `${loadPct}%` }}
                />
              </div>
              <p className="text-[11px] text-gray-600 mt-2">
                Min gap between starts: {dash?.pipelineEnv.minGapMs ?? '—'} ms · Max concurrent:{' '}
                {dash?.pipelineEnv.maxConcurrent ?? '—'} (env tunable)
              </p>
            </div>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 pt-2">
            <StatBox label="Learned tracks" value={lib?.totalRecordings ?? '—'} sub="rows in DB" />
            <StatBox label="Local lookups (Σ)" value={lib?.totalMatches ?? '—'} sub="timesMatched sum" />
            <StatBox label="Play aggregate (Σ)" value={lib?.sumPlayCountTotal ?? '—'} sub="playCountTotal sum" />
            <StatBox
              label="Learning"
              value={lib?.learningEnabled === false ? 'OFF' : 'ON'}
              sub={lib?.learningEnabled === false ? 'LOCAL_FP_LEARNING_ENABLED' : 'writes enabled'}
            />
          </div>
        </div>

        <div className="bg-white/5 border border-white/10 rounded-3xl p-6 space-y-4">
          <h3 className="text-sm font-semibold text-gray-300">How it learns</h3>
          <ol className="text-xs text-gray-400 space-y-2 list-decimal list-inside leading-relaxed">
            <li>Match from AcoustID, catalog, or paid fallback after audio capture.</li>
            <li>Fingerprint + metadata stored in LocalFingerprint (deduped by SHA-1).</li>
            <li>Next time the same audio pattern appears → instant local match.</li>
          </ol>
          <div className="pt-2 border-t border-white/10">
            <p className="text-[10px] uppercase text-gray-600 mb-2">Teach sources (counts)</p>
            <div className="flex flex-wrap gap-2">
              {bySourceEntries.length === 0 && <span className="text-xs text-gray-600">No rows yet</span>}
              {bySourceEntries.map(([k, v]) => (
                <span key={k} className="px-2 py-1 rounded-lg bg-black/40 border border-white/10 text-xs text-gray-300">
                  {sourceLabel(k)}: <span className="text-white font-mono">{v}</span>
                </span>
              ))}
            </div>
          </div>
          {(lib?.latestMatchedAt || lib?.latestLearnedAt) && (
            <p className="text-[11px] text-gray-600">
              Last library activity:{' '}
              {lib.latestMatchedAt ? new Date(lib.latestMatchedAt).toLocaleString() : '—'}
            </p>
          )}
        </div>
      </div>

      <div className="bg-white/5 border border-white/10 rounded-3xl p-6">
        <h3 className="text-sm font-semibold text-gray-300 mb-4">Recognition stack (env)</h3>
        <div className="flex flex-wrap gap-2">
          <ServicePill ok={dash?.services.acoustid ?? false} label="AcoustID key" />
          <ServicePill ok={dash?.services.musicbrainz ?? false} label="MusicBrainz UA" />
          <ServicePill ok={dash?.services.audd ?? false} label="AudD (paid lane)" />
          <ServicePill ok={dash?.services.acrcloud ?? false} label="ACRCloud (paid lane)" />
          <ServicePill ok={dash?.services.paidFallbacksEnabled ?? true} label="Paid lane enabled" />
          <ServicePill ok={dash?.services.localLearningEnabled ?? true} label="Local learning" />
        </div>
      </div>

      <div className="bg-white/5 border border-white/10 rounded-3xl p-6">
        <div className="flex justify-between items-center mb-4">
          <h3 className="text-sm font-semibold text-gray-300 flex items-center gap-2">
            <Radio className="w-4 h-4 text-brand-cyan" />
            Recent learned fingerprints
          </h3>
          <span className="text-xs text-gray-600">{rows.length} shown</span>
        </div>
        <div className="overflow-x-auto max-h-[560px]">
          <table className="w-full text-left text-sm min-w-[1100px]">
            <thead>
              <tr className="border-b border-white/10 text-gray-500 text-xs uppercase tracking-wide">
                <th className="py-2 pr-3">Track</th>
                <th className="py-2 pr-3">Album / label</th>
                <th className="py-2 pr-3">Source</th>
                <th className="py-2 pr-3 text-right">Plays Σ</th>
                <th className="py-2 pr-3 text-right">Hits</th>
                <th className="py-2 pr-3">Last match</th>
              </tr>
            </thead>
            <tbody>
              {loading && rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-10 text-center text-gray-500">
                    Loading library…
                  </td>
                </tr>
              )}
              {!loading && rows.length === 0 && (
                <tr>
                  <td colSpan={6} className="py-10 text-center text-gray-500">
                    No fingerprints yet. Run the monitor until songs match — first learn happens after a successful ID
                    (and first-play archive when enabled).
                  </td>
                </tr>
              )}
              {rows.map((r) => {
                const feat = parseFeatured(r.featuredArtistsJson);
                const dur =
                  r.durationMs && r.durationMs > 0
                    ? `${Math.round(r.durationMs / 1000)}s`
                    : r.durationSec > 0
                      ? `${r.durationSec}s`
                      : '—';
                return (
                  <tr key={r.id} className="border-b border-white/5 hover:bg-white/[0.03]">
                    <td className="py-3 pr-3 align-top">
                      <div className="font-medium text-white truncate max-w-[280px]" title={r.title || ''}>
                        {r.title || '—'}
                      </div>
                      <div className="text-xs text-gray-500 truncate max-w-[280px]" title={r.artist || ''}>
                        {r.displayArtist || r.artist || '—'}
                      </div>
                      {feat.length > 0 && (
                        <div className="text-[10px] text-brand-cyan/90 mt-0.5">Feat. {feat.join(', ')}</div>
                      )}
                      {r.titleWithoutFeat && r.titleWithoutFeat !== r.title && (
                        <div className="text-[10px] text-gray-600 mt-0.5">Title (clean): {r.titleWithoutFeat}</div>
                      )}
                      <div className="text-[10px] text-gray-600 mt-1 font-mono">
                        {dur} · conf {typeof r.confidence === 'number' ? r.confidence.toFixed(2) : '—'}
                        {r.recordingMbid ? ` · MB ${r.recordingMbid.slice(0, 8)}…` : ''}
                      </div>
                    </td>
                    <td className="py-3 pr-3 align-top text-xs text-gray-400">
                      <div className="truncate max-w-[200px]" title={r.releaseTitle || ''}>
                        {r.releaseTitle || '—'}
                      </div>
                      <div className="text-gray-600 truncate max-w-[200px]" title={r.labelName || ''}>
                        {r.labelName || ''}
                      </div>
                      <div className="text-gray-600">{r.genre || ''}</div>
                      {r.countryCode && <div className="text-[10px]">{r.countryCode}</div>}
                    </td>
                    <td className="py-3 pr-3 align-top">
                      <span className="px-2 py-0.5 rounded-md bg-brand-purple/15 text-brand-purple text-[10px] font-bold uppercase">
                        {r.source}
                      </span>
                    </td>
                    <td className="py-3 pr-3 align-top text-right font-mono text-gray-300">{r.playCountTotal}</td>
                    <td className="py-3 pr-3 align-top text-right font-mono text-gray-400">{r.timesMatched}</td>
                    <td className="py-3 pr-3 align-top text-xs text-gray-500 whitespace-nowrap">
                      {new Date(r.lastMatchedAt).toLocaleString()}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function StatBox({ label, value, sub }: { label: string; value: string | number; sub: string }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/30 px-3 py-3">
      <div className="text-[10px] uppercase text-gray-600 font-bold">{label}</div>
      <div className="text-xl font-bold text-white mt-0.5">{value}</div>
      <div className="text-[10px] text-gray-600 mt-1">{sub}</div>
    </div>
  );
}

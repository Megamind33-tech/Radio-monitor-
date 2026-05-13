import React from 'react';
import type { Metrics } from '../../types/dashboard';
import type { UnknownStorageSummary } from '../../types/dashboard';

const METRIC_ACCENT: Record<string, { border: string; text: string; glow: string }> = {
  cyan: { border: 'border-indigo-200', text: 'text-rm-indigo', glow: 'shadow-sm shadow-indigo-100/80' },
  green: { border: 'border-emerald-200', text: 'text-emerald-700', glow: 'shadow-sm shadow-emerald-100/80' },
  amber: { border: 'border-amber-200', text: 'text-amber-700', glow: 'shadow-sm shadow-amber-100/80' },
  purple: { border: 'border-violet-200', text: 'text-violet-700', glow: 'shadow-sm shadow-violet-100/80' },
  sky: { border: 'border-sky-200', text: 'text-sky-700', glow: 'shadow-sm shadow-sky-100/80' },
  orange: { border: 'border-orange-200', text: 'text-orange-700', glow: 'shadow-sm shadow-orange-100/80' },
};

function MetricCard({
  label,
  value,
  sub,
  accent = 'cyan',
}: {
  label: string;
  value: string;
  sub: string;
  accent?: string;
}) {
  const a = METRIC_ACCENT[accent] ?? METRIC_ACCENT.cyan;
  return (
    <div className={`bg-slate-50 border ${a.border} rounded-2xl p-4 flex flex-col ${a.glow}`}>
      <span className="rm-section-label mb-2">{label}</span>
      <span className={`text-2xl font-bold ${a.text} leading-none mb-1`}>{value}</span>
      <span className="text-[10px] text-slate-400 leading-snug">{sub}</span>
    </div>
  );
}

export function MetricStrip({
  metrics,
  monitoredCount,
  unknownStorage,
  crawlerStatus,
}: {
  metrics: Metrics | null;
  monitoredCount: number;
  unknownStorage: UnknownStorageSummary | null;
  crawlerStatus: { total?: number; byFp?: unknown[]; byClass?: unknown[]; failures?: unknown[]; altSources?: number; possibleDupes?: number } | null;
}) {
  return (
    <div className="mb-8">
      <div className="grid grid-cols-2 sm:grid-cols-3 xl:grid-cols-6 gap-3">
        <MetricCard
          label="Match Rate"
          value={
            metrics
              ? `${((metrics.music_match_rate_24h ?? metrics.music_match_rate ?? metrics.match_rate_24h ?? metrics.match_rate) * 100).toFixed(1)}%`
              : '--'
          }
          sub={
            metrics?.music_detections_24h != null && metrics.music_matched_24h != null
              ? `${metrics.music_matched_24h}/${metrics.music_detections_24h} songs 24h`
              : 'Last 24h · songs only'
          }
          accent="cyan"
        />
        <MetricCard label="Monitoring" value={monitoredCount.toString()} sub="Active stations" accent="green" />
        <MetricCard
          label="Unknown Queue"
          value={(unknownStorage?.totalUnknownSampleCount ?? 0).toString()}
          sub="Awaiting review"
          accent="amber"
        />
        <MetricCard
          label="Matched 24h"
          value={(metrics?.music_matched_24h ?? metrics?.music_matched ?? 0).toString()}
          sub="Song identifications"
          accent="purple"
        />
        <MetricCard
          label="Catalog Sources"
          value={(crawlerStatus?.total ?? 0).toString()}
          sub="Discovered media"
          accent="sky"
        />
        <MetricCard
          label="Reclaimable"
          value={`${Math.round((unknownStorage?.estimatedBytesReclaimable ?? 0) / (1024 * 1024))} MB`}
          sub="Storage dry-run"
          accent="orange"
        />
      </div>

      {metrics?.matched_by_detection_method_24h && (
        <p className="text-[11px] text-slate-400 mt-3">
          24h by method — AcoustID:{' '}
          <span className="text-slate-600 font-mono">{metrics.matched_by_detection_method_24h.fingerprint_acoustid ?? 0}</span>
          {' · '}Local:{' '}
          <span className="text-slate-600 font-mono">{metrics.matched_by_detection_method_24h.fingerprint_local ?? 0}</span>
          {' · '}Catalog:{' '}
          <span className="text-slate-600 font-mono">{metrics.matched_by_detection_method_24h.catalog_lookup ?? 0}</span>
          {' · '}AudD/ACR:{' '}
          <span className="text-slate-600 font-mono">
            {(metrics.matched_by_detection_method_24h.fingerprint_audd ?? 0) +
              (metrics.matched_by_detection_method_24h.fingerprint_acrcloud ?? 0)}
          </span>
        </p>
      )}
    </div>
  );
}

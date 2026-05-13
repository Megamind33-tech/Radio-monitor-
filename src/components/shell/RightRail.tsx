import React, { useMemo } from 'react';
import type { Metrics, DetectionLog, Station } from '../../types/dashboard';
import type { RailTaskItem, CalendarCell } from '../../lib/dashboard-rail';
import {
  calendarCellsForMonth,
  calendarMonthLabel,
  provinceMixFromStations,
  railRecentFromLogs,
} from '../../lib/dashboard-rail';

export function RightRail({
  metrics,
  monitoredCount,
  stations,
  logs,
  tasks,
}: {
  metrics: Metrics | null;
  monitoredCount: number;
  stations: Station[];
  logs: DetectionLog[];
  tasks: RailTaskItem[];
}) {
  const matchPct = metrics
    ? `${((metrics.music_match_rate_24h ?? metrics.music_match_rate ?? metrics.match_rate_24h ?? metrics.match_rate) * 100).toFixed(1)}%`
    : '—';

  const stationNameById = useMemo(() => new Map(stations.map((s) => [s.id, s.name])), [stations]);
  const provinceRows = useMemo(() => provinceMixFromStations(stations), [stations]);
  const recentLines = useMemo(() => railRecentFromLogs(logs, stationNameById, 8), [logs, stationNameById]);
  const monthLabel = useMemo(() => calendarMonthLabel(), []);
  const calCells = useMemo<CalendarCell[]>(() => calendarCellsForMonth(), []);

  return (
    <aside className="hidden xl:flex w-80 shrink-0 flex-col border-l border-slate-200/80 bg-white/50 backdrop-blur-sm min-h-screen sticky top-0">
      <div className="p-5 space-y-5 overflow-y-auto max-h-screen">
        <div>
          <p className="rm-section-label mb-2">Snapshot</p>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div className="rounded-xl border border-slate-200 bg-white p-3">
              <div className="text-slate-500">Match (24h)</div>
              <div className="text-lg font-bold text-rm-indigo">{matchPct}</div>
            </div>
            <div className="rounded-xl border border-slate-200 bg-white p-3">
              <div className="text-slate-500">Active monitors</div>
              <div className="text-lg font-bold text-slate-900">{monitoredCount}</div>
            </div>
          </div>
        </div>

        <div>
          <div className="flex items-center justify-between mb-2">
            <p className="rm-section-label">Calendar</p>
            <span className="text-[10px] text-slate-400 font-medium">{monthLabel}</span>
          </div>
          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="grid grid-cols-7 gap-1 text-center text-[10px] text-slate-400 mb-1">
              {['S', 'M', 'T', 'W', 'T', 'F', 'S'].map((d, i) => (
                <span key={`${d}-${i}`}>{d}</span>
              ))}
            </div>
            <div className="grid grid-cols-7 gap-1">
              {calCells.map((cell) => (
                <div
                  key={cell.key}
                  className={`aspect-square rounded-lg flex items-center justify-center text-[11px] font-medium ${
                    cell.d == null ? 'text-slate-200' : cell.muted ? 'text-slate-300' : 'text-slate-700'
                  } ${cell.highlight ? 'bg-rm-indigo text-white shadow-sm' : cell.d != null && !cell.muted ? 'hover:bg-slate-50' : ''}`}
                >
                  {cell.d == null ? '·' : cell.d}
                </div>
              ))}
            </div>
          </div>
        </div>

        <div>
          <p className="rm-section-label mb-2">Tasks</p>
          <ul className="space-y-2">
            {tasks.map((t) => (
              <li
                key={t.id}
                className={`flex items-start gap-2 rounded-xl border px-3 py-2 text-xs ${
                  t.done ? 'border-emerald-200 bg-emerald-50/60 text-emerald-900' : 'border-slate-200 bg-white text-slate-700'
                }`}
              >
                <span className={`mt-0.5 h-3.5 w-3.5 rounded border ${t.done ? 'border-emerald-400 bg-emerald-400' : 'border-slate-300'}`} />
                <span className="flex-1">
                  <span className="font-medium block">{t.title}</span>
                  <span className="text-[10px] text-slate-500">{t.due}</span>
                </span>
              </li>
            ))}
          </ul>
        </div>

        <div>
          <p className="rm-section-label mb-2">Recent detections</p>
          <ul className="space-y-2">
            {(recentLines).map((a) => (
              <li key={a.id} className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs">
                <div className="font-semibold text-slate-800">{a.label}</div>
                <div
                  className={`text-[10px] mt-0.5 ${
                    a.tone === 'warn' ? 'text-amber-700' : a.tone === 'ok' ? 'text-emerald-700' : 'text-slate-500'
                  }`}
                >
                  {a.detail}
                </div>
              </li>
            ))}
            {recentLines.length === 0 ? (
              <li className="text-xs text-slate-500 px-1 py-2">No logs loaded yet.</li>
            ) : null}
          </ul>
        </div>

        <div>
          <p className="rm-section-label mb-2">Stations by province</p>
          <div className="rounded-2xl border border-slate-200 bg-white p-3 space-y-2">
            {provinceRows.map((row) => (
              <div key={row.name}>
                <div className="flex justify-between text-[10px] text-slate-600 mb-0.5">
                  <span className="truncate pr-2">{row.name}</span>
                  <span className="font-mono shrink-0">{row.pct}%</span>
                </div>
                <div className="h-1.5 rounded-full bg-slate-100 overflow-hidden">
                  <div className="h-full rounded-full bg-gradient-to-r from-indigo-500 to-violet-500" style={{ width: `${row.pct}%` }} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </aside>
  );
}

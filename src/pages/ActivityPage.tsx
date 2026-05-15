import React from 'react';
import { Activity, CheckCircle2, AlertCircle, RefreshCw } from 'lucide-react';
import type { DetectionLog, Station } from '../types/dashboard';
import { formatFusionV2Line, formatMethod, parseFusionV2FromDiagnostics } from '../lib/dashboard-format';

export function ActivityPage({
  logs,
  totalLogCount,
  loading,
  stationNameById,
  selectedStationId,
  orderedStations,
  onStationChange,
  onRefresh,
  barHeights,
}: {
  logs: DetectionLog[];
  totalLogCount: number;
  loading: boolean;
  stationNameById: Map<string, string>;
  selectedStationId: string;
  orderedStations: Station[];
  onStationChange: (id: string) => void;
  onRefresh: () => void;
  barHeights: number[];
}) {
  const recent = logs.slice(0, 40);

  return (
    <div className="space-y-6">
      <div className="rm-card overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-200 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Activity className="w-5 h-5 text-rm-indigo" />
            <div>
              <h2 className="text-base font-semibold text-slate-900">Live activity</h2>
              <p className="text-xs text-slate-500">Latest detections — same feed as History, scoped by station below.</p>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <select
              value={selectedStationId}
              onChange={(e) => onStationChange(e.target.value)}
              className="rm-input bg-slate-100 px-3 py-1.5 text-xs min-w-[140px]"
            >
              <option value="all">All stations</option>
              {orderedStations.map((station) => (
                <option key={station.id} value={station.id}>{station.name}</option>
              ))}
            </select>
            <button
              type="button"
              onClick={onRefresh}
              className="btn-ghost-sm px-3 py-1.5 text-slate-600 flex items-center gap-1.5 text-xs"
            >
              <RefreshCw className="w-3 h-3" />
              Refresh
            </button>
            <span className="text-[10px] uppercase tracking-widest text-slate-400 font-semibold">{recent.length} rows</span>
          </div>
        </div>
        <div className="h-28 bg-gradient-to-r from-slate-950 via-indigo-950 to-slate-900 relative">
          <div className="absolute inset-0 opacity-50 flex items-end justify-around px-2 pb-2 gap-0.5">
            {barHeights.map((h, i) => (
              <div
                key={i}
                className="w-1 rounded-t bg-indigo-300/90"
                style={{ height: `${Math.max(10, Math.round(h * 72))}px` }}
              />
            ))}
          </div>
          <div className="absolute inset-x-0 bottom-0 h-10 bg-gradient-to-t from-black/55 to-transparent" />
          <div className="absolute left-4 top-3 text-[10px] text-white/70 font-medium tracking-widest uppercase">
            Match activity (from recent logs)
          </div>
        </div>
      </div>

      <div className="rm-card p-5 lg:p-7">
        <div className="overflow-x-auto rounded-2xl border border-slate-200">
          <table className="w-full text-left table-fixed min-w-[720px] text-sm">
            <thead>
              <tr className="border-b border-slate-200 bg-slate-50">
                <th className="py-3 px-4 rm-section-label w-[18%]">Time</th>
                <th className="py-3 px-4 rm-section-label w-[18%]">Station</th>
                <th className="py-3 px-4 rm-section-label w-[44%]">Track</th>
                <th className="py-3 px-4 rm-section-label w-[12%]">Method</th>
                <th className="py-3 px-4 rm-section-label w-[8%]">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {recent.map((log) => {
                const fusionLine = formatFusionV2Line(parseFusionV2FromDiagnostics(log.matchDiagnosticsJson));
                return (
                <tr key={log.id} className="hover:bg-slate-50 transition-colors group">
                  <td className="py-3 px-4 text-slate-500 text-xs whitespace-nowrap">{new Date(log.observedAt).toLocaleString()}</td>
                  <td className="py-3 px-4 font-medium text-slate-800 truncate pr-2" title={log.station?.name || stationNameById.get(log.stationId) || 'Unknown'}>
                    {log.station?.name || stationNameById.get(log.stationId) || 'Unknown'}
                  </td>
                  <td className="py-3 px-4 min-w-0">
                    <div className="font-medium text-slate-900 truncate group-hover:text-rm-indigo transition-colors" title={log.titleFinal || 'Unknown track'}>
                      {log.titleFinal || 'Unknown track'}
                    </div>
                    <div className="text-xs text-slate-500 truncate" title={log.artistFinal || ''}>{log.artistFinal || ''}</div>
                    {fusionLine && (
                      <div className="text-[10px] text-indigo-700/80 truncate" title={fusionLine}>
                        Fusion · {fusionLine}
                      </div>
                    )}
                  </td>
                  <td className="py-3 px-4">
                    <span className="px-2 py-0.5 bg-rm-indigo-soft text-rm-indigo rounded-md text-[10px] uppercase font-bold tracking-wide">
                      {formatMethod(log.detectionMethod)}
                    </span>
                  </td>
                  <td className="py-3 px-4">
                    {log.status === 'matched' ? (
                      <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                    ) : (
                      <AlertCircle className="w-4 h-4 text-amber-600" />
                    )}
                  </td>
                </tr>
                );
              })}
              {!loading && recent.length === 0 && totalLogCount > 0 && (
                <tr>
                  <td colSpan={5} className="py-12 text-center text-slate-500 text-sm">
                    No rows match the header filter for this station scope.
                  </td>
                </tr>
              )}
              {!loading && recent.length === 0 && totalLogCount === 0 && (
                <tr>
                  <td colSpan={5} className="py-12 text-center text-slate-500 text-sm">
                    No recent activity yet. Open History for the full log or probe a station.
                  </td>
                </tr>
              )}
              {loading && (
                <tr>
                  <td colSpan={5} className="py-10 text-center text-slate-500 text-sm">
                    Loading activity…
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

import React from 'react';
import { Activity, CheckCircle2, AlertCircle } from 'lucide-react';
import type { DetectionLog } from '../types/dashboard';
import { formatMethod } from '../lib/dashboard-format';

export function ActivityPage({
  logs,
  loading,
  stationNameById,
}: {
  logs: DetectionLog[];
  loading: boolean;
  stationNameById: Map<string, string>;
}) {
  const recent = logs.slice(0, 24);

  return (
    <div className="space-y-6">
      <div className="rm-card overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Activity className="w-5 h-5 text-rm-indigo" />
            <div>
              <h2 className="text-base font-semibold text-slate-900">Live activity</h2>
              <p className="text-xs text-slate-500">Latest detections across all stations (mirrors History feed).</p>
            </div>
          </div>
          <span className="text-[10px] uppercase tracking-widest text-slate-400 font-semibold">{recent.length} rows</span>
        </div>
        <div className="h-28 bg-gradient-to-r from-slate-950 via-indigo-950 to-slate-900 relative">
          <div className="absolute inset-0 opacity-40 flex items-end justify-around px-2 pb-2 gap-0.5">
            {Array.from({ length: 48 }).map((_, i) => (
              <div
                key={i}
                className="w-1 rounded-t bg-indigo-300/80"
                style={{ height: `${20 + ((i * 17) % 55)}px` }}
              />
            ))}
          </div>
          <div className="absolute inset-x-0 bottom-0 h-10 bg-gradient-to-t from-black/50 to-transparent" />
          <div className="absolute left-4 top-3 text-[10px] text-white/70 font-medium tracking-widest uppercase">
            Spectrum (decorative)
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
              {recent.map((log) => (
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
              ))}
              {!loading && recent.length === 0 && (
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

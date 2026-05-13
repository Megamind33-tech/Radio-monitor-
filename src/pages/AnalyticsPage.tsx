import React from 'react';
import { BarChart3, RefreshCw } from 'lucide-react';
import type { SongSpinRow, Station } from '../types/dashboard';

export function AnalyticsPage({
  songSpins,
  totalSpinCount,
  stations,
  analyticsLoading,
  onRefresh,
}: {
  songSpins: SongSpinRow[];
  /** Unfiltered count from API (for empty vs filter-empty messaging). */
  totalSpinCount: number;
  stations: Station[];
  analyticsLoading: boolean;
  onRefresh: () => void;
}) {
  return (
    <div className="rm-card p-6 lg:p-8 space-y-5">
      <div className="flex flex-wrap justify-between items-center gap-4">
        <div>
          <h2 className="text-lg font-semibold flex items-center gap-2">
            <BarChart3 className="w-5 h-5 text-rm-indigo" />
            Song Spins — All Stations
          </h2>
          <p className="text-sm text-slate-500 mt-1">Per-station CSV exports are on each station page.</p>
        </div>
        <button
          type="button"
          onClick={onRefresh}
          className="btn-ghost px-3 py-2 text-sm text-slate-600 flex items-center gap-1.5"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          Refresh
        </button>
      </div>

      <div className="overflow-x-auto rounded-2xl border border-slate-200">
        <table className="w-full text-left text-sm table-fixed min-w-[960px]">
          <thead>
            <tr className="border-b border-slate-200 bg-slate-50">
              <th className="py-3 px-4 rm-section-label w-[20%]">Station</th>
              <th className="py-3 px-4 rm-section-label w-[12%]">Province</th>
              <th className="py-3 px-4 rm-section-label w-[12%]">District</th>
              <th className="py-3 px-4 rm-section-label w-[22%]">Title</th>
              <th className="py-3 px-4 rm-section-label w-[16%]">Artist</th>
              <th className="py-3 px-4 rm-section-label w-[12%]">Album</th>
              <th className="py-3 px-4 rm-section-label text-right w-[6%]">Plays</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {analyticsLoading && totalSpinCount === 0 ? (
              <tr>
                <td colSpan={7} className="py-10 text-center text-slate-500">Loading analytics…</td>
              </tr>
            ) : (
              songSpins.map((row, i) => {
                const st = stations.find((s) => s.id === row.stationId);
                return (
                  <tr key={`${row.stationId}-${row.title}-${i}`} className="hover:bg-slate-50 transition-colors">
                    <td className="py-3 px-4 font-medium text-slate-800 truncate pr-2" title={st?.name ?? row.stationId}>{st?.name ?? row.stationId}</td>
                    <td className="py-3 px-4 text-slate-500 truncate pr-2" title={st?.province || '—'}>{st?.province || '—'}</td>
                    <td className="py-3 px-4 text-slate-500 truncate pr-2" title={st?.district || '—'}>{st?.district || '—'}</td>
                    <td className="py-3 px-4 text-slate-800 truncate pr-2" title={row.title || '—'}>{row.title || '—'}</td>
                    <td className="py-3 px-4 text-slate-600 truncate pr-2" title={row.artist || '—'}>{row.artist || '—'}</td>
                    <td className="py-3 px-4 text-slate-500 truncate pr-2" title={row.album || '—'}>{row.album || '—'}</td>
                    <td className="py-3 px-4 text-right font-mono text-slate-700">{row.playCount}</td>
                  </tr>
                );
              })
            )}
            {!analyticsLoading && songSpins.length === 0 && totalSpinCount > 0 && (
              <tr>
                <td colSpan={7} className="py-12 text-center text-slate-500 text-sm">
                  No spins match the current search. Clear the header filter to see all loaded rows.
                </td>
              </tr>
            )}
            {!analyticsLoading && songSpins.length === 0 && totalSpinCount === 0 && (
              <tr>
                <td colSpan={7} className="py-12 text-center text-slate-500 text-sm">
                  No matched detections yet. Leave the monitor running — spins appear as tracks are logged.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

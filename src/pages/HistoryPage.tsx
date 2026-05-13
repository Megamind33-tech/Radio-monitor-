import React from 'react';
import { History, RefreshCw, CheckCircle2, AlertCircle } from 'lucide-react';
import type { DetectionLog, Station } from '../types/dashboard';
import { formatMethod } from '../lib/dashboard-format';

export function HistoryPage({
  logs,
  totalLogCount,
  historyLoading,
  selectedStationId,
  orderedStations,
  stationNameById,
  onStationChange,
  onRefresh,
}: {
  logs: DetectionLog[];
  totalLogCount: number;
  historyLoading: boolean;
  selectedStationId: string;
  orderedStations: Station[];
  stationNameById: Map<string, string>;
  onStationChange: (id: string) => void;
  onRefresh: () => void;
}) {
  return (
    <div className="rm-card p-6 lg:p-8 space-y-5">
      <div className="flex flex-wrap justify-between items-center gap-4">
        <h2 className="text-lg font-semibold flex items-center gap-2">
          <History className="w-5 h-5 text-rm-indigo" />
          Station Airplay Timeline
        </h2>
        <div className="flex gap-2">
          <select
            value={selectedStationId}
            onChange={(event) => onStationChange(event.target.value)}
            className="rm-input bg-slate-100 px-3 py-1.5 text-sm"
          >
            <option value="all">All Stations</option>
            {orderedStations.map((station) => (
              <option key={station.id} value={station.id}>{station.name}</option>
            ))}
          </select>
          <button
            type="button"
            onClick={onRefresh}
            className="btn-ghost-sm px-3 py-1.5 text-slate-600 flex items-center gap-1.5"
          >
            <RefreshCw className="w-3 h-3" />
            Refresh
          </button>
        </div>
      </div>

      <div className="overflow-x-auto rounded-2xl border border-slate-200">
        <table className="w-full text-left table-fixed min-w-[960px] text-sm">
          <thead>
            <tr className="border-b border-slate-200 bg-slate-50">
              <th className="py-3 px-4 rm-section-label w-[18%]">Time</th>
              <th className="py-3 px-4 rm-section-label w-[18%]">Station</th>
              <th className="py-3 px-4 rm-section-label w-[42%]">Track</th>
              <th className="py-3 px-4 rm-section-label w-[13%]">Method</th>
              <th className="py-3 px-4 rm-section-label w-[9%]">Status</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {logs.map((log) => (
              <tr key={log.id} className="hover:bg-slate-50 transition-colors group">
                <td className="py-3.5 px-4 text-slate-500 text-xs whitespace-nowrap">{new Date(log.observedAt).toLocaleString()}</td>
                <td className="py-3.5 px-4 font-medium text-slate-800 truncate pr-2" title={log.station?.name || stationNameById.get(log.stationId) || 'Unknown'}>
                  {log.station?.name || stationNameById.get(log.stationId) || 'Unknown'}
                </td>
                <td className="py-3.5 px-4 min-w-0">
                  <div className="font-medium text-slate-900 truncate group-hover:text-rm-indigo transition-colors" title={log.titleFinal || 'Unknown track'}>
                    {log.titleFinal || 'Unknown track'}
                  </div>
                  <div className="text-xs text-slate-500 truncate" title={log.artistFinal || ''}>{log.artistFinal || ''}</div>
                  {(log.genreFinal || log.sourceProvider) && (
                    <div className="text-[10px] text-slate-400">
                      {log.genreFinal}{log.genreFinal && log.sourceProvider ? ' · ' : ''}{log.sourceProvider ? `via ${log.sourceProvider}` : ''}
                    </div>
                  )}
                </td>
                <td className="py-3.5 px-4">
                  <span className="px-2 py-0.5 bg-rm-indigo-soft text-rm-indigo rounded-md text-[10px] uppercase font-bold tracking-wide">
                    {formatMethod(log.detectionMethod)}
                  </span>
                </td>
                <td className="py-3.5 px-4">
                  {log.status === 'matched' ? (
                    <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                  ) : (
                    <AlertCircle className="w-4 h-4 text-amber-600" />
                  )}
                </td>
              </tr>
            ))}
            {!historyLoading && logs.length === 0 && totalLogCount > 0 && (
              <tr>
                <td colSpan={5} className="py-12 text-center text-slate-500 text-sm">
                  No logs match the current header filter for this station scope. Clear the search to see all loaded rows.
                </td>
              </tr>
            )}
            {!historyLoading && logs.length === 0 && totalLogCount === 0 && (
              <tr>
                <td colSpan={5} className="py-12 text-center text-slate-500 text-sm">
                  No airplay detections yet. Probe a station to create logs.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

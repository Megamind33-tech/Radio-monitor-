import type { DetectionLog, SongSpinRow, Station } from '../types/dashboard';
import { formatMethod } from './dashboard-format';

export interface RailTaskItem {
  id: string;
  title: string;
  due: string;
  done?: boolean;
}

export interface RailActivityLine {
  id: string;
  label: string;
  detail: string;
  tone: 'ok' | 'warn' | 'idle';
}

export function formatRelativeTime(iso: string): string {
  const t = new Date(iso).getTime();
  if (!Number.isFinite(t)) return '—';
  const d = Math.max(0, Date.now() - t);
  const sec = Math.floor(d / 1000);
  if (sec < 45) return 'just now';
  const m = Math.floor(d / 60000);
  if (m < 1) return '<1m ago';
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const days = Math.floor(h / 24);
  if (days < 14) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}

export function provinceMixFromStations(stations: Station[]): { name: string; pct: number }[] {
  if (!stations.length) return [{ name: 'No stations', pct: 100 }];
  const counts = new Map<string, number>();
  for (const s of stations) {
    const p = (s.province || '').trim() || 'Unspecified';
    counts.set(p, (counts.get(p) ?? 0) + 1);
  }
  const total = stations.length;
  const rows = [...counts.entries()].sort((a, b) => b[1] - a[1]);
  const top = rows.slice(0, 5);
  const shown = top.reduce((a, [, c]) => a + c, 0);
  const rest = total - shown;
  const out = top.map(([name, c]) => ({ name, pct: Math.round((c / total) * 100) }));
  if (rest > 0 && rows.length > 5) {
    out.push({ name: 'Other', pct: Math.round((rest / total) * 100) });
  }
  return out;
}

export function railTasksFromDashboard(opts: {
  unknownTotal: number;
  degradedCount: number;
  rematchPending?: number;
  untaggedAudio?: number;
}): RailTaskItem[] {
  const tasks: RailTaskItem[] = [];
  if (opts.unknownTotal > 0) {
    tasks.push({
      id: 'unknown-q',
      title: `Review unknown sample queue (${opts.unknownTotal})`,
      due: 'Ops',
    });
  }
  if (opts.degradedCount > 0) {
    tasks.push({
      id: 'degraded',
      title: `Check degraded streams (${opts.degradedCount})`,
      due: 'Live ops',
    });
  }
  const rp = opts.rematchPending ?? 0;
  if (rp > 0) {
    tasks.push({
      id: 'rematch',
      title: `Self-healing rematch pending (${rp})`,
      due: 'System',
    });
  }
  if ((opts.untaggedAudio ?? 0) > 0) {
    tasks.push({
      id: 'audio',
      title: `Audio editor — needs tagging (${opts.untaggedAudio})`,
      due: 'Review',
    });
  }
  if (tasks.length === 0) {
    tasks.push({ id: 'clear', title: 'No blocking operator tasks', due: 'All clear', done: true });
  }
  return tasks.slice(0, 5);
}

export function railRecentFromLogs(
  logs: DetectionLog[],
  stationNameById: Map<string, string>,
  limit: number
): RailActivityLine[] {
  const slice = logs.slice(0, limit);
  return slice.map((log) => {
    const st = log.station?.name || stationNameById.get(log.stationId) || 'Unknown';
    const rel = formatRelativeTime(log.observedAt);
    const title = log.titleFinal || 'Unknown track';
    const shortTitle = title.length > 42 ? `${title.slice(0, 40)}…` : title;
    return {
      id: log.id,
      label: formatMethod(log.detectionMethod),
      detail: `${rel} · ${st} · ${shortTitle}`,
      tone: log.status === 'matched' ? 'ok' : 'warn',
    };
  });
}

/** Bar heights 0–1 for activity spectrum from recent logs (matched = taller). */
export function activitySpectrumFromLogs(logs: DetectionLog[], count: number): number[] {
  const out: number[] = [];
  const slice = logs.slice(0, count);
  for (let i = 0; i < count; i++) {
    const log = slice[i % Math.max(slice.length, 1)] ?? null;
    if (!log) {
      out.push(0.15 + ((i * 13) % 20) / 100);
      continue;
    }
    const base = log.status === 'matched' ? 0.55 : 0.28;
    const jitter = ((i * 17 + (log.id?.length ?? 0) * 3) % 25) / 100;
    out.push(Math.min(0.95, base + jitter));
  }
  return out;
}

export function calendarMonthLabel(d = new Date()): string {
  return new Intl.DateTimeFormat('en', { month: 'long', year: 'numeric' }).format(d);
}

export interface CalendarCell {
  key: string;
  d: number | null;
  muted?: boolean;
  highlight?: boolean;
}

export function filterDetectionLogs(
  logs: DetectionLog[],
  q: string,
  stationNameById: Map<string, string>
): DetectionLog[] {
  const s = q.trim().toLowerCase();
  if (!s) return logs;
  return logs.filter((log) => {
    const st = (log.station?.name || stationNameById.get(log.stationId) || '').toLowerCase();
    return (
      st.includes(s) ||
      (log.titleFinal || '').toLowerCase().includes(s) ||
      (log.artistFinal || '').toLowerCase().includes(s) ||
      (log.detectionMethod || '').toLowerCase().includes(s) ||
      (log.status || '').toLowerCase().includes(s) ||
      (log.sourceProvider || '').toLowerCase().includes(s)
    );
  });
}

export function filterSongSpins(
  rows: SongSpinRow[],
  q: string,
  stations: Station[]
): SongSpinRow[] {
  const s = q.trim().toLowerCase();
  if (!s) return rows;
  const stName = (id: string) => (stations.find((x) => x.id === id)?.name || id).toLowerCase();
  return rows.filter((row) => {
    const sn = stName(row.stationId);
    const st = stations.find((x) => x.id === row.stationId);
    const prov = (st?.province || '').toLowerCase();
    const dist = (st?.district || '').toLowerCase();
    return (
      sn.includes(s) ||
      prov.includes(s) ||
      dist.includes(s) ||
      (row.title || '').toLowerCase().includes(s) ||
      (row.artist || '').toLowerCase().includes(s) ||
      (row.album || '').toLowerCase().includes(s)
    );
  });
}

export function calendarCellsForMonth(d = new Date()): CalendarCell[] {
  const y = d.getFullYear();
  const m = d.getMonth();
  const first = new Date(y, m, 1);
  const last = new Date(y, m + 1, 0);
  const today = d.getDate();
  const startPad = first.getDay();
  const cells: CalendarCell[] = [];
  const prevLast = new Date(y, m, 0).getDate();
  for (let i = 0; i < startPad; i++) {
    const dayNum = prevLast - startPad + i + 1;
    cells.push({ key: `p-${i}`, d: dayNum, muted: true });
  }
  for (let day = 1; day <= last.getDate(); day++) {
    cells.push({
      key: `c-${day}`,
      d: day,
      highlight: day === today,
    });
  }
  const tail = 42 - cells.length;
  for (let i = 0; i < tail; i++) {
    cells.push({ key: `t-${i}`, d: null, muted: true });
  }
  return cells.slice(0, 42);
}

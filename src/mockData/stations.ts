/**
 * Prototype-style mock shell data for the dashboard Right Rail and demos.
 * Live station rows still come from `/api/stations`; this file is layout chrome only.
 */

export interface MockStationRow {
  id: string;
  name: string;
  country: string;
  province?: string;
  status: 'live' | 'degraded' | 'offline';
  nowPlaying?: string;
}

export const mockStations: MockStationRow[] = [
  { id: 'zm-001', name: 'ZNBC Radio 1', country: 'Zambia', province: 'Lusaka', status: 'live', nowPlaying: 'Local hits · ICY' },
  { id: 'zm-002', name: 'Phoenix FM', country: 'Zambia', province: 'Copperbelt', status: 'live', nowPlaying: 'Urban rotation' },
  { id: 'zm-003', name: 'Hot FM', country: 'Zambia', province: 'Lusaka', status: 'degraded', nowPlaying: 'Weak metadata' },
  { id: 'mz-001', name: 'LM Radio', country: 'Mozambique', province: 'Maputo', status: 'live', nowPlaying: 'Lusophone pop' },
  { id: 'mz-002', name: 'Radio Moçambique', country: 'Mozambique', province: 'Nampula', status: 'offline' },
];

export interface RailTask {
  id: string;
  title: string;
  due: string;
  done?: boolean;
}

export const railTasks: RailTask[] = [
  { id: 't1', title: 'Review unknown fingerprint queue', due: 'Today' },
  { id: 't2', title: 'Export weekly spins (ASCAP-ready)', due: 'Fri' },
  { id: 't3', title: 'Verify crawler rejections', due: 'Next week', done: true },
];

export interface RailActivity {
  id: string;
  label: string;
  detail: string;
  tone: 'ok' | 'warn' | 'idle';
}

export const railRecentActivity: RailActivity[] = [
  { id: 'a1', label: 'AcoustID hit', detail: '2m ago · ZNBC Radio 1', tone: 'ok' },
  { id: 'a2', label: 'Local library teach', detail: '6m ago · Phoenix FM', tone: 'ok' },
  { id: 'a3', label: 'Catalog crawl', detail: '12m ago · +3 media URLs', tone: 'idle' },
  { id: 'a4', label: 'Stream degraded', detail: '18m ago · Hot FM', tone: 'warn' },
];

export const railCalendarMonth = 'May 2026';

export const railCalendarDays: Array<{ d: number; muted?: boolean; highlight?: boolean }> = [
  { d: 1, muted: true },
  { d: 2 },
  { d: 3 },
  { d: 4 },
  { d: 5 },
  { d: 6 },
  { d: 7 },
  { d: 8 },
  { d: 9 },
  { d: 10 },
  { d: 11 },
  { d: 12 },
  { d: 13, highlight: true },
  { d: 14 },
  { d: 15 },
  { d: 16 },
  { d: 17 },
  { d: 18 },
  { d: 19 },
  { d: 20 },
  { d: 21 },
  { d: 22 },
  { d: 23 },
  { d: 24 },
  { d: 25 },
  { d: 26 },
  { d: 27 },
  { d: 28 },
  { d: 29 },
  { d: 30 },
  { d: 31 },
];

export const provinceBreakdown = [
  { name: 'Lusaka', pct: 38 },
  { name: 'Copperbelt', pct: 27 },
  { name: 'Central', pct: 12 },
  { name: 'Southern', pct: 11 },
  { name: 'Other', pct: 12 },
];

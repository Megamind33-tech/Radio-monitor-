import type { Station, StationListFilter } from '../types/dashboard';

export const REQUESTED_STATION_PRIORITY: string[] = [
  'znbc radio 1',
  'znbc radio 2',
  'znbc radio 4',
  'hot fm',
  'hone fm',
  'yar fm',
  'chikuni',
  'phoenix',
  'faith radio',
  'chongwe radio',
  'ichengelo',
  'oblate radio',
  'unza',
  'zamcom',
  'kwithu',
  'rock fm',
  'money fm',
  'pan african radio',
  'komboni radio',
  'chimwemwe radio',
  'rooster fm',
  'power fm',
];

export function normalizeStationName(value: string): string {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

export function formatMethod(method: string) {
  if (method === 'stream_metadata') return 'Metadata';
  if (method === 'fingerprint_acoustid') return 'AcoustID';
  if (method === 'fingerprint_local') return 'Local library';
  if (method === 'catalog_lookup') return 'Catalog';
  return method;
}

export function parseStationHash(): string | null {
  if (typeof window === 'undefined') return null;
  const match = window.location.hash.match(/^#\/stations\/([^?]+)/);
  return match ? decodeURIComponent(match[1]) : null;
}

export function setStationHash(stationId?: string) {
  if (typeof window === 'undefined') return;
  window.location.hash = stationId ? `#/stations/${encodeURIComponent(stationId)}` : '#/stations';
}

export function stationGroup(station: Station): Exclude<StationListFilter, 'all'> {
  if (!station.isActive || station.monitorState === 'INACTIVE') return 'inactive';
  if (station.monitorState === 'DEGRADED') return 'degraded';
  if (!station.monitorState || station.monitorState === 'UNKNOWN') return 'unknown';
  return 'running';
}

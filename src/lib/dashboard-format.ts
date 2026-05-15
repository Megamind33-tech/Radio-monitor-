import type { FusionV2Diagnostics, Station, StationListFilter } from '../types/dashboard';

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

export function parseFusionV2FromDiagnostics(
  matchDiagnosticsJson: string | null | undefined
): FusionV2Diagnostics | null {
  if (!matchDiagnosticsJson) return null;
  try {
    const j = JSON.parse(matchDiagnosticsJson) as { fusionV2?: FusionV2Diagnostics };
    const f = j.fusionV2;
    return f && typeof f === 'object' ? f : null;
  } catch {
    return null;
  }
}

/** One-line operator hint from fusionV2 diagnostics blob. */
export function formatFusionV2Line(f: FusionV2Diagnostics | null): string | null {
  if (!f) return null;
  const parts: string[] = [];
  if (f.winningType) parts.push(`win: ${f.winningType}`);
  if (typeof f.conflicts === 'number' && f.conflicts > 0) parts.push(`${f.conflicts} lane conflict(s)`);
  if (f.secondPassCatalogApplied) parts.push('2nd-pass catalog');
  const laneTypes = (f.lanes ?? []).map((l) => l.type);
  if (laneTypes.length) parts.push(`lanes: ${laneTypes.join(', ')}`);
  return parts.length ? parts.join(' · ') : null;
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

import React from 'react';
import { Settings } from 'lucide-react';
import type { DependencyStatus, UnknownStorageSummary } from '../types/dashboard';

export function SettingsPage({
  dependencies,
  crawlerStatus,
  rematchSummary,
  unknownStorage,
  unresolvedRecovery,
  storageLoading,
  storageError,
  storageDryRun,
  hashBackfillResult,
  includeHiddenStations,
  onRefreshCrawler,
  onRefreshRematch,
  onRefreshUnresolvedRecovery,
  onStorageError,
  onHashBackfillResult,
  onStorageDryRun,
  onRefetchUnknownStorage,
  onIncludeHiddenChange,
  onRecheckDependencies,
  onRefreshStations,
}: {
  dependencies: DependencyStatus | null;
  crawlerStatus: any;
  rematchSummary: any;
  unknownStorage: UnknownStorageSummary | null;
  unresolvedRecovery: Record<string, unknown> | null;
  storageLoading: boolean;
  storageError: string | null;
  storageDryRun: any;
  hashBackfillResult: any;
  includeHiddenStations: boolean;
  onRefreshCrawler: () => void;
  onRefreshRematch: () => void;
  onRefreshUnresolvedRecovery: () => Promise<void>;
  onStorageError: (msg: string | null) => void;
  onHashBackfillResult: (v: any) => void;
  onStorageDryRun: (v: any) => void;
  onRefetchUnknownStorage: () => Promise<void>;
  onIncludeHiddenChange: (v: boolean) => void;
  onRecheckDependencies: () => void;
  onRefreshStations: () => void;
}) {
  return (
    <div className="max-w-2xl rm-card p-8">
      <h2 className="text-lg font-semibold mb-6 flex items-center gap-2 text-slate-900">
        <Settings className="w-5 h-5 text-rm-indigo" />
        Environment & Fingerprint Readiness
      </h2>

      <div className="space-y-6">
        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between">
            <span className="font-semibold text-slate-800">Fingerprint pipeline</span>
            <span className={`px-2.5 py-1 rounded-lg text-xs font-bold ${dependencies?.fingerprintReady ? 'bg-emerald-100 text-emerald-800 border border-emerald-200' : 'bg-amber-50 text-amber-900 border border-amber-200'}`}>
              {dependencies?.fingerprintReady ? 'READY' : 'NEEDS SETUP'}
            </span>
          </div>
          <div className="grid grid-cols-2 gap-2 text-sm text-slate-700">
            <div>ffmpeg: {dependencies?.ffmpeg ? 'OK' : 'Missing'}</div>
            <div>ffprobe: {dependencies?.ffprobe ? 'OK' : 'Missing'}</div>
            <div>fpcalc: {dependencies?.fpcalc ? 'OK' : 'Missing'}</div>
            <div>AcoustID key: {dependencies?.acoustidApiKeyConfigured ? 'Configured' : 'Missing'}</div>
          </div>
          <div className="grid grid-cols-1 gap-2 text-xs text-slate-600 border-t border-slate-100 pt-3">
            <div>
              Free APIs active: AcoustID ({dependencies?.freeApisEnabled?.acoustid ? 'on' : 'off'}), MusicBrainz (
              {dependencies?.freeApisEnabled?.musicbrainz ? 'on' : 'off'}), iTunes Search (
              {dependencies?.freeApisEnabled?.itunesSearch ? 'on' : 'off'}), Deezer Search (
              {dependencies?.freeApisEnabled?.deezerSearch !== false ? 'on' : 'off'})
            </div>
            <div>Catalog lookup fallback: {dependencies?.catalogLookupReady ? 'ready' : 'needs MusicBrainz user-agent'}</div>
          </div>
          {dependencies && dependencies.missing.length > 0 && (
            <p className="text-xs text-amber-800">
              Missing: {dependencies.missing.join(', ')}
            </p>
          )}
        </div>

        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between">
            <span className="font-semibold text-slate-800">Crawler / Catalog Status</span>
            <button type="button" onClick={onRefreshCrawler} className="btn-ghost-sm px-2.5 py-1 text-slate-600">Refresh</button>
          </div>
          {crawlerStatus ? (
            <>
              <div className="grid grid-cols-2 gap-2 text-xs text-slate-700">
                <div>Total discovered URLs: {crawlerStatus.total}</div>
                <div>Fingerprinted rows: {(crawlerStatus.byFp || []).find((x: any) => x.fingerprintStatus === 'fingerprinted')?._count?._all ?? 0}</div>
                <div>Failed fingerprints: {(crawlerStatus.byFp || []).find((x: any) => x.fingerprintStatus === 'failed')?._count?._all ?? 0}</div>
                <div>Rejected non-media: {(crawlerStatus.byClass || []).filter((x: any) => ['news','sports','generic_html','non_media','unsupported'].includes(x.classification)).reduce((a: number, b: any) => a + (b._count?._all || 0), 0)}</div>
                <div>Alternate sources linked: {crawlerStatus.altSources ?? 0}</div>
                <div>Possible duplicates (review): {crawlerStatus.possibleDupes ?? 0}</div>
              </div>
              <p className="text-xs text-slate-600">Top failing reasons: {(crawlerStatus.failures || []).slice(0, 5).map((x: any) => `${x.failureReason}:${x._count?._all || 0}`).join(', ') || 'none'}</p>
            </>
          ) : (
            <p className="text-xs text-slate-500">No crawler status loaded yet.</p>
          )}
        </div>

        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between">
            <span className="font-semibold text-slate-800">Self-Healing Rematch</span>
            <button type="button" onClick={onRefreshRematch} className="btn-ghost-sm px-2.5 py-1 text-slate-600">Refresh</button>
          </div>
          <p className="text-xs text-slate-600">Human-verified logs are protected. Dry-run does not change logs. Strong fingerprint evidence required for automatic correction.</p>
          {rematchSummary ? <div className="grid grid-cols-2 gap-2 text-xs text-slate-700"><div>Pending: {rematchSummary.pending ?? 0}</div><div>Matched: {rematchSummary.matched ?? 0}</div><div>Needs review: {rematchSummary.needs_review ?? 0}</div><div>Failed: {rematchSummary.failed ?? 0}</div></div> : <p className="text-xs text-slate-500">No rematch summary loaded.</p>}
        </div>

        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between">
            <span className="font-semibold text-slate-800">Unresolved recovery (honest backlog)</span>
            <button
              type="button"
              onClick={() => void onRefreshUnresolvedRecovery()}
              className="btn-ghost-sm px-2.5 py-1 text-slate-600"
            >
              Refresh
            </button>
          </div>
          <p className="text-xs text-slate-600">
            Status codes separate fingerprint misses from title-only evidence, weak stream-learned library hits,
            catalogue gaps, and programme/junk metadata. CLI: <code className="text-slate-700">npm run audit:unresolved-recovery</code>
          </p>
          {unresolvedRecovery ? (
            <div className="space-y-3 text-xs text-slate-700">
              <div className="grid grid-cols-2 gap-2">
                <div>24h created: {(unresolvedRecovery.flow24h as { created?: number })?.created ?? "—"}</div>
                <div>24h recovered: {(unresolvedRecovery.flow24h as { recovered?: number })?.recovered ?? "—"}</div>
              </div>
              <div>
                <div className="font-medium text-slate-800 mb-1">By recoveryStatus</div>
                <ul className="list-disc list-inside space-y-0.5">
                  {Object.entries((unresolvedRecovery.totals as Record<string, number>) || {}).map(([k, v]) => (
                    <li key={k}>
                      {k}: {v}
                    </li>
                  ))}
                </ul>
              </div>
              <div>
                <div className="font-medium text-slate-800 mb-1">By recoveryReason (semantic lane)</div>
                <ul className="list-disc list-inside space-y-0.5 max-h-40 overflow-y-auto">
                  {Object.entries((unresolvedRecovery.byReason as Record<string, number>) || {})
                    .sort((a, b) => b[1] - a[1])
                    .map(([k, v]) => (
                      <li key={k}>
                        {k}: {v}
                      </li>
                    ))}
                </ul>
              </div>
            </div>
          ) : (
            <p className="text-xs text-slate-500">Load status to see lane breakdown.</p>
          )}
        </div>
        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between">
            <span className="font-semibold text-slate-800">Paid audio fallbacks (AudD / ACRCloud)</span>
            <span
              className={`px-2 py-1 rounded-lg text-xs font-bold ${
                !dependencies?.paidApis
                  ? 'bg-slate-100 text-slate-500'
                  : !dependencies.paidApis.paidFallbacksEnabled
                    ? 'bg-slate-100 text-slate-600'
                    : dependencies.paidApis.paidLaneReady
                      ? 'bg-emerald-100 text-emerald-800'
                      : 'bg-amber-500/20 text-amber-200'
              }`}
            >
              {!dependencies?.paidApis
                ? '…'
                : !dependencies.paidApis.paidFallbacksEnabled
                  ? 'DISABLED'
                  : dependencies.paidApis.paidLaneReady
                    ? 'READY'
                    : 'NO KEYS'}
            </span>
          </div>
          <p className="text-xs text-slate-500">
            Used only after <strong className="text-slate-600">local + AcoustID</strong> miss when ICY looks like slogans /
            programmes (not normal song titles). Same binaries as fingerprint pipeline (ffmpeg, fpcalc).
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm text-slate-700">
            <div>
              AudD token:{' '}
              {dependencies?.paidApis?.auddConfigured ? (
                <span className="text-emerald-700">configured</span>
              ) : (
                <span className="text-slate-500">not set</span>
              )}
            </div>
            <div>
              ACRCloud:{' '}
              {dependencies?.paidApis?.acrcloudConfigured ? (
                <span className="text-emerald-700">host + keys</span>
              ) : (
                <span className="text-slate-500">not set</span>
              )}
            </div>
            <div className="sm:col-span-2 text-xs text-slate-500">
              Env: <code className="text-slate-600">AUDD_API_TOKEN</code> ·{' '}
              <code className="text-slate-600">ACRCLOUD_HOST</code>,{' '}
              <code className="text-slate-600">ACRCLOUD_ACCESS_KEY</code>,{' '}
              <code className="text-slate-600">ACRCLOUD_ACCESS_SECRET</code> · optional{' '}
              <code className="text-slate-600">PAID_AUDIO_FALLBACKS_ENABLED=false</code> to disable paid calls entirely.
            </div>
          </div>
          {dependencies?.integrationNotes && dependencies.integrationNotes.length > 0 && (
            <ul className="text-xs text-amber-200/90 space-y-1 list-disc list-inside border-t border-slate-100 pt-3">
              {dependencies.integrationNotes.map((note, i) => (
                <li key={i}>{note}</li>
              ))}
            </ul>
          )}
        </div>

        <div className="rm-card-inner p-5 space-y-4">
          <div className="flex items-center justify-between">
            <span className="font-semibold text-slate-800">Unknown Sample Storage</span>
            <span className="px-2.5 py-1 rounded-lg text-[11px] font-bold bg-amber-50 text-amber-900 border border-amber-200">
              Dry Run Only — No files deleted
            </span>
          </div>
          {storageError ? <p className="text-xs text-red-700">{storageError}</p> : null}
          {storageLoading ? <p className="text-xs text-slate-500">Loading storage summary…</p> : null}
          {unknownStorage && (
            <>
              <div className="grid grid-cols-2 gap-2 text-xs text-slate-700">
                <div>Total unknown: {unknownStorage.totalUnknownSampleCount}</div>
                <div>With audio: {unknownStorage.countWithAudioFile}</div>
                <div>Missing audio: {unknownStorage.countMissingAudioFile}</div>
                <div>Audio bytes: {unknownStorage.totalAudioBytes.toLocaleString()}</div>
                <div>Human verified: {unknownStorage.humanVerifiedCount}</div>
                <div>Fingerprinted: {unknownStorage.fingerprintedCount}</div>
                <div>Fingerprint failed: {unknownStorage.fingerprintFailedCount}</div>
                <div>Eligible: {unknownStorage.eligibleForPurgeCount}</div>
                <div>Blocked missing hash: {(storageDryRun?.blockedReasonsSummary?.missing_file_hash as number | undefined) ?? '—'}</div>
              </div>
              <p className="text-xs text-slate-600">
                Estimated reclaimable: {unknownStorage.estimatedBytesReclaimable.toLocaleString()} bytes
              </p>
              <p className="text-xs text-amber-200">
                No files deleted. This only records sha256 and file size for audit safety.
              </p>
              <div className="overflow-x-auto border border-slate-200 rounded-xl">
                <table className="w-full text-xs min-w-[680px]">
                  <thead className="bg-slate-100 text-slate-600">
                    <tr>
                      <th className="p-2 text-left">Station</th>
                      <th className="p-2 text-left">Samples</th>
                      <th className="p-2 text-left">Audio bytes</th>
                      <th className="p-2 text-left">Eligible</th>
                      <th className="p-2 text-left">Reclaimable</th>
                    </tr>
                  </thead>
                  <tbody>
                    {unknownStorage.byStation.map((row) => (
                      <tr key={row.stationId} className="border-t border-slate-200">
                        <td className="p-2">{row.stationName}</td>
                        <td className="p-2">{row.sampleCount}</td>
                        <td className="p-2">{row.audioBytes.toLocaleString()}</td>
                        <td className="p-2">{row.purgeEligibleCount}</td>
                        <td className="p-2">{row.reclaimableBytes.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="flex gap-2 flex-wrap">
                <button
                  type="button"
                  onClick={async () => {
                    const res = await fetch('/api/admin/storage/unknown-samples/hash-backfill-preview', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ limit: 100, dryRun: true }),
                    });
                    const body = await res.json();
                    if (!res.ok) {
                      onStorageError(body?.error || `Hash preview failed (${res.status})`);
                      return;
                    }
                    onHashBackfillResult(body);
                  }}
                  className="px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-xs hover:bg-slate-200"
                >
                  Preview Hash Backfill
                </button>
                <button
                  type="button"
                  onClick={async () => {
                    const res = await fetch('/api/admin/storage/unknown-samples/hash-backfill-run', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({ limit: 100, dryRun: false }),
                    });
                    const body = await res.json();
                    if (!res.ok) {
                      onStorageError(body?.error || `Hash backfill run failed (${res.status})`);
                      return;
                    }
                    onHashBackfillResult(body);
                    await onRefetchUnknownStorage();
                  }}
                  className="px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-xs hover:bg-slate-200"
                >
                  Run Hash Backfill Batch
                </button>
                <button
                  type="button"
                  onClick={async () => {
                    const res = await fetch('/api/admin/storage/unknown-samples/purge-dry-run', { method: 'POST' });
                    const body = await res.json();
                    if (!res.ok) {
                      onStorageError(body?.error || `Dry run failed (${res.status})`);
                      return;
                    }
                    onStorageDryRun(body);
                  }}
                  className="px-3 py-2 rounded-lg border border-slate-200 bg-slate-100 text-xs hover:bg-slate-200"
                >
                  Run Purge Dry-Run
                </button>
                <button type="button" disabled className="px-3 py-2 rounded-lg border border-slate-200 text-xs opacity-50">
                  Purge (Disabled until Phase 4/5)
                </button>
              </div>
              {storageDryRun && (
                <div className="text-xs text-slate-700 border border-slate-200 rounded-xl p-3">
                  Eligible: {storageDryRun.eligibleCount} · Blocked: {storageDryRun.blockedCount} · Reclaimable:{' '}
                  {Number(storageDryRun.reclaimableBytes || 0).toLocaleString()} bytes
                  <div className="text-slate-500 mt-1">
                    Blocked reasons: {Object.entries(storageDryRun.blockedReasonsSummary || {}).map(([k, v]) => `${k}:${v}`).join(', ') || 'none'}
                  </div>
                </div>
              )}
              {hashBackfillResult && (
                <div className="text-xs text-slate-700 border border-slate-200 rounded-xl p-3">
                  Hash backfill ({hashBackfillResult.dryRunOnly ? 'preview' : 'run'}): scanned {hashBackfillResult.totalScanned}, wouldUpdate {hashBackfillResult.wouldUpdateCount}, updated {hashBackfillResult.updatedCount || 0}
                  <div className="text-slate-500 mt-1">
                    Missing file: {hashBackfillResult.missingFile} · No file path: {hashBackfillResult.noFilePath} · Missing hash: {hashBackfillResult.missingHash}
                  </div>
                </div>
              )}
            </>
          )}
        </div>

        <label className="flex items-start gap-3 cursor-pointer select-none">
          <input
            type="checkbox"
            className="mt-1 rounded border-slate-300 bg-slate-100"
            checked={includeHiddenStations}
            onChange={(e) => onIncludeHiddenChange(e.target.checked)}
          />
          <span className="text-sm text-slate-700">
            <span className="font-semibold text-slate-900">Show all stations in the database</span>
            <span className="block text-xs text-slate-500 mt-1">
              When enabled, the dashboard loads every station row (including those with visibility turned off). Use this to
              audit the full catalog on production, then re-enable visibility per station if needed.
            </span>
          </span>
        </label>

        <div className="pt-4 border-t border-slate-200 flex gap-3">
          <button
            type="button"
            onClick={onRecheckDependencies}
            className="flex-1 bg-violet-600 hover:bg-violet-500 text-white font-semibold py-2.5 rounded-xl transition-all text-sm"
          >
            Re-check Dependencies
          </button>
          <button
            type="button"
            onClick={onRefreshStations}
            className="flex-1 btn-ghost text-slate-800 font-semibold py-2.5 text-sm"
          >
            Refresh Station Status
          </button>
        </div>
      </div>
    </div>
  );
}

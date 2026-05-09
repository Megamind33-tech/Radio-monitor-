# Unknown Review + Workers Audit (Phase 0)

Date: 2026-05-09
Branch target: `feature/unknown-review-workers`

## 1) Current branch and git state
- Current branch after checkout: `feature/unknown-review-workers`.
- Working tree at audit time: clean (no modified tracked files before this report).
- Existing local branch observed before switch: `work`.

## 2) Current app stack
- Frontend: React 19 + Vite + TailwindCSS 4 (`src/`, `vite.config.ts`, `src/index.css`).
- Backend: Express + TypeScript served via `tsx server/main.ts` in dev.
- Database: SQLite by default.
- ORM: Prisma (`prisma/schema.prisma`).
- Build/lint/test scripts:
  - `npm run build` => Vite build + server TS compile.
  - `npm run lint` => `tsc --noEmit`.
  - targeted tests for station-health, stream-url-guard, local fingerprint, merge pipeline.
- Deployment assumptions:
  - systemd units in `deploy/systemd/`
  - production start: `NODE_ENV=production node dist/server/main.js`
  - DigitalOcean helper scripts under `deploy/`.

## 3) Current station dashboard routes/components
- SPA nav tabs currently include Monitor, History, Song spins, Learning library, Audio Editor, Settings.
- Station detail view exists via hash route style: `#/stations/:id`.
- Existing “Audio Editor” section already lists unresolved/recovered samples and supports editing/identify flows.

## 4) Current monitoring services
- `MonitorService`: core polling + metadata + sampling + fingerprint + external resolver merge.
- `SchedulerService`: station poll scheduling + recovery/repair cron tasks.
- `StreamHealthService`: stream health probing/deep checks.
- `StreamRefreshService` + `StreamDiscoveryService`: stream URL refresh/discovery.

## 5) Current fingerprinting/local matching services
- `FingerprintService`: fpcalc generation from audio files.
- `LocalFingerprintService`: local cache lookup/learn with sha/prefix + approximate comparison.
- `AcoustidService`, `MusicbrainzService`, `CatalogLookupService`, optional `AuddService`/`AcrcloudService` fallback path.
- Pipeline gate exists for concurrency/rate limiting (`server/lib/fingerprint-pipeline-gate.ts`).

## 6) Current unresolved/unknown sample storage
- `UnresolvedSample` model stores `stationId`, optional `detectionLogId`, `filePath`, recovery state, attempts, timestamps.
- `SongSampleArchive` stores per-detection archived sample files + optional chromaprint.
- No dedicated review-status lifecycle enum for unknown moderation/purge workflow yet.

## 7) Current database schema coverage vs requested scope
### Already exists
- Stations: rich station health/metadata tuning fields.
- Logs: `DetectionLog` has parsed/final metadata, diagnostics, manual tagging marker.
- Fingerprints: `LocalFingerprint` has confidence/source/isrc json/artist-title metadata.
- Unknown/unresolved: `UnresolvedSample` table exists.
- “Now playing” + spin aggregation tables exist.

### Missing or partial for requested roadmap
- No normalized `Track/Song` master entity with full royalty metadata (ISWC, publisher, writers, territory, external IDs, verification source).
- No worker table / API key hash / worker heartbeat.
- No recognition job queue table.
- No unknown review grouping table/keys.
- No purge audit fields/status on unresolved rows (sha256, purge reason, purge failed status, purgedAt, fingerprintId linkage).
- No explicit metadata-source normalization table for provenance confidence by field.

## 8) Current scripts audit (relevant)
- Local fingerprint / matching: `scripts/safe_fp_exact_probe.py`, `scripts/diagnose_match_rate.mjs`, `scripts/drain_unresolved_aggressive.mjs`.
- Stream/source discovery: `scripts/strict_stream_validator.mjs`, `scripts/optimize_stations.mjs`, `scripts/mytuner_refresh_stream.py`, harvest scripts.
- Metadata repair/catalog: `scripts/sync_zambia_catalog.mjs`, `scripts/song_spin_upsert.mjs`, `scripts/backfill_station_song_spins.mjs`.
- Radio directory recovery/crawling related: `mytuner_*`, `radiotime_*`, `streema_*`, `onlineradio_*`, `zambia_radio_browser_harvest.mjs`.

## 9) Current UI structure and styling
- Single-page React component architecture centered in `src/App.tsx`.
- Styling via Tailwind utility classes in JSX.
- Existing reusable module: `src/LearningLibraryTab.tsx`.
- Opportunity: split large `App.tsx` into dashboard feature modules before adding large Unknown Review UI.

## 10) Existing APIs reusable for Phase 1+
- `/api/audio-editor/samples`
- `/api/audio-editor/samples/:id/audio` (already prevents raw file path exposure and supports ranged streaming)
- `/api/audio-editor/samples/:id` (patch metadata)
- `/api/audio-editor/samples/:id/identify`
- `/api/recovery/unresolved/*` endpoints
- `/api/fingerprints/local*` endpoints
- `/api/stations/:id/logs` and analytics endpoints

---

## What exists
- Strong base for station monitoring, fingerprint generation, fallback recognizers, unresolved sample capture, and manual correction primitives.
- Existing audio editor APIs cover much of secure playback/edit/identify mechanics needed for Unknown Review.

## What is missing
- End-to-end unknown lifecycle state machine (review → verified → fingerprinted → purge-ready → purged/fail).
- First-class track catalog model for royalty-grade metadata.
- Worker queue and job orchestration schema/API.
- Similarity grouping + bulk action audit trail.
- Safer purge policy with dry-run and immutable purge audit fields.

## Risky areas to touch
- `server/services/monitor.service.ts` and `scheduler.service.ts` (core production polling path; regressions can disrupt all stations).
- `server/main.ts` (currently monolithic API file; route edits need careful isolation/tests).
- Prisma migrations on existing SQLite data (must be additive, backfill-safe).
- Existing unresolved cleanup scripts (avoid deleting audio prematurely).

## Recommended implementation order (mapped to requested phases)
1. Add audit note + implementation plan (this file).
2. Add additive Prisma migration: unknown statuses, worker/job tables, track metadata, purge audit fields.
3. Add backend domain services for unknown review state transitions + queue operations.
4. Add/extend secure audio streaming endpoint tests (range behavior).
5. Build Unknown Review UI module using existing Audio Editor flows.
6. Implement save+fingerprint workflow against new track model and local fingerprint table.
7. Add grouping logic and bulk-apply with confirmation/audit.
8. Add worker queue endpoints and polling/lock semantics.
9. Add purge dry-run endpoint + actual purge endpoint with fail-safe statusing.
10. Extend discovery/crawler classification persistence and backoff.
11. Add dashboard observability cards.
12. Add tests + lint/build cleanup.

## Exact files planned for modification (next phases)
- Database/ORM:
  - `prisma/schema.prisma`
  - `prisma/migrations/<new_timestamp>_unknown_review_workers/*`
- Backend API + services:
  - `server/main.ts`
  - `server/services/unresolved-recovery.service.ts`
  - `server/services/local-fingerprint.service.ts`
  - `server/services/monitor.service.ts` (minimal integration points only)
  - New files likely under `server/services/`:
    - `unknown-review.service.ts`
    - `recognition-queue.service.ts`
    - `purge-policy.service.ts`
- Backend shared types/libs:
  - `server/types.ts`
  - `server/lib/*` (new small helpers for grouping/hash/audit)
- Frontend:
  - `src/App.tsx` (integration)
  - New components under `src/components/unknown-review/*`
  - Possible extraction helpers under `src/lib/*`
- Tests:
  - `server/services/*.spec.ts` additions
  - targeted API behavior checks (if test harness exists)

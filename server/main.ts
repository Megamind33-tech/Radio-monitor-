import "dotenv/config";
import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import fs from "fs";
import os from "os";
import crypto from "crypto";
import cors from "cors";
import helmet from "helmet";
import { spawnSync, spawn } from "child_process";
import { logger } from "./lib/logger.js";
import { prisma } from "./lib/prisma.js";
import { z } from "zod";
import { SchedulerService } from "./services/scheduler.service.js";
import { MonitorService } from "./services/monitor.service.js";
import { StreamRefreshService } from "./services/stream-refresh.service.js";
import { validateCandidateStreamUrl } from "./lib/stream-url-guard.js";
import { StreamHealthService } from "./services/stream-health.service.js";
import { effectiveMountUrl } from "./lib/stream-source.js";
import { StreamDiscoveryService } from "./services/stream-discovery.service.js";
import { monitorEvents } from "./lib/monitor-events.js";
import { UnresolvedRecoveryService } from "./services/unresolved-recovery.service.js";
import { LocalFingerprintService } from "./services/local-fingerprint.service.js";
import {
  exportRowsToCsv,
  isQualityLibraryRow,
  localFingerprintToExportRow,
} from "./lib/local-fingerprint-export.js";
import { parseFeaturedFromArtist, titleWithoutFeaturing } from "./lib/track-credits.js";
import { fingerprintPipelineGate } from "./lib/fingerprint-pipeline-gate.js";
import { AuddService } from "./services/audd.service.js";
import { AcrcloudService } from "./services/acrcloud.service.js";
import { SpinRefreshService } from "./services/spin-refresh.service.js";
import { FingerprintService } from "./services/fingerprint.service.js";
import { AcoustidService } from "./services/acoustid.service.js";
import { MusicbrainzService } from "./services/musicbrainz.service.js";
import { SampleStorageService } from "./services/sample-storage.service.js";
import { CatalogCrawlerService } from "./services/catalog-crawler.service.js";
import { RematchService } from "./services/rematch.service.js";
import { ExternalRecognitionService } from "./services/external-recognition.service.js";
import * as XLSX from "xlsx";

function envBoolTrue(key: string, defaultTrue = true): boolean {
  const v = process.env[key];
  if (v === undefined || v === null || String(v).trim() === "") return defaultTrue;
  const t = String(v).trim().toLowerCase();
  return !(t === "0" || t === "false" || t === "no" || t === "off");
}

function isCommandAvailable(command: string, args: string[] = ["-version"]) {
  try {
    const result = spawnSync(command, args, { stdio: "ignore" });
    return result.status === 0;
  } catch {
    return false;
  }
}

function normalizeStationName(value: string): string {
  return String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

type DedupableStation = {
  id: string;
  name: string;
  country: string;
  frequencyMhz?: string | null;
  isActive: boolean;
  visibilityEnabled?: boolean | null;
  monitorState?: string | null;
  lastGoodAudioAt?: Date | null;
  lastPollAt?: Date | null;
  lastPollStatus?: string | null;
};

function stationDedupKey(station: Pick<DedupableStation, "name" | "country">): string {
  const country = String(station.country || "").trim().toLowerCase();
  return `${country}|${normalizeStationName(station.name)}`;
}

function stationRank(station: DedupableStation): number {
  let score = 0;
  if (station.isActive) score += 100;
  if (station.visibilityEnabled !== false) score += 8;
  if (station.lastGoodAudioAt) score += 12;
  if (station.lastPollStatus === "ok") score += 8;
  if (station.frequencyMhz && String(station.frequencyMhz).trim()) score += 4;
  switch (station.monitorState) {
    case "ACTIVE_MUSIC":
      score += 30;
      break;
    case "ACTIVE_NO_MATCH":
      score += 24;
      break;
    case "ACTIVE_TALK":
      score += 18;
      break;
    case "DEGRADED":
      score += 8;
      break;
    case "INACTIVE":
      score += 2;
      break;
    default:
      break;
  }
  return score;
}

function dedupeStations<T extends DedupableStation>(stations: T[]): T[] {
  const winners = new Map<string, T>();
  for (const station of stations) {
    const key = stationDedupKey(station);
    const current = winners.get(key);
    if (!current) {
      winners.set(key, station);
      continue;
    }
    const currentRank = stationRank(current);
    const nextRank = stationRank(station);
    if (nextRank > currentRank) {
      winners.set(key, station);
      continue;
    }
    if (nextRank === currentRank) {
      const currentPoll = current.lastPollAt?.getTime?.() ?? 0;
      const nextPoll = station.lastPollAt?.getTime?.() ?? 0;
      if (nextPoll > currentPoll) {
        winners.set(key, station);
      }
    }
  }
  return Array.from(winners.values()).sort((a, b) => a.name.localeCompare(b.name));
}

async function startServer() {
  const app = express();
  const PORT = 3000;

  app.use(cors());
  app.use(helmet({
    contentSecurityPolicy: false,
  }));
  app.use(express.json());

  app.get("/api/events/monitoring", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders?.();

    const sendEvent = (event: string, payload: unknown) => {
      res.write(`event: ${event}\n`);
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    sendEvent("ready", { ok: true, ts: new Date().toISOString() });
    const heartbeat = setInterval(() => {
      sendEvent("heartbeat", { ts: new Date().toISOString() });
    }, 15_000);

    const onSongDetected = (payload: unknown) => {
      sendEvent("song_detected", payload);
    };
    const onStationPoll = (payload: unknown) => {
      sendEvent("station_poll", payload);
    };
    monitorEvents.on("song-detected", onSongDetected);
    monitorEvents.on("station-poll", onStationPoll);

    req.on("close", () => {
      clearInterval(heartbeat);
      monitorEvents.off("song-detected", onSongDetected);
      monitorEvents.off("station-poll", onStationPoll);
      res.end();
    });
  });

  // Health check
  app.get("/api/health", async (req, res) => {
    try {
      await prisma.$queryRaw`SELECT 1`;
      res.json({ 
        status: "ok", 
        database: "connected",
        env: process.env.APP_ENV || 'development'
      });
    } catch (error) {
      res.status(500).json({ status: "error", database: "disconnected" });
    }
  });

  app.get("/api/system/dependencies", (req, res) => {
    const ffmpeg = isCommandAvailable("ffmpeg");
    const ffprobe = isCommandAvailable("ffprobe");
    const fpcalc = isCommandAvailable("fpcalc");
    const acoustidApiKeyConfigured = !!process.env.ACOUSTID_API_KEY;
    const musicbrainzUserAgentConfigured = !!process.env.MUSICBRAINZ_USER_AGENT;
    const catalogLookupReady = musicbrainzUserAgentConfigured;
    const freeApisEnabled = {
      acoustid: acoustidApiKeyConfigured,
      musicbrainz: musicbrainzUserAgentConfigured,
      itunesSearch: true,
      deezerSearch: process.env.DEEZER_LOOKUP_ENABLED !== "false",
      theAudioDbSearch: process.env.THEAUDIODB_LOOKUP_ENABLED !== "false",
      acoustidOpenClient: !!process.env.ACOUSTID_OPEN_CLIENT,
    };

    const missing = [];
    if (!ffmpeg) missing.push("ffmpeg");
    if (!ffprobe) missing.push("ffprobe");
    if (!fpcalc) missing.push("fpcalc");
    if (!acoustidApiKeyConfigured) missing.push("ACOUSTID_API_KEY");
    if (!musicbrainzUserAgentConfigured) missing.push("MUSICBRAINZ_USER_AGENT");

    const auddApiConfigured = AuddService.isEnabled();
    const acrcloudApiConfigured = AcrcloudService.isEnabled();
    const paidFallbacksEnabled = envBoolTrue("PAID_AUDIO_FALLBACKS_ENABLED", true);
    const paidLaneReady = !paidFallbacksEnabled || auddApiConfigured || acrcloudApiConfigured;
    const integrationNotes: string[] = [];
    if (paidFallbacksEnabled && !auddApiConfigured && !acrcloudApiConfigured) {
      integrationNotes.push(
        "Paid audio lane is on but neither AUDD_API_TOKEN nor ACRCLOUD_* is set — suspicious ICY will not reach AudD/ACRCloud."
      );
    }
    if (paidFallbacksEnabled && auddApiConfigured) {
      integrationNotes.push("AudD: token present — used after AcoustID miss when ICY is flagged non-song / untrusted.");
    }
    if (paidFallbacksEnabled && acrcloudApiConfigured) {
      integrationNotes.push("ACRCloud: host + keys present — optional fallback after AudD on the same capture.");
    }
    if (!paidFallbacksEnabled) {
      integrationNotes.push("PAID_AUDIO_FALLBACKS_ENABLED is off — AudD and ACRCloud are never called.");
    }

    res.json({
      ffmpeg,
      ffprobe,
      fpcalc,
      acoustidApiKeyConfigured,
      musicbrainzUserAgentConfigured,
      catalogLookupReady,
      freeApisEnabled,
      fingerprintReady: ffmpeg && ffprobe && fpcalc && acoustidApiKeyConfigured,
      missing,
      paidApis: {
        auddConfigured: auddApiConfigured,
        acrcloudConfigured: acrcloudApiConfigured,
        paidFallbacksEnabled,
        paidLaneReady,
      },
      integrationNotes,
    });
  });

  // Stations API — `visibility=all` returns every row in Station (admin / full catalog); default hides visibilityEnabled=false.
  app.get("/api/stations", async (req, res) => {
    const dedupe = String(req.query?.dedupe ?? "1") !== "0";
    const visibility = String(req.query?.visibility ?? "visible").toLowerCase();
    const stations = await prisma.station.findMany({
      where: visibility === "all" ? undefined : { visibilityEnabled: true },
      include: { currentNowPlaying: true },
      orderBy: [{ name: "asc" }],
    });
    const out = dedupe ? dedupeStations(stations as unknown as DedupableStation[]) : stations;
    res.json(out);
  });

  app.post("/api/stations", async (req, res) => {
    const station = await prisma.station.create({
      data: {
        ...req.body,
        monitorState: req.body?.monitorState || "UNKNOWN",
        contentClassification: req.body?.contentClassification || "unknown",
        visibilityEnabled: req.body?.visibilityEnabled ?? true,
      },
    });
    res.json(station);
  });

  const stationPatchSchema = z
    .object({
      name: z.string().min(1).max(500).optional(),
      country: z.string().min(1).max(120).optional(),
      district: z.string().max(200).optional(),
      province: z.string().max(200).optional(),
      frequencyMhz: z.string().max(32).nullable().optional(),
      streamUrl: z.string().url().max(4000).optional(),
      preferredStreamUrl: z.string().url().max(4000).nullable().optional(),
      streamFormatHint: z.string().max(64).nullable().optional(),
      sourceIdsJson: z.string().max(8000).nullable().optional(),
      icyQualification: z.string().max(32).nullable().optional(),
      icySampleTitle: z.string().max(2000).nullable().optional(),
      isActive: z.boolean().optional(),
      metadataPriorityEnabled: z.boolean().optional(),
      fingerprintFallbackEnabled: z.boolean().optional(),
      metadataStaleSeconds: z.number().int().min(30).max(86400).optional(),
      sampleSeconds: z.number().int().min(5).max(120).optional(),
      pollIntervalSeconds: z.number().int().min(5).max(3600).optional(),
      audioFingerprintIntervalSeconds: z.number().int().min(30).max(86400).optional(),
      icyVerificationIntervalSeconds: z.number().int().min(60).max(86400).optional(),
      metadataTrustTightness: z.number().int().min(0).max(2).optional(),
      fingerprintRetries: z.number().int().min(1).max(4).optional(),
      fingerprintRetryDelayMs: z.number().int().min(0).max(60000).optional(),
      catalogConfidenceFloor: z.number().min(0.35).max(1).nullable().optional(),
      archiveSongSamples: z.boolean().optional(),
      visibilityEnabled: z.boolean().optional(),
      monitorState: z
        .enum(["ACTIVE_MUSIC", "ACTIVE_NO_MATCH", "ACTIVE_TALK", "DEGRADED", "INACTIVE", "UNKNOWN"])
        .optional(),
    })
    .strict();

  app.patch("/api/stations/:id", async (req, res) => {
    const parsed = stationPatchSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid body", details: parsed.error.flatten() });
      return;
    }
    if (Object.keys(parsed.data).length === 0) {
      res.status(400).json({ error: "No valid fields to update" });
      return;
    }
    if (parsed.data.streamUrl) {
      const check = validateCandidateStreamUrl(parsed.data.streamUrl);
      if (!check.accepted) {
        res.status(400).json({ error: `Rejected stream URL: ${check.reason}` });
        return;
      }
      parsed.data.streamUrl = check.canonicalUrl;
    }
    if (parsed.data.preferredStreamUrl) {
      const check = validateCandidateStreamUrl(parsed.data.preferredStreamUrl);
      if (!check.accepted) {
        res.status(400).json({ error: `Rejected preferred stream URL: ${check.reason}` });
        return;
      }
      parsed.data.preferredStreamUrl = check.canonicalUrl;
    }
    const station = await prisma.station.update({
      where: { id: req.params.id },
      data: parsed.data,
    });
    res.json(station);
  });

  /** Re-fetch stream URL from harvest hints (MyTuner page / ORB / Streema) when the CDN URL rotated. */
  app.post("/api/stations/:id/refresh-stream", async (req, res) => {
    const st = await prisma.station.findUnique({ where: { id: req.params.id } });
    if (!st) {
      res.status(404).json({ error: "Station not found" });
      return;
    }
    const next = await StreamRefreshService.refreshFromSourceHints(st.sourceIdsJson, st.streamUrl);
    if (!next) {
      res.json({ updated: false, streamUrl: st.streamUrl, message: "No alternate URL from hints" });
      return;
    }
    const updated = await prisma.station.update({
      where: { id: req.params.id },
      data: { streamUrl: next, streamRefreshedAt: new Date() },
    });
    res.json({ updated: true, streamUrl: updated.streamUrl });
  });

  app.post("/api/stations/:id/validate-stream", async (req, res) => {
    const st = await prisma.station.findUnique({ where: { id: req.params.id } });
    if (!st) {
      res.status(404).json({ error: "Station not found" });
      return;
    }
    const mount = effectiveMountUrl(st.streamUrl, st.preferredStreamUrl);
    const health = await StreamHealthService.validateStream(mount);
    const now = new Date();
    await prisma.station.update({
      where: { id: st.id },
      data: {
        lastValidationAt: now,
        lastValidationReason: health.reason,
        lastResolvedStreamUrl: health.resolvedUrl || st.streamUrl,
        lastStreamContentType: health.contentTypeHeader || null,
        lastStreamCodec: health.codec || null,
        lastStreamBitrate: health.bitrate ?? null,
        lastHealthyAt: health.reachable && health.audioFlowing ? now : st.lastHealthyAt,
        lastGoodAudioAt: health.reachable && health.audioFlowing ? now : st.lastGoodAudioAt,
      },
    });
    res.json({ stationId: st.id, health, validatedUrl: mount });
  });

  /** Alternate / historical mount URLs with per-source quality and hit counters. */
  app.get("/api/stations/:id/stream-endpoints", async (req, res) => {
    const rows = await prisma.stationStreamEndpoint.findMany({
      where: { stationId: req.params.id },
      orderBy: [{ qualityScore: "desc" }, { isCurrent: "desc" }],
    });
    res.json(rows);
  });

  /**
   * Multi-server stream discovery (no station website required): Radio-Browser mirrors,
   * TuneIn OPML search + resolve, harvest hints (MyTuner / ORB / Streema). Read-only.
   */
  app.get("/api/stations/:id/discover-streams", async (req, res) => {
    const st = await prisma.station.findUnique({ where: { id: req.params.id } });
    if (!st) {
      res.status(404).json({ error: "Station not found" });
      return;
    }
    try {
      const result = await StreamDiscoveryService.discoverForStation({
        id: st.id,
        name: st.name,
        country: st.country,
        streamUrl: st.streamUrl,
        sourceIdsJson: st.sourceIdsJson,
      });
      res.json(result);
    } catch (error) {
      logger.error({ error, stationId: req.params.id }, "discover-streams failed");
      res.status(500).json({ error: "discover_streams_failed", detail: String(error) });
    }
  });

  const discoverApplySchema = z
    .object({
      streamUrl: z.string().url().max(4000).optional(),
      preferredOnly: z.boolean().optional(),
    })
    .strict();

  /**
   * Run discovery and optionally set `preferredStreamUrl` (default) or replace `streamUrl`.
   * Validates URL before write.
   */
  app.post("/api/stations/:id/discover-streams/apply", async (req, res) => {
    const parsed = discoverApplySchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid body", details: parsed.error.flatten() });
      return;
    }
    const st = await prisma.station.findUnique({ where: { id: req.params.id } });
    if (!st) {
      res.status(404).json({ error: "Station not found" });
      return;
    }
    try {
      const discovery = await StreamDiscoveryService.discoverForStation({
        id: st.id,
        name: st.name,
        country: st.country,
        streamUrl: st.streamUrl,
        sourceIdsJson: st.sourceIdsJson,
      });
      let chosen = discovery.candidates[0]?.streamUrl ?? null;
      if (parsed.data.streamUrl) {
        const v = validateCandidateStreamUrl(parsed.data.streamUrl);
        if (!v.accepted) {
          res.status(400).json({ error: "invalid_stream_url", reason: v.reason });
          return;
        }
        chosen = v.canonicalUrl;
      }
      if (!chosen) {
        res.json({ updated: false, discovery, message: "No candidate stream URL found" });
        return;
      }
      const check = validateCandidateStreamUrl(chosen);
      if (!check.accepted) {
        res.status(400).json({ error: "Rejected stream URL", reason: check.reason });
        return;
      }
      const preferredOnly = parsed.data.preferredOnly !== false;
      const updated = await prisma.station.update({
        where: { id: st.id },
        data: preferredOnly
          ? { preferredStreamUrl: check.canonicalUrl, streamRefreshedAt: new Date() }
          : { streamUrl: check.canonicalUrl, preferredStreamUrl: null, streamRefreshedAt: new Date() },
      });
      res.json({
        updated: true,
        preferredOnly,
        streamUrl: updated.streamUrl,
        preferredStreamUrl: updated.preferredStreamUrl,
        chosen: check.canonicalUrl,
        discovery,
      });
    } catch (error) {
      logger.error({ error, stationId: req.params.id }, "discover-streams apply failed");
      res.status(500).json({ error: "discover_streams_apply_failed", detail: String(error) });
    }
  });

  app.get("/api/stations/:id/health-events", async (req, res) => {
    const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 200;
    const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 2000) : 200;
    const events = await prisma.stationHealthEvent.findMany({
      where: { stationId: req.params.id },
      orderBy: { observedAt: "desc" },
      take,
    });
    res.json(events);
  });

  app.get("/api/stations/:id/stream-endpoints", async (req, res) => {
    const rows = await prisma.stationStreamEndpoint.findMany({
      where: { stationId: req.params.id },
      orderBy: [{ isCurrent: "desc" }, { updatedAt: "desc" }],
    });
    res.json(rows);
  });

  app.get("/api/monitoring/status-overview", async (_req, res) => {
    const states = await prisma.station.groupBy({
      by: ["monitorState"],
      where: { visibilityEnabled: true },
      _count: { _all: true },
    });
    const out: Record<string, number> = {
      ACTIVE_MUSIC: 0,
      ACTIVE_NO_MATCH: 0,
      ACTIVE_TALK: 0,
      DEGRADED: 0,
      INACTIVE: 0,
      UNKNOWN: 0,
    };
    for (const row of states) {
      const k = row.monitorState || "UNKNOWN";
      out[k] = Number(row._count._all || 0);
    }
    res.json(out);
  });

  app.get("/api/stations/:id/logs", async (req, res) => {
    const takeQuery = typeof req.query.take === "string" ? Number(req.query.take) : 300;
    const take = Number.isFinite(takeQuery) ? Math.min(Math.max(Math.trunc(takeQuery), 1), 2000) : 300;
    const logs = await prisma.detectionLog.findMany({
      where: { stationId: req.params.id },
      orderBy: { observedAt: 'desc' },
      take
    });
    res.json(logs);
  });

  app.get("/api/fingerprints/local/stats", async (_req, res) => {
    try {
      const stats = await LocalFingerprintService.stats();
      res.json(stats);
    } catch (error) {
      logger.error({ error }, "Failed to read local fingerprint stats");
      res.status(500).json({ error: "failed_to_read_local_fingerprint_stats" });
    }
  });

  app.get("/api/fingerprints/local", async (req, res) => {
    try {
      const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 100;
      const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 2000) : 100;
      const rows = await prisma.localFingerprint.findMany({
        orderBy: { lastMatchedAt: "desc" },
        take,
        select: {
          id: true,
          title: true,
          artist: true,
          displayArtist: true,
          titleWithoutFeat: true,
          featuredArtistsJson: true,
          releaseTitle: true,
          releaseDate: true,
          genre: true,
          labelName: true,
          countryCode: true,
          durationSec: true,
          durationMs: true,
          playCountTotal: true,
          acoustidTrackId: true,
          recordingMbid: true,
          isrcsJson: true,
          source: true,
          confidence: true,
          timesMatched: true,
          firstLearnedAt: true,
          lastMatchedAt: true,
        },
      });
      res.json(rows);
    } catch (error) {
      logger.error({ error }, "Failed to list local fingerprints");
      res.status(500).json({ error: "failed_to_list_local_fingerprints" });
    }
  });

  /** Quality-filtered catalog export (JSON or CSV) for identified songs only. */
  app.get("/api/fingerprints/local/export", async (req, res) => {
    try {
      const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 5000;
      const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 50_000) : 5000;
      const format = String(req.query.format || "json").toLowerCase();
      const rows = await prisma.localFingerprint.findMany({
        orderBy: [{ playCountTotal: "desc" }, { lastMatchedAt: "desc" }],
        take,
      });
      const quality = rows.filter(isQualityLibraryRow).map(localFingerprintToExportRow);
      if (format === "csv") {
        res.setHeader("Content-Type", "text/csv; charset=utf-8");
        res.setHeader("Content-Disposition", 'attachment; filename="identified_songs_library.csv"');
        res.send(exportRowsToCsv(quality));
        return;
      }
      res.json({ count: quality.length, rows: quality });
    } catch (error) {
      logger.error({ error }, "Failed to export local fingerprint library");
      res.status(500).json({ error: "failed_to_export_local_fingerprints" });
    }
  });

  app.get("/api/recovery/unresolved/status", async (_req, res) => {
    try {
      const status = UnresolvedRecoveryService.status();
      const totals = await prisma.unresolvedSample.groupBy({
        by: ["recoveryStatus"],
        _count: { _all: true },
      });
      const byStatus: Record<string, number> = {};
      for (const row of totals) {
        byStatus[row.recoveryStatus || "pending"] = Number(row._count._all || 0);
      }
      const byReasonRows = await prisma.unresolvedSample.groupBy({
        by: ["recoveryReason"],
        _count: { _all: true },
      });
      const byReason: Record<string, number> = {};
      for (const row of byReasonRows) {
        const k = row.recoveryReason || "(null)";
        byReason[k] = Number(row._count._all || 0);
      }
      const dayAgo = new Date(Date.now() - 86400000);
      const [created24h, recovered24h] = await Promise.all([
        prisma.unresolvedSample.count({ where: { createdAt: { gte: dayAgo } } }),
        prisma.unresolvedSample.count({
          where: { recoveredAt: { gte: dayAgo }, recoveryStatus: "recovered" },
        }),
      ]);
      res.json({
        ...status,
        totals: byStatus,
        byReason,
        flow24h: { created: created24h, recovered: recovered24h },
      });
    } catch (error) {
      logger.error({ error }, "Failed unresolved recovery status request");
      res.status(500).json({ error: "failed_to_read_recovery_status" });
    }
  });

  app.post("/api/recovery/unresolved/classify", async (req, res) => {
    try {
      const body = req.body && typeof req.body === "object" ? (req.body as Record<string, unknown>) : {};
      const stationId = typeof body.stationId === "string" && body.stationId.trim() ? body.stationId.trim() : undefined;
      const limitRaw = typeof body.limit === "number" ? body.limit : Number(body.limit);
      const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 500) : undefined;
      const out = await UnresolvedRecoveryService.classifyPendingBatch({ stationId, limit });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "Failed unresolved classify batch");
      res.status(500).json({ error: "failed_to_classify_unresolved" });
    }
  });

  app.post("/api/recovery/unresolved/safe-title-recover", async (req, res) => {
    try {
      const body = req.body && typeof req.body === "object" ? (req.body as Record<string, unknown>) : {};
      const stationId = typeof body.stationId === "string" && body.stationId.trim() ? body.stationId.trim() : undefined;
      const limitRaw = typeof body.limit === "number" ? body.limit : Number(body.limit);
      const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 500) : undefined;
      const dryRun =
        typeof body.dryRun === "boolean"
          ? body.dryRun
          : String(body.dryRun || "true").toLowerCase() !== "false";
      const out = await UnresolvedRecoveryService.runSafeTitleAutoRecoveryBatch({ stationId, limit, dryRun });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "Failed safe title recovery batch");
      res.status(500).json({ error: "failed_safe_title_recovery" });
    }
  });

  app.post("/api/recovery/unresolved/run", async (req, res) => {
    try {
      const body = req.body && typeof req.body === "object" ? req.body as Record<string, unknown> : {};
      const stationId = typeof body.stationId === "string" && body.stationId.trim() ? body.stationId.trim() : undefined;
      const limitRaw = typeof body.limit === "number" ? body.limit : Number(body.limit);
      const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 200) : undefined;
      const force = typeof body.force === "boolean"
        ? body.force
        : String(body.force || "").toLowerCase() === "true";
      const maxPassesRaw =
        typeof body.maxPasses === "number" ? body.maxPasses : Number(body.maxPasses);
      const maxPasses = Number.isFinite(maxPassesRaw)
        ? Math.min(Math.max(Math.trunc(maxPassesRaw), 1), 200)
        : undefined;
      const out = force
        ? await UnresolvedRecoveryService.runUntilDrained({
            stationId,
            limit,
            maxPasses,
            continueWithoutAcoustid: true,
          })
        : await UnresolvedRecoveryService.runBatch({ stationId, limit });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "Failed unresolved recovery run request");
      res.status(500).json({ error: "failed_to_run_recovery_batch" });
    }
  });

  app.post("/api/recovery/unresolved/cleanup", async (req, res) => {
    try {
      const body = req.body && typeof req.body === "object" ? req.body as Record<string, unknown> : {};
      const keepRaw = typeof body.keepRecovered === "number" ? body.keepRecovered : Number(body.keepRecovered);
      const keepRecovered = Number.isFinite(keepRaw) ? Math.max(0, Math.trunc(keepRaw)) : 0;
      const out = await UnresolvedRecoveryService.cleanupRecoveredFiles(keepRecovered);
      res.json(out);
    } catch (error) {
      logger.error({ error }, "Failed unresolved recovery cleanup request");
      res.status(500).json({ error: "failed_to_cleanup_recovered_samples" });
    }
  });

  /**
   * LIST songs with no usable metadata — the queue of detections that AcoustID (or any
   * alternative identification service) still needs to resolve.
   *
   * Returns UnresolvedSample rows with station name, linked DetectionLog ICY text,
   * recovery status, and whether the saved audio file is still on disk.
   * Use ?status=no_match to see only songs that came back empty from AcoustID.
   * Use ?status=pending  to see songs waiting for their first recovery attempt.
   * Omit ?status to get all non-recovered entries (pending + no_match + error).
   *
   * Pipeline rule: at most 2 recovery operations run concurrently (fingerprintPipelineGate).
   * Callers sending these clips to alternative services should also respect that limit.
   */
  app.get("/api/recovery/unresolved/list", async (req, res) => {
    try {
      const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 100;
      const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 500) : 100;
      const stationId = typeof req.query.stationId === "string" && req.query.stationId.trim()
        ? req.query.stationId.trim()
        : undefined;
      const statusFilter = typeof req.query.status === "string" && req.query.status.trim()
        ? req.query.status.trim()
        : undefined;

      const allowedStatuses = ["pending", "no_match", "error", "skipped", "recovered"];
      const statusWhere = statusFilter && allowedStatuses.includes(statusFilter)
        ? [statusFilter]
        : ["pending", "no_match", "error"];

      const rows = await prisma.unresolvedSample.findMany({
        where: {
          ...(stationId ? { stationId } : {}),
          recoveryStatus: { in: statusWhere },
        },
        orderBy: { createdAt: "desc" },
        take,
        select: {
          id: true,
          stationId: true,
          detectionLogId: true,
          filePath: true,
          createdAt: true,
          recoveryStatus: true,
          recoveryAttempts: true,
          lastRecoveryAt: true,
          lastRecoveryError: true,
          recoveredAt: true,
          recoveryReason: true,
          titleNormKey: true,
          recoveryPriority: true,
        },
      });

      const stationIds = [...new Set(rows.map((r) => r.stationId))];
      const stations = stationIds.length
        ? await prisma.station.findMany({
            where: { id: { in: stationIds } },
            select: { id: true, name: true, country: true, province: true },
          })
        : [];
      const stationById = new Map(stations.map((s) => [s.id, s]));

      const detectionLogIds = rows.map((r) => r.detectionLogId).filter((id): id is string => !!id);
      const detectionLogs = detectionLogIds.length
        ? await prisma.detectionLog.findMany({
            where: { id: { in: detectionLogIds } },
            select: {
              id: true,
              observedAt: true,
              rawStreamText: true,
              parsedArtist: true,
              parsedTitle: true,
              detectionMethod: true,
              reasonCode: true,
            },
          })
        : [];
      const logById = new Map(detectionLogs.map((l) => [l.id, l]));

      const result = rows.map((row) => {
        const station = stationById.get(row.stationId);
        const log = row.detectionLogId ? logById.get(row.detectionLogId) : undefined;
        const hasAudioFile = !!row.filePath && fs.existsSync(row.filePath);
        return {
          id: row.id,
          stationId: row.stationId,
          stationName: station?.name ?? null,
          stationCountry: station?.country ?? null,
          stationProvince: station?.province ?? null,
          detectionLogId: row.detectionLogId ?? null,
          createdAt: row.createdAt,
          recoveryStatus: row.recoveryStatus,
          recoveryAttempts: row.recoveryAttempts,
          lastRecoveryAt: row.lastRecoveryAt ?? null,
          lastRecoveryError: row.lastRecoveryError ?? null,
          recoveredAt: row.recoveredAt ?? null,
          recoveryReason: row.recoveryReason ?? null,
          titleNormKey: row.titleNormKey ?? null,
          recoveryPriority: row.recoveryPriority ?? 0,
          hasAudioFile,
          detectedAt: log?.observedAt ?? null,
          rawStreamText: log?.rawStreamText ?? null,
          parsedArtist: log?.parsedArtist ?? null,
          parsedTitle: log?.parsedTitle ?? null,
          detectionMethod: log?.detectionMethod ?? null,
          reasonCode: log?.reasonCode ?? null,
        };
      });

      res.json({
        total: result.length,
        statusFilter: statusWhere,
        pipelineGate: fingerprintPipelineGate.getStatus(),
        items: result,
      });
    } catch (error) {
      logger.error({ error }, "Failed unresolved list request");
      res.status(500).json({ error: "failed_to_list_unresolved_samples" });
    }
  });

  /**
   * Fingerprint pipeline gate status — shows how many capture operations are
   * currently in-flight and the global throughput limit (max 2/second).
   */
  app.get("/api/fingerprints/pipeline-gate", (_req, res) => {
    res.json(fingerprintPipelineGate.getStatus());
  });

  /** Self-learned library + pipeline snapshot for the Learning dashboard UI. */
  app.get("/api/learning/dashboard", async (_req, res) => {
    try {
      const [lib, gate, deps] = await Promise.all([
        LocalFingerprintService.dashboardStats(),
        Promise.resolve(fingerprintPipelineGate.getStatus()),
        Promise.resolve({
          acoustid: !!process.env.ACOUSTID_API_KEY,
          acoustidOpen: !!process.env.ACOUSTID_OPEN_CLIENT,
          musicbrainz: !!process.env.MUSICBRAINZ_USER_AGENT,
          audd: AuddService.isEnabled(),
          acrcloud: AcrcloudService.isEnabled(),
          paidFallbacksEnabled: envBoolTrue("PAID_AUDIO_FALLBACKS_ENABLED", true),
          localLearningEnabled: envBoolTrue("LOCAL_FP_LEARNING_ENABLED", true),
        }),
      ]);
      res.json({
        library: lib,
        pipelineGate: gate,
        services: deps,
        pipelineEnv: {
          minGapMs: Math.min(
            5000,
            Math.max(200, parseInt(process.env.FINGERPRINT_PIPELINE_MIN_GAP_MS || "750", 10) || 750)
          ),
          maxConcurrent: Math.min(
            8,
            Math.max(1, parseInt(process.env.FINGERPRINT_PIPELINE_MAX_CONCURRENT || "2", 10) || 2)
          ),
        },
      });
    } catch (error) {
      logger.error({ error }, "Failed learning dashboard snapshot");
      res.status(500).json({ error: "failed_learning_dashboard" });
    }
  });

  app.get("/api/stations/:id/airplays", async (req, res) => {
    const airplays = await prisma.detectionLog.findMany({
      where: {
        stationId: req.params.id,
        status: "matched",
        titleFinal: { not: null }
      },
      orderBy: { observedAt: "desc" },
      take: 200,
      select: {
        id: true,
        stationId: true,
        observedAt: true,
        detectionMethod: true,
        titleFinal: true,
        artistFinal: true,
        genreFinal: true,
        sourceProvider: true,
        status: true
      }
    });
    res.json(airplays);
  });

  /** Past detections (real rows in DetectionLog only — no synthetic backfill). */
  app.get("/api/detections/history", async (req, res) => {
    const stationId = typeof req.query.stationId === "string" ? req.query.stationId : undefined;
    const daysRaw = typeof req.query.days === "string" ? Number(req.query.days) : undefined;
    const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 500;
    const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 5000) : 500;

    const since =
      daysRaw !== undefined && Number.isFinite(daysRaw) && daysRaw > 0
        ? new Date(Date.now() - Math.trunc(daysRaw) * 86400000)
        : undefined;

    const logs = await prisma.detectionLog.findMany({
      where: {
        ...(stationId ? { stationId } : {}),
        ...(since ? { observedAt: { gte: since } } : {}),
        status: "matched",
        titleFinal: { not: null },
      },
      orderBy: { observedAt: "desc" },
      take,
      include: {
        station: { select: { id: true, name: true, province: true, district: true } },
      },
    });
    res.json(logs);
  });

  app.get("/api/logs", async (req, res) => {
    const stationIdQuery = typeof req.query.stationId === "string" ? req.query.stationId : undefined;
    const takeQuery = typeof req.query.take === "string" ? Number(req.query.take) : 500;
    const take = Number.isFinite(takeQuery) ? Math.min(Math.max(Math.trunc(takeQuery), 1), 2000) : 500;
    const statusQuery = typeof req.query.status === "string" ? req.query.status : undefined;

    const where: { stationId?: string; status?: string } = {};
    if (stationIdQuery && stationIdQuery !== "all") where.stationId = stationIdQuery;
    if (statusQuery && statusQuery !== "all") where.status = statusQuery;
    const logs = await prisma.detectionLog.findMany({
      where: Object.keys(where).length > 0 ? where : undefined,
      orderBy: { observedAt: "desc" },
      take,
      include: {
        station: {
          select: {
            id: true,
            name: true,
            country: true
          }
        }
      }
    });

    res.json(logs);
  });

  app.post("/api/stations/:id/probe", async (req, res) => {
    try {
      await MonitorService.pollStation(req.params.id);
      res.json({ status: "probed" });
    } catch (error) {
      res.status(500).json({ error: String(error) });
    }
  });

  app.get("/api/stations/status-overview", async (req, res) => {
    const dedupe = String(req.query?.dedupe ?? "1") !== "0";
    const visibility = String(req.query?.visibility ?? "visible").toLowerCase();
    const stations = await prisma.station.findMany({
      where: visibility === "all" ? undefined : { visibilityEnabled: true },
      select: {
        id: true,
        name: true,
        isActive: true,
        monitorState: true,
        monitorStateReason: true,
        contentClassification: true,
        lastPollAt: true,
        lastHealthyAt: true,
        lastGoodAudioAt: true,
        lastSongDetectedAt: true,
        lastValidationReason: true,
      },
      orderBy: { name: "asc" },
    });
    const out = dedupe ? dedupeStations(stations as unknown as DedupableStation[]) : stations;
    res.json(out);
  });

  app.get("/api/stations/znbc", async (_req, res) => {
    const stations = await prisma.station.findMany({
      where: {
        OR: [
          { name: { contains: "ZNBC" } },
          { sourceIdsJson: { contains: "requested_seed" } },
        ],
      },
      include: { currentNowPlaying: true },
      orderBy: [{ isActive: "desc" }, { name: "asc" }],
    });
    res.json(stations);
  });

  app.get("/api/metrics/summary", async (req, res) => {
    const totalLogs = await prisma.detectionLog.count();
    const matchedLogs = await prisma.detectionLog.count({ where: { status: "matched" } });
    const stationErrors = await prisma.jobRun.count({ where: { status: "failure" } });
    const recentWindow = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const recentLogs = await prisma.detectionLog.count({
      where: { observedAt: { gte: recentWindow } },
    });
    const recentMatched = await prisma.detectionLog.count({
      where: { observedAt: { gte: recentWindow }, status: "matched" },
    });

    const programNoiseWhere = {
      OR: [
        { reasonCode: "talk_or_program_content" },
        { sourceProvider: "stream_metadata_program" },
      ],
    };
    const musicCandidateLogs = await prisma.detectionLog.count({
      where: { NOT: programNoiseWhere },
    });
    const musicMatchedLogs = await prisma.detectionLog.count({
      where: { status: "matched", NOT: programNoiseWhere },
    });
    const recentMusicCandidates = await prisma.detectionLog.count({
      where: { observedAt: { gte: recentWindow }, NOT: programNoiseWhere },
    });
    const recentMusicMatched = await prisma.detectionLog.count({
      where: {
        observedAt: { gte: recentWindow },
        status: "matched",
        NOT: programNoiseWhere,
      },
    });

    const [matchedByMethod24h, allByMethod24h] = await Promise.all([
      prisma.detectionLog.groupBy({
        by: ["detectionMethod"],
        where: { observedAt: { gte: recentWindow }, status: "matched" },
        _count: { _all: true },
      }),
      prisma.detectionLog.groupBy({
        by: ["detectionMethod"],
        where: { observedAt: { gte: recentWindow } },
        _count: { _all: true },
      }),
    ]);

    const toMethodMap = (rows: { detectionMethod: string; _count: { _all: number } }[]) =>
      Object.fromEntries(rows.map((r) => [r.detectionMethod, r._count._all]));

    res.json({
      total_detections: totalLogs,
      match_rate: totalLogs > 0 ? matchedLogs / totalLogs : 0,
      match_rate_24h: recentLogs > 0 ? recentMatched / recentLogs : 0,
      detections_24h: recentLogs,
      /** Same as match_rate but excludes talk/program ICY rows (fairer “song ID” rate). */
      music_match_rate: musicCandidateLogs > 0 ? musicMatchedLogs / musicCandidateLogs : 0,
      music_detections: musicCandidateLogs,
      music_matched: musicMatchedLogs,
      music_match_rate_24h: recentMusicCandidates > 0 ? recentMusicMatched / recentMusicCandidates : 0,
      music_detections_24h: recentMusicCandidates,
      music_matched_24h: recentMusicMatched,
      errors_count: stationErrors,
      /** Proves AcoustID path: matched rows last 24h with detectionMethod fingerprint_acoustid (and similar). */
      matched_by_detection_method_24h: toMethodMap(matchedByMethod24h as never),
      all_detections_by_detection_method_24h: toMethodMap(allByMethod24h as never),
      match_rate_note:
        "Default: trusted ICY can match again (ALLOW_STREAM_METADATA_MATCH_WITHOUT_ID). Use music_match_rate and matched_by_detection_method_24h for AcoustID vs catalog.",
    });
  });

  /** Match pipeline audit: unresolved reasons, JSON diagnostics, station hotspots (SQLite). */
  app.get("/api/metrics/match-pipeline", async (req, res) => {
    try {
      const stationFilter =
        typeof req.query.stationId === "string" && req.query.stationId.trim().length > 0
          ? req.query.stationId.trim()
          : null;
      const since7d = new Date(Date.now() - 7 * 86400000);
      const since24h = new Date(Date.now() - 86400000);

      const baseWhere7 = stationFilter ? { stationId: stationFilter, observedAt: { gte: since7d } } : { observedAt: { gte: since7d } };
      const baseWhere24 = stationFilter
        ? { stationId: stationFilter, observedAt: { gte: since24h } }
        : { observedAt: { gte: since24h } };

      const [total7, matched7, total24, matched24] = await Promise.all([
        prisma.detectionLog.count({ where: baseWhere7 }),
        prisma.detectionLog.count({ where: { ...baseWhere7, status: "matched" } }),
        prisma.detectionLog.count({ where: baseWhere24 }),
        prisma.detectionLog.count({ where: { ...baseWhere24, status: "matched" } }),
      ]);

      const byReason = stationFilter
        ? await prisma.$queryRaw<{ reasonCode: string | null; c: bigint }[]>`
            SELECT "reasonCode" AS reasonCode, COUNT(*) AS c
            FROM "DetectionLog"
            WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d} AND "stationId" = ${stationFilter}
            GROUP BY "reasonCode"
            ORDER BY c DESC
            LIMIT 50
          `
        : await prisma.$queryRaw<{ reasonCode: string | null; c: bigint }[]>`
            SELECT "reasonCode" AS reasonCode, COUNT(*) AS c
            FROM "DetectionLog"
            WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d}
            GROUP BY "reasonCode"
            ORDER BY c DESC
            LIMIT 50
          `;

      const byStation = stationFilter
        ? []
        : await prisma.$queryRaw<{ stationId: string; c: bigint }[]>`
            SELECT "stationId", COUNT(*) AS c
            FROM "DetectionLog"
            WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d}
            GROUP BY "stationId"
            ORDER BY c DESC
            LIMIT 30
          `;

      const stationIdsForNames = byStation.map((r) => r.stationId);
      const stationNames =
        stationIdsForNames.length > 0
          ? await prisma.station.findMany({
              where: { id: { in: stationIdsForNames } },
              select: { id: true, name: true, isActive: true, monitorState: true },
            })
          : [];
      const nameById = new Map(stationNames.map((s) => [s.id, s]));

      let pollReasonRows: { reason: string; c: bigint }[] = [];
      try {
        pollReasonRows = stationFilter
          ? await prisma.$queryRaw<{ reason: string; c: bigint }[]>`
              SELECT COALESCE(json_extract("matchDiagnosticsJson", '$.pollReason'), "reasonCode", 'unknown') AS reason,
                     COUNT(*) AS c
              FROM "DetectionLog"
              WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d} AND "stationId" = ${stationFilter}
              GROUP BY 1 ORDER BY c DESC LIMIT 40
            `
          : await prisma.$queryRaw<{ reason: string; c: bigint }[]>`
              SELECT COALESCE(json_extract("matchDiagnosticsJson", '$.pollReason'), "reasonCode", 'unknown') AS reason,
                     COUNT(*) AS c
              FROM "DetectionLog"
              WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d}
              GROUP BY 1
              ORDER BY c DESC
              LIMIT 40
            `;
      } catch {
        pollReasonRows = stationFilter
          ? await prisma.$queryRaw<{ reason: string; c: bigint }[]>`
              SELECT COALESCE("reasonCode", 'unknown') AS reason, COUNT(*) AS c
              FROM "DetectionLog"
              WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d} AND "stationId" = ${stationFilter}
              GROUP BY 1 ORDER BY c DESC LIMIT 40
            `
          : await prisma.$queryRaw<{ reason: string; c: bigint }[]>`
              SELECT COALESCE("reasonCode", 'unknown') AS reason, COUNT(*) AS c
              FROM "DetectionLog"
              WHERE "status" = 'unresolved' AND "observedAt" >= ${since7d}
              GROUP BY 1 ORDER BY c DESC LIMIT 40
            `;
      }

      res.json({
        stationId_filter: stationFilter,
        window_days: 7,
        match_rate_7d: total7 > 0 ? matched7 / total7 : 0,
        detections_7d: total7,
        matched_7d: matched7,
        match_rate_24h: total24 > 0 ? matched24 / total24 : 0,
        detections_24h: total24,
        matched_24h: matched24,
        unresolved_by_reason_code: byReason.map((r) => ({
          reasonCode: r.reasonCode ?? "null",
          count: Number(r.c),
        })),
        unresolved_by_poll_reason_json: pollReasonRows.map((r) => ({
          pollReason: r.reason,
          count: Number(r.c),
        })),
        top_unresolved_stations_7d: byStation.map((r) => ({
          stationId: r.stationId,
          count: Number(r.c),
          station: nameById.get(r.stationId) ?? null,
        })),
        notes: [
          "Compare music_match_rate in /api/metrics/summary for song-oriented rate.",
          "pollReason groups use matchDiagnosticsJson when present (deploy migration).",
          "Local vs server: compare match_rate_7d with same time window after deploy.",
        ],
      });
    } catch (error) {
      logger.error({ error }, "match-pipeline metrics failed");
      res.status(500).json({ error: "match_pipeline_metrics_failed", detail: String(error) });
    }
  });

  // Song spin analytics (StationSongSpin: one row per song, playCount = total plays)
  app.get("/api/analytics/station-summaries", async (_req, res) => {
    const rows = await prisma.$queryRaw<
      { stationId: string; uniqueSongs: bigint; totalPlays: bigint }[]
    >`
      SELECT "stationId",
        COUNT(*) AS "uniqueSongs",
        COALESCE(SUM("playCount"), 0) AS "totalPlays"
      FROM "StationSongSpin"
      GROUP BY "stationId"
    `;
    res.json(
      rows.map((r) => ({
        stationId: r.stationId,
        uniqueSongs: Number(r.uniqueSongs),
        /** Total matched plays (sum of spin counts); same spirit as former detection row count */
        detectionCount: Number(r.totalPlays),
        totalPlays: Number(r.totalPlays),
      }))
    );
  });

  app.get("/api/analytics/songs", async (req, res) => {
    const stationId = typeof req.query.stationId === "string" ? req.query.stationId : "all";
    const limitRaw = typeof req.query.limit === "string" ? Number(req.query.limit) : 300;
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 2000) : 300;

    if (stationId && stationId !== "all") {
      const rows = await prisma.$queryRaw<
        {
          stationId: string;
          artist: string | null;
          title: string | null;
          album: string | null;
          playCount: number;
          lastPlayed: Date;
          firstPlayed: Date;
          mixRuleApplied: string | null;
          mixSplitConfidence: number | null;
          originalCombinedRaw: string | null;
        }[]
      >`
        SELECT "stationId",
          NULLIF(TRIM("artistLast"), '') AS artist,
          NULLIF(TRIM("titleLast"), '') AS title,
          NULLIF(TRIM("albumLast"), '') AS album,
          "playCount",
          "lastPlayedAt" AS "lastPlayed",
          "firstPlayedAt" AS "firstPlayed",
          "mixRuleApplied",
          "mixSplitConfidence",
          "originalCombinedRaw"
        FROM "StationSongSpin"
        WHERE "stationId" = ${stationId}
          AND TRIM("titleNorm") != ''
        ORDER BY "playCount" DESC
        LIMIT ${limit}
      `;
      return res.json(
        rows.map((r) => ({
          stationId: r.stationId,
          artist: r.artist,
          title: r.title,
          album: r.album,
          playCount: Number(r.playCount),
          lastPlayed: r.lastPlayed,
          firstPlayed: r.firstPlayed,
          mixRuleApplied: r.mixRuleApplied,
          mixSplitConfidence: r.mixSplitConfidence,
          originalCombinedRaw: r.originalCombinedRaw,
        }))
      );
    }

    const rows = await prisma.$queryRaw<
      {
        stationId: string;
        artist: string | null;
        title: string | null;
        album: string | null;
        playCount: number;
        lastPlayed: Date;
        firstPlayed: Date;
        mixRuleApplied: string | null;
        mixSplitConfidence: number | null;
        originalCombinedRaw: string | null;
      }[]
    >`
      SELECT "stationId",
        NULLIF(TRIM("artistLast"), '') AS artist,
        NULLIF(TRIM("titleLast"), '') AS title,
        NULLIF(TRIM("albumLast"), '') AS album,
        "playCount",
        "lastPlayedAt" AS "lastPlayed",
        "firstPlayedAt" AS "firstPlayed",
        "mixRuleApplied",
        "mixSplitConfidence",
        "originalCombinedRaw"
      FROM "StationSongSpin"
      WHERE TRIM("titleNorm") != ''
      ORDER BY "playCount" DESC
      LIMIT ${limit}
    `;
    res.json(
      rows.map((r) => ({
        stationId: r.stationId,
        artist: r.artist,
        title: r.title,
        album: r.album,
        playCount: Number(r.playCount),
        lastPlayed: r.lastPlayed,
        firstPlayed: r.firstPlayed,
        mixRuleApplied: r.mixRuleApplied,
        mixSplitConfidence: r.mixSplitConfidence,
        originalCombinedRaw: r.originalCombinedRaw,
      }))
    );
  });

  /**
   * Songs whose metadata was saved in a poorly-structured way — e.g. artist and
   * title concatenated into one field, or featured artists embedded in the title.
   * These entries need a catalog re-query to split them correctly.
   *
   * Use ?stationId=<id> to narrow to one station, ?take=N to page (max 500).
   */
  app.get("/api/songs/needs-refresh", async (req, res) => {
    try {
      const stationId = typeof req.query.stationId === "string" && req.query.stationId.trim()
        ? req.query.stationId.trim()
        : undefined;
      const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 100;
      const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 500) : 100;
      const result = await SpinRefreshService.listNeedsRefresh({ stationId, take });
      res.json({ ...result, spinRefreshStatus: SpinRefreshService.status() });
    } catch (error) {
      logger.error({ error }, "Failed needs-refresh list request");
      res.status(500).json({ error: "failed_to_list_needs_refresh" });
    }
  });

  /**
   * Trigger an immediate batch of metadata repairs.
   * Body (optional): { stationId?: string, limit?: number (max 200) }
   * The service is self-throttled (1 catalog request / second) and is a no-op
   * when a batch is already running.
   */
  app.post("/api/songs/refresh", async (req, res) => {
    try {
      const body = req.body && typeof req.body === "object" ? req.body as Record<string, unknown> : {};
      const stationId = typeof body.stationId === "string" && body.stationId.trim()
        ? body.stationId.trim()
        : undefined;
      const limitRaw = typeof body.limit === "number" ? body.limit : Number(body.limit);
      const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 200) : 50;
      const out = await SpinRefreshService.runBatch({ stationId, limit });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "Failed songs refresh request");
      res.status(500).json({ error: "failed_to_run_songs_refresh" });
    }
  });

  app.get("/api/export/songs.csv", async (req, res) => {
    const stationId = typeof req.query.stationId === "string" ? req.query.stationId : "all";
    const limitRaw = typeof req.query.limit === "string" ? Number(req.query.limit) : 5000;
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 50000) : 5000;

    const stations = await prisma.station.findMany({
      select: { id: true, name: true, district: true, province: true, frequencyMhz: true },
    });
    const stationMeta = new Map(stations.map((s) => [s.id, s]));

    const rows =
      stationId && stationId !== "all"
        ? await prisma.$queryRaw<
            {
              stationId: string;
              artist: string | null;
              title: string | null;
              album: string | null;
              playCount: number;
              lastPlayed: Date;
              firstPlayed: Date;
            }[]
          >`
            SELECT "stationId",
              NULLIF(TRIM("artistLast"), '') AS artist,
              NULLIF(TRIM("titleLast"), '') AS title,
              NULLIF(TRIM("albumLast"), '') AS album,
              "playCount",
              "lastPlayedAt" AS "lastPlayed",
              "firstPlayedAt" AS "firstPlayed"
            FROM "StationSongSpin"
            WHERE TRIM("titleNorm") != ''
              AND "stationId" = ${stationId}
            ORDER BY "playCount" DESC
            LIMIT ${limit}
          `
        : await prisma.$queryRaw<
            {
              stationId: string;
              artist: string | null;
              title: string | null;
              album: string | null;
              playCount: number;
              lastPlayed: Date;
              firstPlayed: Date;
            }[]
          >`
            SELECT "stationId",
              NULLIF(TRIM("artistLast"), '') AS artist,
              NULLIF(TRIM("titleLast"), '') AS title,
              NULLIF(TRIM("albumLast"), '') AS album,
              "playCount",
              "lastPlayedAt" AS "lastPlayed",
              "firstPlayedAt" AS "firstPlayed"
            FROM "StationSongSpin"
            WHERE TRIM("titleNorm") != ''
            ORDER BY "playCount" DESC
            LIMIT ${limit}
          `;

    const esc = (v: string | null | undefined) => {
      const s = v ?? "";
      if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
      return s;
    };

    const header = [
      "station_id",
      "station_name",
      "province",
      "district",
      "fm_frequency_mhz_from_name",
      "artist",
      "title",
      "album",
      "play_count",
      "first_played_utc",
      "last_played_utc",
    ];

    const lines = [header.join(",")];
    for (const r of rows) {
      const m = stationMeta.get(r.stationId);
      lines.push(
        [
          esc(r.stationId),
          esc(m?.name ?? ""),
          esc(m?.province ?? ""),
          esc(m?.district ?? ""),
          esc(m?.frequencyMhz ?? ""),
          esc(r.artist),
          esc(r.title),
          esc(r.album),
          String(r.playCount),
          esc(r.firstPlayed.toISOString()),
          esc(r.lastPlayed.toISOString()),
        ].join(",")
      );
    }

    const csv = "\uFEFF" + lines.join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", 'attachment; filename="zambia-song-spins.csv"');
    res.send(csv);
  });

  app.get("/api/export/logs.xlsx", async (req, res) => {
    const stationId = typeof req.query.stationId === "string" ? req.query.stationId : "all";
    const limitRaw = typeof req.query.limit === "string" ? Number(req.query.limit) : 5000;
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(Math.trunc(limitRaw), 1), 50000) : 5000;

    const stationWhere = stationId && stationId !== "all" ? { id: stationId } : undefined;
    const stations = await prisma.station.findMany({
      where: stationWhere,
      select: { id: true, name: true },
      orderBy: { name: "asc" },
    });
    const stationNameById = new Map(stations.map((s) => [s.id, s.name]));

    const where = stationId && stationId !== "all" ? { stationId } : undefined;
    const rows = await prisma.detectionLog.findMany({
      where,
      orderBy: { observedAt: "desc" },
      take: limit,
      select: {
        id: true,
        stationId: true,
        observedAt: true,
        rawStreamText: true,
        titleFinal: true,
        artistFinal: true,
        releaseFinal: true,
        trackDurationMs: true,
        processingMs: true,
        confidence: true,
        sourceProvider: true,
        releaseDate: true,
        acoustidId: true,
        recordingMbid: true,
        isrcList: true,
        status: true,
      },
    });

    const toNum = (n: unknown) => (typeof n === "number" && Number.isFinite(n) ? n : undefined);
    const toSec = (ms: number | null) => (typeof ms === "number" && ms > 0 ? Math.round(ms / 1000) : undefined);
    const parseIsrc = (v: string | null): string | undefined => {
      if (!v) return undefined;
      try {
        const arr = JSON.parse(v);
        if (Array.isArray(arr)) return arr.filter((x) => typeof x === "string").join(" / ") || undefined;
      } catch {
        return v || undefined;
      }
      return undefined;
    };
    const timestampLabel = "Timestamp(UTC-07:00)";

    const allResults = rows.map((r, idx) => ({
      No: idx,
      "Channel Name": stationNameById.get(r.stationId) || r.stationId,
      [timestampLabel]: r.observedAt.toISOString().replace("T", " ").slice(0, 19),
      "Bucket ID": "",
      From: stationNameById.get(r.stationId) || r.stationId,
      Title: r.titleFinal || "",
      Artist: r.artistFinal || "",
      Album: r.releaseFinal || "",
      Duration: toSec(r.trackDurationMs ?? null) ?? "",
      "Played Duration": toSec(r.trackDurationMs ?? null) ?? "",
      "Program Title": "",
      Tag: r.sourceProvider || r.status || "",
      Score: toNum(r.confidence ?? null) ?? "",
      Label: "",
      "Release Date": r.releaseDate || "",
      AcoustID: r.acoustidId || r.recordingMbid || "",
      MBID: r.recordingMbid || "",
      ISRC: parseIsrc(r.isrcList) || "",
      ISWC: "",
      UPC: "",
      Deezer: "",
      Spotify: "",
      Youtube: "",
      Composers: "",
      Publishers: "",
    }));

    const statMap = new Map<
      string,
      {
        title: string;
        artist: string;
        album: string;
        from: string;
        label: string;
        isrc: string;
        plays: number;
        playTimeSeconds: number;
        acoustid: string;
        mbid: string;
      }
    >();
    for (const r of rows) {
      if (!r.titleFinal && !r.artistFinal) continue;
      const title = r.titleFinal || "";
      const artist = r.artistFinal || "";
      const album = r.releaseFinal || "";
      const from = stationNameById.get(r.stationId) || r.stationId;
      const label = r.sourceProvider || "";
      const isrc = parseIsrc(r.isrcList) || "";
      const acoustid = r.acoustidId || r.recordingMbid || "";
      const mbid = r.recordingMbid || "";
      const key = [title, artist, album, from, isrc, acoustid].join("||");
      const current = statMap.get(key) || {
        title,
        artist,
        album,
        from,
        label,
        isrc,
        plays: 0,
        playTimeSeconds: 0,
        acoustid,
        mbid,
      };
      current.plays += 1;
      current.playTimeSeconds += toSec(r.trackDurationMs ?? null) || 0;
      statMap.set(key, current);
    }

    const statistic = Array.from(statMap.values())
      .sort((a, b) => b.plays - a.plays)
      .map((x, idx) => ({
        No: idx,
        Title: x.title,
        Artist: x.artist,
        Album: x.album,
        From: x.from,
        Label: x.label,
        ISRC: x.isrc,
        Plays: x.plays,
        "Play Time(seconds)": x.playTimeSeconds,
        AcoustID: x.acoustid,
        MBID: x.mbid,
      }));

    const wb = XLSX.utils.book_new();
    const wsAll = XLSX.utils.json_to_sheet(allResults, {
      header: [
        "No",
        "Channel Name",
        timestampLabel,
        "Bucket ID",
        "From",
        "Title",
        "Artist",
        "Album",
        "Duration",
        "Played Duration",
        "Program Title",
        "Tag",
        "Score",
        "Label",
        "Release Date",
        "AcoustID",
        "MBID",
        "ISRC",
        "ISWC",
        "UPC",
        "Deezer",
        "Spotify",
        "Youtube",
        "Composers",
        "Publishers",
      ],
    });
    const wsStat = XLSX.utils.json_to_sheet(statistic, {
      header: [
        "No",
        "Title",
        "Artist",
        "Album",
        "From",
        "Label",
        "ISRC",
        "Plays",
        "Play Time(seconds)",
        "AcoustID",
        "MBID",
      ],
    });
    XLSX.utils.book_append_sheet(wb, wsAll, "All Results");
    XLSX.utils.book_append_sheet(wb, wsStat, "Statistic");

    const fileName = stationId && stationId !== "all" ? `station-${stationId}-logs.xlsx` : "all-stations-logs.xlsx";
    const buffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.send(buffer);
  });

  // ─── Audio Metadata Editor ────────────────────────────────────────────────

  function normTagStr(val: string): string {
    return String(val || "")
      .normalize("NFKD")
      .replace(/[̀-ͯ]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  async function writeAudioFileTags(
    filePath: string,
    meta: { title?: string; artist?: string; album?: string; genre?: string }
  ): Promise<void> {
    if (!fs.existsSync(filePath)) return;
    const ext = path.extname(filePath).toLowerCase() || ".wav";
    const tmpPath = path.join(os.tmpdir(), `rm_tag_${Date.now()}${ext}`);
    await new Promise<void>((resolve, reject) => {
      const args = ["-y", "-i", filePath];
      if (meta.title) args.push("-metadata", `title=${meta.title}`);
      if (meta.artist) args.push("-metadata", `artist=${meta.artist}`);
      if (meta.album) args.push("-metadata", `album=${meta.album}`);
      if (meta.genre) args.push("-metadata", `genre=${meta.genre}`);
      args.push("-c", "copy", tmpPath);
      const proc = spawn("ffmpeg", args, { stdio: ["ignore", "ignore", "pipe"] });
      proc.on("close", (code) => {
        if (code === 0) resolve();
        else reject(new Error(`ffmpeg tag-write exited ${code}`));
      });
      proc.on("error", reject);
    });
    fs.renameSync(tmpPath, filePath);
  }

  /**
   * LIST unresolved audio samples available for manual metadata editing.
   * Query params:
   *   status: "untagged" (default) | "tagged" | "all"
   *   stationId: filter to one station
   *   take: max rows (1-500, default 100)
   */
  app.get("/api/audio-editor/samples", async (req, res) => {
    try {
      const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 100;
      const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 500) : 100;
      const stationId = typeof req.query.stationId === "string" && req.query.stationId.trim()
        ? req.query.stationId.trim() : undefined;
      const statusFilter = typeof req.query.status === "string" ? req.query.status.trim() : "untagged";

      // Recovery statuses to include
      const recoveryStatuses =
        statusFilter === "tagged"
          ? ["recovered"]
          : statusFilter === "all"
          ? ["pending", "no_match", "error", "recovered"]
          : ["pending", "no_match", "error"];

      const rows = await prisma.unresolvedSample.findMany({
        where: {
          ...(stationId ? { stationId } : {}),
          recoveryStatus: { in: recoveryStatuses },
        },
        orderBy: { createdAt: "desc" },
        take,
        select: {
          id: true,
          stationId: true,
          detectionLogId: true,
          filePath: true,
          createdAt: true,
          recoveryStatus: true,
          recoveryAttempts: true,
          lastRecoveryAt: true,
          recoveredAt: true,
          lastRecoveryError: true,
        },
      });

      const stationIds = [...new Set(rows.map((r) => r.stationId))];
      const stations = stationIds.length
        ? await prisma.station.findMany({
            where: { id: { in: stationIds } },
            select: { id: true, name: true, country: true, province: true },
          })
        : [];
      const stationById = new Map(stations.map((s) => [s.id, s]));

      const logIds = rows.map((r) => r.detectionLogId).filter((id): id is string => !!id);
      const logs = logIds.length
        ? await prisma.detectionLog.findMany({
            where: { id: { in: logIds } },
            select: {
              id: true,
              observedAt: true,
              rawStreamText: true,
              parsedArtist: true,
              parsedTitle: true,
              titleFinal: true,
              artistFinal: true,
              releaseFinal: true,
              genreFinal: true,
              detectionMethod: true,
              reasonCode: true,
              manuallyTagged: true,
              manualTaggedAt: true,
            },
          })
        : [];
      const logById = new Map(logs.map((l) => [l.id, l]));

      const items = rows.map((row) => {
        const station = stationById.get(row.stationId);
        const log = row.detectionLogId ? logById.get(row.detectionLogId) : undefined;
        const hasAudioFile = !!row.filePath && fs.existsSync(row.filePath);
        return {
          id: row.id,
          stationId: row.stationId,
          stationName: station?.name ?? null,
          stationCountry: station?.country ?? null,
          stationProvince: station?.province ?? null,
          detectionLogId: row.detectionLogId ?? null,
          createdAt: row.createdAt,
          recoveryStatus: row.recoveryStatus,
          recoveryAttempts: row.recoveryAttempts,
          lastRecoveryAt: row.lastRecoveryAt ?? null,
          recoveredAt: row.recoveredAt ?? null,
          lastRecoveryError: row.lastRecoveryError ?? null,
          hasAudioFile,
          detectedAt: log?.observedAt ?? null,
          rawStreamText: log?.rawStreamText ?? null,
          parsedArtist: log?.parsedArtist ?? null,
          parsedTitle: log?.parsedTitle ?? null,
          reasonCode: log?.reasonCode ?? null,
          titleFinal: log?.titleFinal ?? null,
          artistFinal: log?.artistFinal ?? null,
          releaseFinal: log?.releaseFinal ?? null,
          genreFinal: log?.genreFinal ?? null,
          manuallyTagged: log?.manuallyTagged ?? false,
          manualTaggedAt: log?.manualTaggedAt ?? null,
        };
      });

      res.json({ total: items.length, statusFilter, items });
    } catch (error) {
      logger.error({ error }, "audio-editor list failed");
      res.status(500).json({ error: "audio_editor_list_failed" });
    }
  });

  /**
   * List unknown/unmatched samples for a station dashboard review pane.
   * This is a Phase-1 read adapter over existing UnresolvedSample + DetectionLog.
   */
  app.get("/api/stations/:stationId/unknown-samples", async (req, res) => {
    try {
      const stationId = String(req.params.stationId || "").trim();
      if (!stationId) {
        res.status(400).json({ error: "station_id_required" });
        return;
      }
      const takeRaw = typeof req.query.take === "string" ? Number(req.query.take) : 200;
      const take = Number.isFinite(takeRaw) ? Math.min(Math.max(Math.trunc(takeRaw), 1), 500) : 200;
      const rows = await prisma.unresolvedSample.findMany({
        where: { stationId },
        orderBy: { createdAt: "desc" },
        take,
        select: {
          id: true,
          stationId: true,
          detectionLogId: true,
          filePath: true,
          createdAt: true,
          recoveryStatus: true,
          recoveredAt: true,
          fingerprintStatus: true,
          verifiedTrackId: true,
        },
      });
      const station = await prisma.station.findUnique({
        where: { id: stationId },
        select: { id: true, name: true },
      });
      const logIds = rows.map((r) => r.detectionLogId).filter((id): id is string => !!id);
      const logs = logIds.length
        ? await prisma.detectionLog.findMany({
            where: { id: { in: logIds } },
            select: {
              id: true,
              observedAt: true,
              rawStreamText: true,
              sourceProvider: true,
              status: true,
              reasonCode: true,
              confidence: true,
              parsedArtist: true,
              parsedTitle: true,
              artistFinal: true,
              titleFinal: true,
            },
          })
        : [];
      const logById = new Map(logs.map((l) => [l.id, l]));
      const items = rows.map((row) => {
        const log = row.detectionLogId ? logById.get(row.detectionLogId) : undefined;
        const fileAvailable = !!row.filePath && fs.existsSync(row.filePath);
        const reviewStatus = row.recoveryStatus === "human_verified" || row.recoveryStatus === "recovered"
          ? "human_verified"
          : "unreviewed";
        return {
          id: row.id,
          stationId: row.stationId,
          stationName: station?.name ?? null,
          capturedAt: log?.observedAt ?? row.createdAt,
          playedAt: log?.observedAt ?? null,
          createdAt: row.createdAt,
          duration: null,
          hasAudio: fileAvailable,
          fileAvailable,
          matchStatus: log?.status ?? "unmatched",
          reviewStatus,
          fingerprintStatus: row.fingerprintStatus ?? "not_started",
          linkedTrackId: row.verifiedTrackId ?? null,
          metadataSource: log?.sourceProvider ?? null,
          rawMetadataText: log?.rawStreamText ?? null,
          suggestedArtist: log?.artistFinal ?? log?.parsedArtist ?? null,
          suggestedTitle: log?.titleFinal ?? log?.parsedTitle ?? null,
          confidence: log?.confidence ?? null,
          reasonCode: log?.reasonCode ?? null,
          audioUrl: `/api/unknown-samples/${encodeURIComponent(row.id)}/audio`,
        };
      });
      res.json({
        stationId,
        stationName: station?.name ?? null,
        total: items.length,
        items,
      });
    } catch (error) {
      logger.error({ error, stationId: req.params.stationId }, "unknown samples list failed");
      res.status(500).json({ error: "unknown_samples_list_failed" });
    }
  });

  function streamAudioFileWithRange(req: express.Request, res: express.Response, filePath: string) {
    const stat = fs.statSync(filePath);
    const fileSize = stat.size;
    const range = req.headers.range;
    const ext = path.extname(filePath).toLowerCase();
    const contentType =
      ext === ".mp3" ? "audio/mpeg" :
      ext === ".ogg" ? "audio/ogg" :
      ext === ".aac" ? "audio/aac" :
      ext === ".flac" ? "audio/flac" :
      ext === ".m4a" ? "audio/mp4" :
      "audio/wav";
    res.setHeader("Content-Type", contentType);
    res.setHeader("Accept-Ranges", "bytes");

    if (!range) {
      res.setHeader("Content-Length", fileSize);
      fs.createReadStream(filePath).pipe(res);
      return;
    }

    const match = /bytes=(\d*)-(\d*)/.exec(range);
    if (!match) {
      res.status(416).setHeader("Content-Range", `bytes */${fileSize}`).end();
      return;
    }
    const start = match[1] ? Number(match[1]) : 0;
    const end = match[2] ? Number(match[2]) : fileSize - 1;
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end < start || end >= fileSize) {
      res.status(416).setHeader("Content-Range", `bytes */${fileSize}`).end();
      return;
    }
    const chunkSize = end - start + 1;
    res.status(206);
    res.setHeader("Content-Range", `bytes ${start}-${end}/${fileSize}`);
    res.setHeader("Content-Length", chunkSize);
    fs.createReadStream(filePath, { start, end }).pipe(res);
  }

  /** Stream the raw audio file for in-browser playback (supports Range requests). */
  app.get("/api/audio-editor/samples/:id/audio", async (req, res) => {
    try {
      const sample = await prisma.unresolvedSample.findUnique({
        where: { id: req.params.id },
        select: { filePath: true },
      });
      if (!sample) {
        res.status(404).json({ error: "sample_not_found" });
        return;
      }
      if (!sample.filePath || !fs.existsSync(sample.filePath)) {
        res.status(404).json({ error: "audio_file_not_on_disk" });
        return;
      }
      streamAudioFileWithRange(req, res, path.resolve(sample.filePath));
    } catch (error) {
      logger.error({ error, id: req.params.id }, "audio-editor stream failed");
      res.status(500).json({ error: "audio_stream_failed" });
    }
  });

  app.get("/api/unknown-samples/:id/audio", async (req, res) => {
    try {
      const sample = await prisma.unresolvedSample.findUnique({
        where: { id: req.params.id },
        select: { filePath: true },
      });
      if (!sample) {
        res.status(404).json({ error: "sample_not_found" });
        return;
      }
      if (!sample.filePath || !fs.existsSync(sample.filePath)) {
        res.status(404).json({ error: "audio_file_not_on_disk" });
        return;
      }
      streamAudioFileWithRange(req, res, path.resolve(sample.filePath));
    } catch (error) {
      logger.error({ error, id: req.params.id }, "unknown-sample stream failed");
      res.status(500).json({ error: "unknown_sample_audio_stream_failed" });
    }
  });

  app.get("/api/unknown-samples/:id", async (req, res) => {
    try {
      const sample = await prisma.unresolvedSample.findUnique({
        where: { id: req.params.id },
      });
      if (!sample) {
        res.status(404).json({ error: "sample_not_found" });
        return;
      }
      const station = await prisma.station.findUnique({
        where: { id: sample.stationId },
        select: { id: true, name: true, country: true, province: true, district: true },
      });
      const log = sample.detectionLogId
        ? await prisma.detectionLog.findUnique({ where: { id: sample.detectionLogId } })
        : null;
      const track = sample.verifiedTrackId
        ? await prisma.verifiedTrack.findUnique({ where: { id: sample.verifiedTrackId } })
        : null;
      const fileAvailable = !!sample.filePath && fs.existsSync(sample.filePath);
      res.json({
        id: sample.id,
        station,
        capturedAt: log?.observedAt ?? sample.createdAt,
        createdAt: sample.createdAt,
        rawStreamMetadata: log?.rawStreamText ?? null,
        parsedArtist: log?.parsedArtist ?? null,
        parsedTitle: log?.parsedTitle ?? null,
        matchStatus: log?.status ?? "unmatched",
        reviewStatus: sample.recoveryStatus,
        fingerprintStatus: sample.fingerprintStatus,
        hasAudio: fileAvailable,
        fileAvailable,
        linkedTrack: track,
        detectionLogId: sample.detectionLogId ?? null,
      });
    } catch (error) {
      logger.error({ error, id: req.params.id }, "unknown sample detail failed");
      res.status(500).json({ error: "unknown_sample_detail_failed" });
    }
  });

  const unknownSampleReviewSchema = z.object({
    artist: z.string().min(1).max(500),
    title: z.string().min(1).max(500),
    album: z.string().max(500).optional(),
    label: z.string().max(300).optional(),
    isrc: z.string().max(40).optional(),
    iswc: z.string().max(40).optional(),
    composerWriter: z.string().max(500).optional(),
    publisher: z.string().max(500).optional(),
    country: z.string().max(120).optional(),
    sourceSociety: z.string().max(120).optional(),
    notes: z.string().max(2000).optional(),
    verificationStatus: z.literal("human_verified").default("human_verified"),
  }).strict();

  app.post("/api/unknown-samples/:id/review", async (req, res) => {
    try {
      const parsed = unknownSampleReviewSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: "invalid_body", details: parsed.error.flatten() });
        return;
      }
      const sample = await prisma.unresolvedSample.findUnique({ where: { id: req.params.id } });
      if (!sample) {
        res.status(404).json({ error: "sample_not_found" });
        return;
      }
      const now = new Date();
      const data = parsed.data;
      const track = await prisma.verifiedTrack.upsert({
        where: { id: sample.verifiedTrackId ?? "__new__" },
        update: {
          artist: data.artist,
          title: data.title,
          album: data.album || null,
          label: data.label || null,
          isrc: data.isrc || null,
          iswc: data.iswc || null,
          composerWriter: data.composerWriter || null,
          publisher: data.publisher || null,
          country: data.country || null,
          sourceSociety: data.sourceSociety || null,
          notes: data.notes || null,
          verificationStatus: data.verificationStatus,
        },
        create: {
          artist: data.artist,
          title: data.title,
          album: data.album || null,
          label: data.label || null,
          isrc: data.isrc || null,
          iswc: data.iswc || null,
          composerWriter: data.composerWriter || null,
          publisher: data.publisher || null,
          country: data.country || null,
          sourceSociety: data.sourceSociety || null,
          notes: data.notes || null,
          verificationStatus: data.verificationStatus,
        },
      });
      await prisma.unresolvedSample.update({
        where: { id: sample.id },
        data: {
          verifiedTrackId: track.id,
          recoveryStatus: "human_verified",
          reviewedAt: now,
          reviewNotes: data.notes || null,
        },
      });
      if (sample.detectionLogId) {
        await prisma.detectionLog.update({
          where: { id: sample.detectionLogId },
          data: {
            titleFinal: data.title,
            artistFinal: data.artist,
            releaseFinal: data.album || null,
            status: "matched",
            sourceProvider: "human_review",
            manuallyTagged: true,
            manualTaggedAt: now,
            verifiedTrackId: track.id,
          },
        });
      }
      res.json({ ok: true, sampleId: sample.id, trackId: track.id, reviewStatus: "human_verified" });
    } catch (error) {
      logger.error({ error, id: req.params.id }, "unknown sample review save failed");
      res.status(500).json({ error: "unknown_sample_review_save_failed" });
    }
  });

  app.post("/api/unknown-samples/:id/save-fingerprint", async (req, res) => {
    try {
      const sample = await prisma.unresolvedSample.findUnique({ where: { id: req.params.id } });
      if (!sample) {
        res.status(404).json({ error: "sample_not_found" });
        return;
      }
      if (!sample.filePath || !fs.existsSync(sample.filePath)) {
        await prisma.unresolvedSample.update({
          where: { id: sample.id },
          data: { fingerprintStatus: "failed" },
        });
        res.status(404).json({ error: "audio_file_not_on_disk" });
        return;
      }
      const track = sample.verifiedTrackId
        ? await prisma.verifiedTrack.findUnique({ where: { id: sample.verifiedTrackId } })
        : null;
      if (!track) {
        res.status(400).json({ error: "review_required_before_fingerprint" });
        return;
      }
      const fp = await FingerprintService.generateFingerprint(path.resolve(sample.filePath));
      if (!fp) {
        await prisma.unresolvedSample.update({
          where: { id: sample.id },
          data: { fingerprintStatus: "failed" },
        });
        res.status(500).json({ error: "fingerprint_generation_failed" });
        return;
      }
      await LocalFingerprintService.learn({
        fp,
        source: "manual",
        match: {
          title: track.title,
          artist: track.artist,
          releaseTitle: track.album ?? undefined,
          labelName: track.label ?? undefined,
          isrcs: track.isrc ? [track.isrc] : undefined,
          countryCode: track.country ?? undefined,
          confidence: 1,
          score: 1,
          sourceProvider: "local_fingerprint",
          reasonCode: "manual_review_fingerprint",
        },
      });
      await prisma.unresolvedSample.update({
        where: { id: sample.id },
        data: { fingerprintStatus: "fingerprinted", fingerprintedAt: new Date() },
      });
      res.json({ ok: true, sampleId: sample.id, fingerprintStatus: "fingerprinted" });
    } catch (error) {
      logger.error({ error, id: req.params.id }, "unknown sample save fingerprint failed");
      res.status(500).json({ error: "unknown_sample_save_fingerprint_failed" });
    }
  });

  app.get("/api/admin/storage/unknown-samples", async (_req, res) => {
    try {
      const summary = await SampleStorageService.storageSummary();
      res.json(summary);
    } catch (error) {
      logger.error({ error }, "unknown sample storage summary failed");
      res.status(500).json({ error: "unknown_sample_storage_summary_failed" });
    }
  });

  app.post("/api/admin/storage/unknown-samples/purge-dry-run", async (_req, res) => {
    try {
      const rows = await prisma.unresolvedSample.findMany({
        orderBy: { createdAt: "desc" },
      });
      const stationIds = [...new Set(rows.map((r) => r.stationId))];
      const stations = stationIds.length
        ? await prisma.station.findMany({ where: { id: { in: stationIds } }, select: { id: true, name: true } })
        : [];
      const stationById = new Map(stations.map((s) => [s.id, s]));
      const logIds = rows.map((r) => r.detectionLogId).filter((id): id is string => !!id);
      const logs = logIds.length
        ? await prisma.detectionLog.findMany({
            where: { id: { in: logIds } },
            select: { id: true, manuallyTagged: true, verifiedTrackId: true },
          })
        : [];
      const logById = new Map(logs.map((l) => [l.id, l]));
      const blockedReasons: Record<string, number> = {};
      let eligibleCount = 0;
      let blockedCount = 0;
      let reclaimableBytes = 0;
      const items = rows.map((row) => {
        const linkedLog = row.detectionLogId ? logById.get(row.detectionLogId) ?? null : null;
        const check = SampleStorageService.checkEligibility(row as any, linkedLog as any);
        if (check.eligible) {
          eligibleCount += 1;
          reclaimableBytes += check.fileSize;
        } else {
          blockedCount += 1;
          blockedReasons[check.reason] = (blockedReasons[check.reason] || 0) + 1;
        }
        return {
          sampleId: row.id,
          stationId: row.stationId,
          stationName: stationById.get(row.stationId)?.name ?? null,
          fileAvailable: check.fileAvailable,
          fileSize: check.fileSize,
          reviewStatus: row.recoveryStatus,
          fingerprintStatus: row.fingerprintStatus,
          verifiedTrackId: row.verifiedTrackId,
          reason: check.reason,
          wouldDelete: check.eligible,
          dryRunOnly: true,
        };
      });
      res.json({
        dryRunOnly: true,
        eligibleCount,
        blockedCount,
        reclaimableBytes,
        blockedReasonsSummary: blockedReasons,
        items,
      });
    } catch (error) {
      logger.error({ error }, "unknown sample purge dry run failed");
      res.status(500).json({ error: "unknown_sample_purge_dry_run_failed" });
    }
  });

  app.post("/api/admin/storage/unknown-samples/hash-backfill-preview", async (req, res) => {
    try {
      const limit = Number(req.body?.limit ?? 100);
      const stationId = typeof req.body?.stationId === "string" ? req.body.stationId : undefined;
      const onlyVerified = req.body?.onlyVerified === true;
      const force = req.body?.force === true;
      const out = await SampleStorageService.previewHashBackfill({ limit, stationId, onlyVerified, force });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "hash backfill preview failed");
      res.status(500).json({ error: "hash_backfill_preview_failed" });
    }
  });

  app.post("/api/admin/storage/unknown-samples/hash-backfill-run", async (req, res) => {
    try {
      const limit = Number(req.body?.limit ?? 100);
      const stationId = typeof req.body?.stationId === "string" ? req.body.stationId : undefined;
      const onlyVerified = req.body?.onlyVerified === true;
      const force = req.body?.force === true;
      const dryRun = req.body?.dryRun !== false;
      const out = await SampleStorageService.runHashBackfill({ limit, stationId, onlyVerified, force, dryRun });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "hash backfill run failed");
      res.status(500).json({ error: "hash_backfill_run_failed" });
    }
  });

  app.get("/api/admin/crawler/status", async (_req, res) => {
    try {
      const summary = await CatalogCrawlerService.statusSummary();
      res.json(summary);
    } catch (error) {
      logger.error({ error }, "crawler status failed");
      res.status(500).json({ error: "crawler_status_failed" });
    }
  });

  app.post("/api/admin/crawler/discover", async (req, res) => {
    try {
      const urls = Array.isArray(req.body?.urls) ? req.body.urls.filter((u: unknown) => typeof u === "string") : [];
      if (!urls.length) {
        res.status(400).json({ error: "urls_required" });
        return;
      }
      const out = await CatalogCrawlerService.discoverAndUpsert({
        urls,
        sourceType: typeof req.body?.sourceType === "string" ? req.body.sourceType : "manual_seed",
        discoveredFrom: typeof req.body?.discoveredFrom === "string" ? req.body.discoveredFrom : undefined,
      });
      res.json({ discovered: out.length, rows: out });
    } catch (error) {
      logger.error({ error }, "crawler discover failed");
      res.status(500).json({ error: "crawler_discover_failed" });
    }
  });


  app.post("/api/admin/crawler/run-priority", async (req, res) => {
    try {
      const out = await CatalogCrawlerService.runPriorityBatch({
        maxUrlsPerRun: Number(req.body?.maxUrlsPerRun ?? 25),
        maxFingerprintsPerRun: Number(req.body?.maxFingerprintsPerRun ?? 10),
        minDurationSeconds: Number(req.body?.minDurationSeconds ?? 30),
        maxDurationSeconds: Number(req.body?.maxDurationSeconds ?? 540),
        skipIfTrackAlreadyHasFingerprint: req.body?.skipIfTrackAlreadyHasFingerprint !== false,
      });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "crawler run priority failed");
      res.status(500).json({ error: "crawler_run_priority_failed" });
    }
  });


  app.get("/api/admin/crawler/duplicate-reviews", async (_req, res) => {
    try {
      const rows = await prisma.catalogDuplicateReview.findMany({ where: { status: "pending" }, orderBy: { createdAt: "desc" }, take: 100 });
      res.json({ total: rows.length, rows });
    } catch (error) {
      logger.error({ error }, "crawler duplicate reviews failed");
      res.status(500).json({ error: "crawler_duplicate_reviews_failed" });
    }
  });


  app.get("/api/admin/rematch/summary", async (_req, res) => {
    try { res.json(await RematchService.summary()); } catch (error) { logger.error({ error }, "rematch summary failed"); res.status(500).json({ error: "rematch_summary_failed" }); }
  });

  app.post("/api/admin/rematch/preview", async (req, res) => {
    try {
      const out = await RematchService.previewRematchCandidates({ stationId: typeof req.body?.stationId==="string"?req.body.stationId:undefined, onlyUnknowns: req.body?.onlyUnknowns===true, onlyWeakMatches: req.body?.onlyWeakMatches===true, limit: Number(req.body?.limit ?? 100) });
      res.json(out);
    } catch (error) { logger.error({ error }, "rematch preview failed"); res.status(500).json({ error: "rematch_preview_failed" }); }
  });

  app.post("/api/admin/rematch/run", async (req, res) => {
    try { const out = await RematchService.runRematchBatch({ limit: Number(req.body?.limit ?? 50), dryRun: req.body?.dryRun !== false, minConfidenceToApply: Number(req.body?.minConfidenceToApply ?? 0.9) }); res.json(out); }
    catch (error) { logger.error({ error }, "rematch run failed"); res.status(500).json({ error: "rematch_run_failed" }); }
  });

  app.post("/api/admin/rematch/create-for-new-fingerprints", async (req, res) => {
    try { const out = await RematchService.createRematchJobsForNewFingerprint({ maxRematchJobsPerFingerprint: Number(req.body?.maxRematchJobsPerFingerprint ?? 200), stationId: typeof req.body?.stationId==="string"?req.body.stationId:undefined, onlyRecentDays: req.body?.onlyRecentDays ? Number(req.body.onlyRecentDays) : undefined }); res.json(out); }
    catch (error) { logger.error({ error }, "rematch create failed"); res.status(500).json({ error: "rematch_create_failed" }); }
  });

  app.post("/api/admin/external-recognition/test", async (req, res) => {
    try {
      const out = await ExternalRecognitionService.testUnknownSample({
        unknownSampleId: String(req.body?.unknownSampleId || ""),
        provider: (req.body?.provider || "shazam_api_experimental") as any,
        dryRun: req.body?.dryRun !== false,
      });
      res.json(out);
    } catch (error) {
      logger.error({ error }, "external recognition test failed");
      res.status(500).json({ error: "external_recognition_test_failed" });
    }
  });

  app.get("/api/admin/external-recognition/summary", async (_req, res) => {
    try { res.json(await ExternalRecognitionService.summary()); }
    catch (error) { logger.error({ error }, "external recognition summary failed"); res.status(500).json({ error: "external_recognition_summary_failed" }); }
  });
  const audioEditorPatchSchema = z.object({
    title: z.string().max(500).optional(),
    artist: z.string().max(500).optional(),
    album: z.string().max(500).optional(),
    genre: z.string().max(200).optional(),
  }).strict();

  /**
   * Save manually entered metadata for an unresolved audio sample.
   * Updates DetectionLog, StationSongSpin, UnresolvedSample, and embeds
   * ID3/WAV tags into the audio file so the recording is self-describing.
   */
  app.patch("/api/audio-editor/samples/:id", async (req, res) => {
    try {
      const parsed = audioEditorPatchSchema.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: "invalid_body", details: parsed.error.flatten() });
        return;
      }
      const { title = "", artist = "", album = "", genre = "" } = parsed.data;

      const sample = await prisma.unresolvedSample.findUnique({
        where: { id: req.params.id },
        select: { id: true, stationId: true, detectionLogId: true, filePath: true, recoveryStatus: true },
      });
      if (!sample) {
        res.status(404).json({ error: "sample_not_found" });
        return;
      }

      const now = new Date();

      // 1. Update DetectionLog (if linked)
      if (sample.detectionLogId) {
        await prisma.detectionLog.update({
          where: { id: sample.detectionLogId },
          data: {
            titleFinal: title || null,
            artistFinal: artist || null,
            releaseFinal: album || null,
            genreFinal: genre || null,
            status: "matched",
            sourceProvider: "manual",
            manuallyTagged: true,
            manualTaggedAt: now,
          },
        });
      }

      // 2. Mark UnresolvedSample as recovered
      await prisma.unresolvedSample.update({
        where: { id: sample.id },
        data: {
          recoveryStatus: "recovered",
          recoveredAt: now,
        },
      });

      // 3. Upsert StationSongSpin so song appears in analytics
      const titleNorm = normTagStr(title);
      if (titleNorm) {
        const artistNorm = normTagStr(artist);
        const albumNorm = normTagStr(album);
        const credit = parseFeaturedFromArtist(artist);
        const primaryArtist = credit.primaryArtist || artist;
        await prisma.stationSongSpin.upsert({
          where: {
            stationId_artistNorm_titleNorm_albumNorm: {
              stationId: sample.stationId,
              artistNorm,
              titleNorm,
              albumNorm,
            },
          },
          create: {
            stationId: sample.stationId,
            artistNorm,
            titleNorm,
            albumNorm,
            artistLast: artist,
            titleLast: title,
            albumLast: album,
            playCount: 1,
            firstPlayedAt: now,
            lastPlayedAt: now,
            lastDetectionLogId: sample.detectionLogId ?? null,
            manuallyTagged: true,
          },
          update: {
            artistLast: artist,
            titleLast: title,
            albumLast: album,
            lastPlayedAt: now,
            lastDetectionLogId: sample.detectionLogId ?? null,
            manuallyTagged: true,
          },
        });
        await LocalFingerprintService.bumpPlayAggregates({
          recordingMbid: null,
          artist: primaryArtist,
          title: title || null,
        });
      }

      // 4. Create LocalFingerprint entry if a chromaprint is stored in SongSampleArchive
      if (sample.detectionLogId) {
        const archive = await prisma.songSampleArchive.findUnique({
          where: { detectionLogId: sample.detectionLogId },
          select: { chromaprint: true, durationSec: true },
        });
        if (archive?.chromaprint && archive.durationSec) {
          const sha1 = crypto.createHash("sha1").update(archive.chromaprint).digest("hex");
          const prefix = archive.chromaprint.substring(0, 48);
          const credit = parseFeaturedFromArtist(artist);
          const titleWo = titleWithoutFeaturing(title) || null;
          const featuredJson = credit.featured.length ? JSON.stringify(credit.featured) : null;
          const durationMs =
            archive.durationSec > 0 ? Math.round(archive.durationSec * 1000) : null;
          await prisma.localFingerprint.upsert({
            where: { fingerprintSha1: sha1 },
            create: {
              fingerprint: archive.chromaprint,
              fingerprintSha1: sha1,
              fingerprintPrefix: prefix,
              durationSec: archive.durationSec,
              title: title || null,
              artist: credit.primaryArtist || artist || null,
              displayArtist: artist.trim() ? artist : null,
              titleWithoutFeat: titleWo,
              featuredArtistsJson: featuredJson,
              releaseTitle: album || null,
              genre: genre || null,
              durationMs,
              source: "manual",
              confidence: 1.0,
              timesMatched: 1,
            },
            update: {
              title: title || null,
              artist: credit.primaryArtist || artist || null,
              displayArtist: artist.trim() ? artist : null,
              titleWithoutFeat: titleWo,
              featuredArtistsJson: featuredJson,
              releaseTitle: album || null,
              genre: genre || null,
              durationMs,
              source: "manual",
              confidence: 1.0,
              timesMatched: { increment: 1 },
              lastMatchedAt: now,
              updatedAt: now,
            },
          });
        }
      }

      // 5. Embed metadata tags into the audio file via ffmpeg (best-effort)
      if (sample.filePath && fs.existsSync(sample.filePath)) {
        writeAudioFileTags(sample.filePath, { title, artist, album, genre }).catch((err) => {
          logger.warn({ err, filePath: sample.filePath }, "audio tag write failed (non-fatal)");
        });
      }

      res.json({
        ok: true,
        sampleId: sample.id,
        detectionLogId: sample.detectionLogId,
        manuallyTagged: true,
        manualTaggedAt: now,
        title,
        artist,
        album,
        genre,
      });
    } catch (error) {
      logger.error({ error, id: req.params.id }, "audio-editor patch failed");
      res.status(500).json({ error: "audio_editor_patch_failed" });
    }
  });

  /**
   * Identify an unresolved audio sample via AcoustID and/or AudD.
   * Returns the match metadata without saving — caller confirms via PATCH.
   *
   * Body: { provider?: "acoustid" | "audd" | "auto" }
   * "auto" tries AcoustID first, then AudD on miss.
   */
  app.post("/api/audio-editor/samples/:id/identify", async (req, res) => {
    try {
      const provider =
        typeof req.body?.provider === "string"
          ? req.body.provider.trim().toLowerCase()
          : "auto";
      if (!["acoustid", "audd", "auto"].includes(provider)) {
        res.status(400).json({ ok: false, error: "invalid_provider" });
        return;
      }

      const sample = await prisma.unresolvedSample.findUnique({
        where: { id: req.params.id },
        select: { id: true, filePath: true, stationId: true },
      });
      if (!sample) {
        res.status(404).json({ ok: false, error: "sample_not_found" });
        return;
      }
      if (!sample.filePath || !fs.existsSync(sample.filePath)) {
        res.status(422).json({ ok: false, error: "audio_file_not_on_disk" });
        return;
      }

      const tryAcoustid =
        (provider === "auto" || provider === "acoustid") &&
        (!!process.env.ACOUSTID_API_KEY || !!process.env.ACOUSTID_OPEN_CLIENT);
      const tryAudd =
        (provider === "auto" || provider === "audd") && AuddService.isEnabled();

      if (!tryAcoustid && !tryAudd) {
        res.status(422).json({
          ok: false,
          error: "no_provider_available",
          detail:
            provider === "acoustid"
              ? "ACOUSTID_API_KEY / ACOUSTID_OPEN_CLIENT not configured"
              : provider === "audd"
              ? "AUDD_API_TOKEN not configured"
              : "Neither AcoustID nor AudD is configured",
        });
        return;
      }

      type MatchResult = import("./types.js").MatchResult;
      let matchResult: MatchResult | null = null;
      let usedProvider: "acoustid" | "audd" | null = null;
      let acoustidScore: number | null = null;
      let fingerprintOk = false;

      if (tryAcoustid) {
        const releaseGate = await fingerprintPipelineGate.acquire();
        let fp: Awaited<ReturnType<typeof FingerprintService.generateFingerprint>>;
        try {
          fp = await FingerprintService.generateFingerprint(sample.filePath);
        } finally {
          releaseGate();
        }
        if (fp) {
          fingerprintOk = true;
          const acoustid = await AcoustidService.lookup(fp);
          if (acoustid) {
            acoustidScore = acoustid.score ?? null;
            const enriched = await MusicbrainzService.enrich(acoustid);
            matchResult = enriched || acoustid;
            usedProvider = "acoustid";
          }
        }
      }

      if (!matchResult && tryAudd) {
        const auddMatch = await AuddService.lookupSample(sample.filePath);
        if (auddMatch) {
          matchResult = auddMatch;
          usedProvider = "audd";
        }
      }

      if (!matchResult) {
        const tried: string[] = [];
        if (tryAcoustid) tried.push(fingerprintOk ? "acoustid (no match)" : "acoustid (fingerprint failed)");
        if (tryAudd) tried.push("audd (no match)");
        res.json({ ok: false, error: "no_match_found", tried });
        return;
      }

      res.json({
        ok: true,
        title: matchResult.title ?? null,
        artist: matchResult.artist ?? null,
        album: matchResult.releaseTitle ?? null,
        genre: matchResult.genre ?? null,
        score: matchResult.score ?? matchResult.confidence ?? null,
        acoustidScore,
        provider: usedProvider,
        recordingId: matchResult.recordingId ?? null,
      });
    } catch (error) {
      logger.error({ error, id: req.params.id }, "audio-editor identify failed");
      res.status(500).json({ ok: false, error: "identify_failed" });
    }
  });

  // ── end Audio Metadata Editor ─────────────────────────────────────────────

  // Initialize Scheduler
  await SchedulerService.init();
  
  // Vite middleware for development
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    const staticPath = path.join(process.cwd(), 'dist/client');
    app.use(express.static(staticPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(staticPath, 'index.html'));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    logger.info(`Server running on http://localhost:${PORT}`);
  });
}

startServer().catch(err => {
  logger.error(err, "Failed to start server");
});

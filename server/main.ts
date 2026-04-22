import "dotenv/config";
import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import cors from "cors";
import helmet from "helmet";
import { spawnSync } from "child_process";
import { logger } from "./lib/logger.js";
import { prisma } from "./lib/prisma.js";
import { z } from "zod";
import { SchedulerService } from "./services/scheduler.service.js";
import { MonitorService } from "./services/monitor.service.js";
import { StreamRefreshService } from "./services/stream-refresh.service.js";
import { validateCandidateStreamUrl } from "./lib/stream-url-guard.js";
import { StreamHealthService } from "./services/stream-health.service.js";
import { monitorEvents } from "./lib/monitor-events.js";

function isCommandAvailable(command: string, args: string[] = ["-version"]) {
  try {
    const result = spawnSync(command, args, { stdio: "ignore" });
    return result.status === 0;
  } catch {
    return false;
  }
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
    monitorEvents.on("song-detected", onSongDetected);

    req.on("close", () => {
      clearInterval(heartbeat);
      monitorEvents.off("song-detected", onSongDetected);
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
    };

    const missing = [];
    if (!ffmpeg) missing.push("ffmpeg");
    if (!ffprobe) missing.push("ffprobe");
    if (!fpcalc) missing.push("fpcalc");
    if (!acoustidApiKeyConfigured) missing.push("ACOUSTID_API_KEY");
    if (!musicbrainzUserAgentConfigured) missing.push("MUSICBRAINZ_USER_AGENT");

    res.json({
      ffmpeg,
      ffprobe,
      fpcalc,
      acoustidApiKeyConfigured,
      musicbrainzUserAgentConfigured,
      catalogLookupReady,
      freeApisEnabled,
      fingerprintReady: ffmpeg && ffprobe && fpcalc && acoustidApiKeyConfigured,
      missing
    });
  });

  // Stations API
  app.get("/api/stations", async (req, res) => {
    const stations = await prisma.station.findMany({
      where: { visibilityEnabled: true },
      include: { currentNowPlaying: true },
      orderBy: [{ name: "asc" }],
    });
    res.json(stations);
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
    const health = await StreamHealthService.validateStream(st.streamUrl);
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
        lastGoodAudioAt: health.decoderOk ? now : st.lastGoodAudioAt,
      },
    });
    res.json({ stationId: st.id, health });
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

  app.get("/api/stations/status-overview", async (_req, res) => {
    const stations = await prisma.station.findMany({
      where: { visibilityEnabled: true },
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
    res.json(stations);
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
    const matchedLogs = await prisma.detectionLog.count({ where: { status: 'matched' } });
    const stationErrors = await prisma.jobRun.count({ where: { status: 'failure' } });
    const recentWindow = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const recentLogs = await prisma.detectionLog.count({
      where: { observedAt: { gte: recentWindow } },
    });
    const recentMatched = await prisma.detectionLog.count({
      where: { observedAt: { gte: recentWindow }, status: "matched" },
    });
    
    res.json({
      total_detections: totalLogs,
      match_rate: totalLogs > 0 ? (matchedLogs / totalLogs) : 0,
      match_rate_24h: recentLogs > 0 ? (recentMatched / recentLogs) : 0,
      detections_24h: recentLogs,
      errors_count: stationErrors
    });
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
    res.json(
      rows.map((r) => ({
        stationId: r.stationId,
        artist: r.artist,
        title: r.title,
        album: r.album,
        playCount: Number(r.playCount),
        lastPlayed: r.lastPlayed,
        firstPlayed: r.firstPlayed,
      }))
    );
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

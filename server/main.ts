import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import cors from "cors";
import helmet from "helmet";
import { spawnSync } from "child_process";
import { logger } from "./lib/logger.js";
import { prisma } from "./lib/prisma.js";
import { SchedulerService } from "./services/scheduler.service.js";
import { MonitorService } from "./services/monitor.service.js";

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
      itunesSearch: true
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
      include: { currentNowPlaying: true }
    });
    res.json(stations);
  });

  app.post("/api/stations", async (req, res) => {
    const station = await prisma.station.create({ data: req.body });
    res.json(station);
  });

  app.patch("/api/stations/:id", async (req, res) => {
    const station = await prisma.station.update({
      where: { id: req.params.id },
      data: req.body
    });
    res.json(station);
  });

  app.get("/api/stations/:id/logs", async (req, res) => {
    const logs = await prisma.detectionLog.findMany({
      where: { stationId: req.params.id },
      orderBy: { observedAt: 'desc' },
      take: 100
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
    const takeQuery = typeof req.query.take === "string" ? Number(req.query.take) : 100;
    const take = Number.isFinite(takeQuery) ? Math.min(Math.max(Math.trunc(takeQuery), 1), 500) : 100;
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

  app.get("/api/metrics/summary", async (req, res) => {
    const totalLogs = await prisma.detectionLog.count();
    const matchedLogs = await prisma.detectionLog.count({ where: { status: 'matched' } });
    const stationErrors = await prisma.jobRun.count({ where: { status: 'failure' } });
    
    res.json({
      total_detections: totalLogs,
      match_rate: totalLogs > 0 ? (matchedLogs / totalLogs) : 0,
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

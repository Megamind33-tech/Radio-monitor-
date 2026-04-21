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
        status: true
      }
    });
    res.json(airplays);
  });

  app.get("/api/logs", async (req, res) => {
    const stationIdQuery = typeof req.query.stationId === "string" ? req.query.stationId : undefined;
    const takeQuery = typeof req.query.take === "string" ? Number(req.query.take) : 100;
    const take = Number.isFinite(takeQuery) ? Math.min(Math.max(Math.trunc(takeQuery), 1), 500) : 100;

    const where = stationIdQuery && stationIdQuery !== "all" ? { stationId: stationIdQuery } : undefined;
    const logs = await prisma.detectionLog.findMany({
      where,
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

  // Internal seed for dev
  if (process.env.NODE_ENV !== "production") {
    const count = await prisma.station.count();
    if (count === 0) {
      logger.info("Seeding sample stations");
      const samples = [
        {
          name: "BBC Radio 1",
          country: "UK",
          streamUrl: "http://stream.live.vc.bbc.co.uk/bbc_radio_one",
          pollIntervalSeconds: 60
        },
        {
          name: "FIP - Paris",
          country: "FR",
          streamUrl: "http://icecast.radiofrance.fr/fip-midfi.mp3",
          pollIntervalSeconds: 60
        }
      ];
      
      for (const sample of samples) {
        await prisma.station.create({ data: sample });
      }
    }
  }

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

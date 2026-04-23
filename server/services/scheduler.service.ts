import * as cron from "node-cron";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { MonitorService } from "./monitor.service.js";
import { UnresolvedRecoveryService } from "./unresolved-recovery.service.js";

type PollHandle = { stop: () => void };

export class SchedulerService {
  private static tasks: Map<string, PollHandle> = new Map();
  /** Last scheduled poll interval per station (seconds) — resync when DB value changes */
  private static pollIntervals: Map<string, number> = new Map();
  private static pollRunning: Set<string> = new Set();
  private static masterJob: cron.ScheduledTask | null = null;
  private static unresolvedRecoveryJob: cron.ScheduledTask | null = null;

  static async init() {
    logger.info("Initializing station scheduler");

    // Keep scheduler state in sync every 30s (not only once per minute),
    // so station toggles and interval changes are reflected quickly.
    this.masterJob = cron.schedule("*/30 * * * * *", () => {
      void this.resyncStations();
    });
    this.unresolvedRecoveryJob = cron.schedule("*/2 * * * *", () => {
      void this.runUnresolvedRecoveryTick();
    });

    await this.resyncStations();
  }

  private static async runUnresolvedRecoveryTick() {
    if (!process.env.ACOUSTID_API_KEY) return;
    try {
      const out = await UnresolvedRecoveryService.runBatch();
      if (out.processed > 0) {
        logger.info({ out }, "Unresolved recovery scheduler tick");
      }
    } catch (error) {
      logger.warn({ error }, "Unresolved recovery scheduler tick failed");
    }
  }

  private static async resyncStations() {
    const stations = await prisma.station.findMany({ where: { isActive: true } });
    const activeIds = new Set(stations.map((s) => s.id));

    for (const [id, handle] of this.tasks) {
      if (!activeIds.has(id)) {
        logger.info({ stationId: id }, "Stopping scheduler for station");
        handle.stop();
        this.tasks.delete(id);
        this.pollIntervals.delete(id);
      }
    }

    for (const station of stations) {
      const prevInterval = this.pollIntervals.get(station.id);
      const interval = this.normalizePollSeconds(station.pollIntervalSeconds);
      if (this.tasks.has(station.id) && prevInterval !== undefined && prevInterval !== interval) {
        logger.info({ stationId: station.id, prevInterval, interval }, "Poll interval changed; rescheduling");
        this.tasks.get(station.id)?.stop();
        this.tasks.delete(station.id);
      }
      if (!this.tasks.has(station.id)) {
        this.scheduleStation(station, interval);
      }
    }
  }

  /** 5s–3600s; node-cron second field only supports 1–59, so longer intervals use setInterval. */
  private static normalizePollSeconds(raw: unknown): number {
    const n = typeof raw === "number" ? raw : Number(raw);
    const v = Number.isFinite(n) ? Math.trunc(n) : 60;
    return Math.min(3600, Math.max(5, v || 60));
  }

  private static scheduleStation(station: { id: string; name: string }, pollIntervalSeconds: number) {
    logger.info({ station: station.name, interval: pollIntervalSeconds }, "Scheduling station poll");

    const runPoll = async () => {
      if (this.pollRunning.has(station.id)) {
        logger.debug({ stationId: station.id }, "Skipping overlapping poll tick");
        return;
      }
      this.pollRunning.add(station.id);
      try {
        await MonitorService.pollStation(station.id);
      } finally {
        this.pollRunning.delete(station.id);
      }
    };

    let handle: PollHandle;
    if (pollIntervalSeconds <= 59) {
      const cronExpr = `*/${pollIntervalSeconds} * * * * *`;
      const task = cron.schedule(cronExpr, () => {
        void runPoll();
      });
      handle = { stop: () => task.stop() };
    } else {
      const id = setInterval(() => {
        void runPoll();
      }, pollIntervalSeconds * 1000);
      handle = { stop: () => clearInterval(id) };
    }

    // Prime immediately so stations don't wait for the first interval tick.
    void runPoll();

    this.tasks.set(station.id, handle);
    this.pollIntervals.set(station.id, pollIntervalSeconds);
  }

  static stopAll() {
    this.masterJob?.stop();
    this.unresolvedRecoveryJob?.stop();
    for (const h of this.tasks.values()) {
      h.stop();
    }
    this.tasks.clear();
    this.pollIntervals.clear();
    this.pollRunning.clear();
  }
}

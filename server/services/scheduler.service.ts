import * as cron from "node-cron";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { MonitorService } from "./monitor.service.js";

type PollHandle = { stop: () => void };

export class SchedulerService {
  private static tasks: Map<string, PollHandle> = new Map();
  /** Last scheduled poll interval per station (seconds) — resync when DB value changes */
  private static pollIntervals: Map<string, number> = new Map();
  private static masterJob: cron.ScheduledTask | null = null;

  static async init() {
    logger.info("Initializing station scheduler");

    this.masterJob = cron.schedule("*/1 * * * *", () => {
      void this.resyncStations();
    });

    await this.resyncStations();
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

    let handle: PollHandle;
    if (pollIntervalSeconds <= 59) {
      const cronExpr = `*/${pollIntervalSeconds} * * * * *`;
      const task = cron.schedule(cronExpr, async () => {
        await MonitorService.pollStation(station.id);
      });
      handle = { stop: () => task.stop() };
    } else {
      const id = setInterval(() => {
        void MonitorService.pollStation(station.id);
      }, pollIntervalSeconds * 1000);
      handle = { stop: () => clearInterval(id) };
    }

    this.tasks.set(station.id, handle);
    this.pollIntervals.set(station.id, pollIntervalSeconds);
  }

  static stopAll() {
    this.masterJob?.stop();
    for (const h of this.tasks.values()) {
      h.stop();
    }
    this.tasks.clear();
    this.pollIntervals.clear();
  }
}

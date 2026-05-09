import * as cron from "node-cron";
import { prisma } from "../lib/prisma.js";
import { logger } from "../lib/logger.js";
import { MonitorService } from "./monitor.service.js";
import { UnresolvedRecoveryService } from "./unresolved-recovery.service.js";
import { SpinRefreshService } from "./spin-refresh.service.js";
import { CatalogRepairService } from "./catalog-repair.service.js";
import { RematchService } from "./rematch.service.js";

type ScheduledStation = {
  id: string;
  name: string;
  intervalSeconds: number;
  nextDueAt: number;
};

export class SchedulerService {
  private static scheduledStations: Map<string, ScheduledStation> = new Map();
  /** Last scheduled poll interval per station (seconds) - resync when DB value changes */
  private static pollIntervals: Map<string, number> = new Map();
  private static pollRunning: Set<string> = new Set();
  private static stationOrder: string[] = [];
  private static masterJob: cron.ScheduledTask | null = null;
  private static dispatchTimer: NodeJS.Timeout | null = null;
  private static unresolvedRecoveryJob: cron.ScheduledTask | null = null;
  private static spinRefreshJob: cron.ScheduledTask | null = null;
  private static catalogRepairJob: cron.ScheduledTask | null = null;
  private static rematchJob: cron.ScheduledTask | null = null;

  static async init() {
    logger.info("Initializing station scheduler");

    this.masterJob = cron.schedule("*/30 * * * * *", () => {
      void this.resyncStations();
    });
    const dispatchTickMs = Math.max(
      1000,
      Math.min(30_000, parseInt(process.env.SCHEDULER_DISPATCH_TICK_MS || "5000", 10) || 5000)
    );
    this.dispatchTimer = setInterval(() => {
      void this.dispatchDuePolls();
    }, dispatchTickMs);
    this.unresolvedRecoveryJob = cron.schedule("*/2 * * * *", () => {
      void this.runUnresolvedRecoveryTick();
    });
    this.spinRefreshJob = cron.schedule("5 */30 * * * *", () => {
      void this.runSpinRefreshTick();
    });
    this.catalogRepairJob = cron.schedule("20 */3 * * * *", () => {
      void this.runCatalogRepairTick();
    });
    this.rematchJob = cron.schedule("40 */4 * * * *", () => {
      void this.runRematchTick();
    });

    await this.resyncStations();
    await this.dispatchDuePolls();
  }


  private static async runRematchTick() {
    if (String(process.env.REMATCH_ENABLED || "true").toLowerCase() === "false") return;
    try {
      await RematchService.createRematchJobsForNewFingerprint({ maxRematchJobsPerFingerprint: Math.min(200, parseInt(process.env.REMATCH_CREATE_LIMIT || "100", 10) || 100) });
      const out = await RematchService.runRematchBatch({ limit: Math.min(100, parseInt(process.env.REMATCH_RUN_LIMIT || "40", 10) || 40), dryRun: String(process.env.REMATCH_DRY_RUN || "true").toLowerCase() !== "false" });
      if (out.processed > 0) logger.info({ out }, "Rematch scheduler tick");
    } catch (error) {
      logger.warn({ error }, "Rematch scheduler tick failed");
    }
  }

  private static async runCatalogRepairTick() {
    if (String(process.env.CATALOG_REPAIR_ENABLED || "true").toLowerCase() === "false") return;
    try {
      const limit = Math.min(
        80,
        Math.max(5, parseInt(process.env.CATALOG_REPAIR_BATCH_LIMIT || "25", 10) || 25)
      );
      const out = await CatalogRepairService.runBatch({ limit });
      if (out.repaired > 0) logger.info({ out }, "Catalog repair scheduler tick");
    } catch (error) {
      logger.warn({ error }, "Catalog repair scheduler tick failed");
    }
  }

  private static async runSpinRefreshTick() {
    try {
      const out = await SpinRefreshService.runBatch({ limit: 30 });
      if (out.refreshed > 0 || out.scanned > 0) logger.info({ out }, "Spin metadata refresh scheduler tick");
    } catch (error) {
      logger.warn({ error }, "Spin metadata refresh scheduler tick failed");
    }
  }

  private static async runUnresolvedRecoveryTick() {
    const forceWithoutAcoustid =
      String(process.env.UNRESOLVED_FORCE_RETRY_WITHOUT_ACOUSTID || "").toLowerCase() === "true";
    if (!process.env.ACOUSTID_API_KEY && !forceWithoutAcoustid) return;
    try {
      const forcePasses = Math.max(
        1,
        Math.min(200, parseInt(process.env.UNRESOLVED_FORCE_MAX_PASSES || "25", 10) || 25)
      );
      const out = forceWithoutAcoustid
        ? await UnresolvedRecoveryService.runUntilDrained({
            continueWithoutAcoustid: true,
            maxPasses: forcePasses,
          })
        : await UnresolvedRecoveryService.runBatch();
      if (out.processed > 0 || ("remainingPending" in out && Number((out as { remainingPending?: number }).remainingPending ?? 0) > 0)) {
        logger.info({ out }, "Unresolved recovery scheduler tick");
      }
    } catch (error) {
      logger.warn({ error }, "Unresolved recovery scheduler tick failed");
    }
  }

  private static async resyncStations() {
    const stations = await prisma.station.findMany({ where: { isActive: true } });
    const activeIds = new Set(stations.map((s) => s.id));
    this.stationOrder = stations.map((s) => s.id);

    for (const id of Array.from(this.scheduledStations.keys())) {
      if (!activeIds.has(id)) {
        logger.info({ stationId: id }, "Removing inactive station from scheduler queue");
        this.scheduledStations.delete(id);
        this.pollIntervals.delete(id);
      }
    }

    for (const station of stations) {
      const prevInterval = this.pollIntervals.get(station.id);
      const interval = this.normalizePollSeconds(station.pollIntervalSeconds);
      const existing = this.scheduledStations.get(station.id);
      if (!existing) {
        const idx = this.stationOrder.indexOf(station.id);
        const delayMs = this.initialSpreadDelayMs(interval, idx, this.stationOrder.length);
        this.scheduledStations.set(station.id, {
          id: station.id,
          name: station.name,
          intervalSeconds: interval,
          nextDueAt: Date.now() + delayMs,
        });
        this.pollIntervals.set(station.id, interval);
        logger.info({ station: station.name, interval, delayMs }, "Queued station poll schedule");
        continue;
      }

      existing.name = station.name;
      if (prevInterval !== undefined && prevInterval !== interval) {
        const idx = this.stationOrder.indexOf(station.id);
        const delayMs = this.initialSpreadDelayMs(interval, idx, this.stationOrder.length);
        logger.info({ stationId: station.id, prevInterval, interval }, "Poll interval changed; updating queue");
        existing.intervalSeconds = interval;
        existing.nextDueAt = Math.min(existing.nextDueAt, Date.now() + delayMs);
        this.pollIntervals.set(station.id, interval);
      }
    }
  }

  private static normalizePollSeconds(raw: unknown): number {
    const n = typeof raw === "number" ? raw : Number(raw);
    const v = Number.isFinite(n) ? Math.trunc(n) : 60;
    return Math.min(3600, Math.max(5, v || 60));
  }

  private static initialSpreadDelayMs(intervalSeconds: number, index: number, total: number): number {
    const intervalMs = intervalSeconds * 1000;
    const count = Math.max(1, total);
    const idx = Math.max(0, index);
    return Math.floor((idx / count) * intervalMs);
  }

  private static maxStationConcurrency(): number {
    return Math.max(1, Math.min(50, parseInt(process.env.MAX_STATION_CONCURRENCY || "8", 10) || 8));
  }

  private static async dispatchDuePolls() {
    const available = this.maxStationConcurrency() - this.pollRunning.size;
    if (available <= 0) return;

    const now = Date.now();
    const due = Array.from(this.scheduledStations.values())
      .filter((station) => station.nextDueAt <= now && !this.pollRunning.has(station.id))
      .sort((a, b) => a.nextDueAt - b.nextDueAt)
      .slice(0, available);

    for (const station of due) this.startPoll(station);
  }

  private static startPoll(station: ScheduledStation) {
    const runPoll = async () => {
      if (this.pollRunning.has(station.id)) {
        logger.debug({ stationId: station.id }, "Skipping overlapping poll tick");
        return;
      }
      this.pollRunning.add(station.id);
      station.nextDueAt = Date.now() + station.intervalSeconds * 1000;
      try {
        await MonitorService.pollStation(station.id);
      } finally {
        this.pollRunning.delete(station.id);
        void this.dispatchDuePolls();
      }
    };

    void runPoll();
  }

  static stopAll() {
    this.masterJob?.stop();
    if (this.dispatchTimer) {
      clearInterval(this.dispatchTimer);
      this.dispatchTimer = null;
    }
    this.unresolvedRecoveryJob?.stop();
    this.spinRefreshJob?.stop();
    this.catalogRepairJob?.stop();
    this.rematchJob?.stop();
    this.scheduledStations.clear();
    this.pollIntervals.clear();
    this.pollRunning.clear();
  }
}

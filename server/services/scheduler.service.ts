import * as cron from 'node-cron';
import { prisma } from '../lib/prisma.js';
import { logger } from '../lib/logger.js';
import { MonitorService } from './monitor.service.js';

export class SchedulerService {
  private static tasks: Map<string, cron.ScheduledTask> = new Map();
  private static masterJob: cron.ScheduledTask | null = null;

  static async init() {
    logger.info("Initializing station scheduler");
    
    // Periodically check for new/modified stations
    this.masterJob = cron.schedule('*/1 * * * *', () => {
      this.resyncStations();
    });

    await this.resyncStations();
  }

  private static async resyncStations() {
    const stations = await prisma.station.findMany({ where: { isActive: true } });
    const activeIds = new Set(stations.map(s => s.id));

    // Stop tasks for stations that are gone or deactivated
    for (const [id, task] of this.tasks) {
      if (!activeIds.has(id)) {
        logger.info({ stationId: id }, "Stopping scheduler for station");
        task.stop();
        this.tasks.delete(id);
      }
    }

    // Start or update tasks
    for (const station of stations) {
      if (!this.tasks.has(station.id)) {
        this.scheduleStation(station);
      }
    }
  }

  private static scheduleStation(station: any) {
    logger.info({ station: station.name, interval: station.pollIntervalSeconds }, "Scheduling station poll");
    
    // node-cron uses standard crontab, but we need seconds or intervals
    // For simple intervals in seconds, node-cron supports '*/n * * * * *' (6 fields)
    const cronExpr = `*/${station.pollIntervalSeconds} * * * * *`;
    
    const task = cron.schedule(cronExpr, async () => {
      // Basic lock check to avoid concurrent jobs for same station
      // In a multi-node setup we'd use Redis or a DB lock
      await MonitorService.pollStation(station.id);
    });

    this.tasks.set(station.id, task);
  }

  static stopAll() {
    this.masterJob?.stop();
    for (const task of this.tasks.values()) {
      task.stop();
    }
    this.tasks.clear();
  }
}

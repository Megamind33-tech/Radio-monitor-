#!/usr/bin/env node

/**
 * Intelligent ORB Backfill
 * Merges backfilled playlists with existing logged songs per station
 * Fills gaps when ICY metadata or fingerprints are missing
 */

import fetch from 'node-fetch';
import { JSDOM } from 'jsdom';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

class IntelligentBackfill {
  constructor() {
    this.baseUrl = 'https://onlineradiobox.com';
    this.delay = 1000;
  }

  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async fetchPage(url) {
    try {
      const response = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
        timeout: 15000
      });
      if (!response.ok) return null;
      return await response.text();
    } catch (error) {
      return null;
    }
  }

  parseTracks(html) {
    const tracks = [];

    try {
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      const trackHistoryItems = doc.querySelectorAll('td.track_history_item');

      trackHistoryItems.forEach((item, idx) => {
        const text = item.textContent.trim();
        if (text && text.includes(' - ')) {
          const [artist, ...titleParts] = text.split(' - ');
          tracks.push({
            artist: artist.trim(),
            title: titleParts.join(' - ').trim(),
            timestamp: new Date(Date.now() - (idx * 3600000)).toISOString(),
          });
        }
      });

      if (tracks.length === 0) {
        const topSongRows = doc.querySelectorAll('tr td a.ajax');
        topSongRows.forEach((link, idx) => {
          const alt = link.querySelector('img')?.getAttribute('alt') || '';
          if (alt && alt.includes(' - ')) {
            const [artist, title] = alt.split(' - ');
            tracks.push({
              artist: artist.trim(),
              title: title.trim(),
              timestamp: new Date(Date.now() - (idx * 3600000)).toISOString(),
            });
          }
        });
      }

      return tracks;
    } catch (error) {
      return [];
    }
  }

  /**
   * Check if station has gaps in logged songs
   * Returns true if station needs backfill
   */
  async needsBackfill(stationId) {
    // Count matched songs from ICY/fingerprint
    const matchedCount = await prisma.detectionLog.count({
      where: {
        stationId: stationId,
        status: 'matched',
        sourceProvider: { not: 'onlineradiobox.com' } // Not from backfill
      }
    });

    // Count unresolved (missing ICY/fingerprint)
    const unresolvedCount = await prisma.detectionLog.count({
      where: {
        stationId: stationId,
        status: 'unresolved'
      }
    });

    // If unresolvedCount is high or matchedCount is low, needs backfill
    const needsFill = unresolvedCount > matchedCount * 0.5;

    return {
      needsBackfill: needsFill,
      matched: matchedCount,
      unresolved: unresolvedCount
    };
  }

  /**
   * Intelligent merge: combine backfill with logged songs
   * Avoids duplicates, fills gaps strategically
   */
  async mergeWithLogged(stationId, backfillTracks) {
    // Get existing logged songs for this station
    const existingSongs = await prisma.stationSongSpin.findMany({
      where: { stationId: stationId },
      select: {
        artistNorm: true,
        titleNorm: true,
        playCount: true,
        firstPlayedAt: true,
        lastPlayedAt: true
      }
    });

    // Create map of existing songs
    const existingMap = new Map();
    existingSongs.forEach(song => {
      const key = `${song.artistNorm}|${song.titleNorm}`.toLowerCase();
      existingMap.set(key, song);
    });

    const toAdd = [];
    let duplicateCount = 0;
    let newCount = 0;

    // Check each backfilled track
    for (const track of backfillTracks) {
      const key = `${track.artist}|${track.title}`.toLowerCase();

      if (existingMap.has(key)) {
        // Song already logged - skip duplicate
        duplicateCount++;
        continue;
      }

      // New song from backfill - add it
      toAdd.push({
        stationId: stationId,
        artistNorm: track.artist,
        titleNorm: track.title,
        artistLast: track.artist,
        titleLast: track.title,
        playCount: 1,
        firstPlayedAt: new Date(track.timestamp),
        lastPlayedAt: new Date(track.timestamp)
      });
      newCount++;
    }

    // Bulk insert only new songs
    if (toAdd.length > 0) {
      try {
        await prisma.stationSongSpin.createMany({
          data: toAdd,
          skipDuplicates: true
        });
      } catch (error) {
        console.log(`  [!] Error inserting songs: ${error.message}`);
      }
    }

    return {
      added: newCount,
      duplicates: duplicateCount,
      total: backfillTracks.length
    };
  }

  async backfillStation(stationId, stationName) {
    if (!stationId.includes('orb')) return null;

    // Check if station needs backfill
    const needsCheck = await this.needsBackfill(stationId);

    if (!needsCheck.needsBackfill) {
      console.log(`  [✓] ${stationName} - Already has sufficient logged songs (${needsCheck.matched} matched, ${needsCheck.unresolved} unresolved)`);
      return null;
    }

    console.log(`  [*] ${stationName} - Backfilling... (${needsCheck.matched} matched, ${needsCheck.unresolved} unresolved)`);

    // Get station URL
    const parts = stationId.split('.');
    const slug = parts[parts.length - 1];
    const url = `${this.baseUrl}/zm/${slug}/`;

    const html = await this.fetchPage(url);
    if (!html) {
      console.log(`  [-] Failed to fetch page`);
      return null;
    }

    const backfillTracks = this.parseTracks(html);
    if (backfillTracks.length === 0) {
      console.log(`  [-] No tracks found on page`);
      return null;
    }

    // Intelligently merge with existing songs
    const result = await this.mergeWithLogged(stationId, backfillTracks);

    console.log(`  [✓] Added ${result.added}/${result.total} tracks (${result.duplicates} already logged)`);

    // Also save as detection logs for tracking
    for (const track of backfillTracks.slice(0, result.added)) {
      try {
        await prisma.detectionLog.create({
          data: {
            stationId: stationId,
            observedAt: new Date(track.timestamp),
            detectionMethod: 'orb_intelligent_backfill',
            parsedArtist: track.artist,
            parsedTitle: track.title,
            status: 'matched',
            sourceProvider: 'onlineradiobox.com',
            confidence: 0.80
          }
        });
      } catch (error) {
        // Duplicate, skip
      }
    }

    await this.sleep(this.delay);
    return result;
  }

  async runIntelligentBackfill() {
    const startTime = new Date();
    console.log(`\n[*] Starting Intelligent Backfill at ${startTime.toISOString()}\n`);

    try {
      const stations = await prisma.station.findMany({
        where: {
          country: 'Zambia',
          isActive: true
        },
        select: { id: true, name: true },
        orderBy: { name: 'asc' }
      });

      let totalAdded = 0;
      let stationsBackfilled = 0;

      for (const station of stations) {
        const result = await this.backfillStation(station.id, station.name);
        if (result) {
          totalAdded += result.added;
          stationsBackfilled++;
        }
      }

      const endTime = new Date();
      const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);

      console.log(`\n[✓] Intelligent Backfill Completed!`);
      console.log(`    Stations backfilled: ${stationsBackfilled}/${stations.length}`);
      console.log(`    Total songs added: ${totalAdded}`);
      console.log(`    Duration: ${duration} minutes\n`);

      return { stationsBackfilled, totalAdded };

    } catch (error) {
      console.error('[!] Backfill error:', error.message);
      return { stationsBackfilled: 0, totalAdded: 0 };
    } finally {
      await prisma.$disconnect();
    }
  }
}

async function main() {
  const backfill = new IntelligentBackfill();
  await backfill.runIntelligentBackfill();
}

main();

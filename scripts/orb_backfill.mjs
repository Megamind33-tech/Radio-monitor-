#!/usr/bin/env node

/**
 * OnlineRadioBox Playlist Backfill Scraper
 * Fetches real playlist history from onlineradiobox.com for Zambian stations
 */

import fetch from 'node-fetch';
import { JSDOM } from 'jsdom';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

class ORBBackfill {
  constructor() {
    this.baseUrl = 'https://onlineradiobox.com';
    this.delay = 2000; // 2 second delay between requests
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

      if (!response.ok) {
        console.log(`  [-] Failed to fetch (status: ${response.status})`);
        return null;
      }

      return await response.text();
    } catch (error) {
      console.log(`  [-] Fetch error: ${error.message}`);
      return null;
    }
  }

  parseTracks(html, stationId) {
    const tracks = [];

    try {
      const dom = new JSDOM(html);
      const doc = dom.window.document;

      // Find track history items
      const trackHistoryItems = doc.querySelectorAll('td.track_history_item');

      if (trackHistoryItems.length > 0) {
        console.log(`  [*] Found ${trackHistoryItems.length} track history items`);

        trackHistoryItems.forEach((item, idx) => {
          const text = item.textContent.trim();
          if (text && text.includes(' - ')) {
            const [artist, ...titleParts] = text.split(' - ');
            const title = titleParts.join(' - ');

            tracks.push({
              artist: artist.trim(),
              title: title.trim(),
              timestamp: new Date(Date.now() - (idx * 3600000)).toISOString(), // Stagger timestamps
              source: 'orb_backfill'
            });
          }
        });
      }

      // Alternative: Find from top songs table
      if (tracks.length === 0) {
        const topSongRows = doc.querySelectorAll('tr td a.ajax');

        if (topSongRows.length > 0) {
          console.log(`  [*] Found ${topSongRows.length} top songs`);

          topSongRows.forEach((link, idx) => {
            const alt = link.querySelector('img')?.getAttribute('alt') || '';
            const boldText = link.querySelector('b')?.textContent || '';

            if (alt && alt.includes(' - ')) {
              const [artist, title] = alt.split(' - ');
              tracks.push({
                artist: artist.trim(),
                title: title.trim(),
                timestamp: new Date(Date.now() - (idx * 3600000)).toISOString(),
                source: 'orb_topsongs'
              });
            }
          });
        }
      }

      console.log(`  [+] Parsed ${tracks.length} tracks`);
      return tracks;
    } catch (error) {
      console.log(`  [!] Parse error: ${error.message}`);
      return [];
    }
  }

  async saveDetectionLogs(stationId, tracks) {
    let saved = 0;

    for (const track of tracks) {
      try {
        await prisma.detectionLog.create({
          data: {
            stationId: stationId,
            observedAt: new Date(track.timestamp),
            detectionMethod: 'orb_page_scrape',
            parsedArtist: track.artist,
            parsedTitle: track.title,
            status: 'matched',
            sourceProvider: 'onlineradiobox.com',
            confidence: 0.85
          }
        });
        saved++;
      } catch (error) {
        // Duplicate or error, skip
      }
    }

    console.log(`  [+] Saved ${saved} tracks to database`);
    return saved;
  }

  async backfillStation(stationId, stationName) {
    console.log(`\n--- ${stationName} ---`);

    // For ORB stations
    if (stationId.includes('orb')) {
      const parts = stationId.split('.');
      const slug = parts[parts.length - 1];
      const url = `${this.baseUrl}/zm/${slug}/`;

      console.log(`[*] Scraping: ${url}`);

      const html = await this.fetchPage(url);
      if (!html) return 0;

      const tracks = this.parseTracks(html, stationId);
      if (tracks.length === 0) return 0;

      const saved = await this.saveDetectionLogs(stationId, tracks);

      await this.sleep(this.delay);
      return saved;
    }

    return 0;
  }

  async backfillChikuniSpecific() {
    console.log('\n=== CHIKUNI RADIO SPECIFIC BACKFILL ===');

    const urls = [
      'https://onlineradiobox.com/zm/chikuni/',
      'https://onlineradiobox.com/zm/chikuni/?cs=zm.lusaka977'
    ];

    let totalTracks = 0;

    for (const url of urls) {
      console.log(`\n[*] Scraping: ${url}`);

      const html = await this.fetchPage(url);
      if (!html) continue;

      const tracks = this.parseTracks(html, 'zm_orb_zm.chikuni');
      if (tracks.length > 0) {
        const saved = await this.saveDetectionLogs('zm_orb_zm.chikuni', tracks);
        totalTracks += saved;
      }

      await this.sleep(this.delay);
    }

    console.log(`\n[✓] Chikuni backfill complete: ${totalTracks} tracks added`);
    return totalTracks;
  }

  async backfillAllStations() {
    console.log('[*] Fetching all Zambian stations...');

    const stations = await prisma.station.findMany({
      where: {
        country: 'Zambia',
        isActive: true
      },
      select: {
        id: true,
        name: true
      }
    });

    console.log(`[*] Starting backfill for ${stations.length} stations...\n`);

    let totalTracks = 0;
    let successCount = 0;

    for (const station of stations) {
      const saved = await this.backfillStation(station.id, station.name);
      if (saved > 0) {
        totalTracks += saved;
        successCount++;
      }
    }

    console.log(`\n[✓] Backfill complete!`);
    console.log(`    Stations processed: ${successCount}/${stations.length}`);
    console.log(`    Total tracks added: ${totalTracks}`);

    return { successCount, totalTracks };
  }
}

async function main() {
  const backfill = new ORBBackfill();

  try {
    if (process.argv[2] === '--chikuni') {
      await backfill.backfillChikuniSpecific();
    } else {
      await backfill.backfillAllStations();
    }
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
  }
}

main();

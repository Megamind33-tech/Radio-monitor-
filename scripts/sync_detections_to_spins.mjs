#!/usr/bin/env node

/**
 * Sync DetectionLogs to StationSongSpin
 * Converts all matched detections into logged songs
 * This makes backfill data visible in the UI
 */

import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function syncDetectionsToSpins() {
  console.log('[*] Syncing detection logs to logged songs...\n');

  try {
    // Get all matched detections grouped by station and song
    const detections = await prisma.detectionLog.findMany({
      where: {
        status: 'matched',
        parsedTitle: { not: '' },
        parsedArtist: { not: '' }
      },
      select: {
        stationId: true,
        parsedArtist: true,
        parsedTitle: true,
        observedAt: true
      },
      orderBy: {
        observedAt: 'desc'
      }
    });

    console.log(`[*] Found ${detections.length} matched detections\n`);

    // Group by station and song
    const songMap = new Map();

    detections.forEach(det => {
      const key = `${det.stationId}|${det.parsedArtist}|${det.parsedTitle}`;

      if (!songMap.has(key)) {
        songMap.set(key, {
          stationId: det.stationId,
          artist: det.parsedArtist,
          title: det.parsedTitle,
          plays: 0,
          firstPlayed: det.observedAt,
          lastPlayed: det.observedAt
        });
      }

      const song = songMap.get(key);
      song.plays++;
      song.lastPlayed = det.observedAt;
    });

    console.log(`[*] Processing ${songMap.size} unique songs...\n`);

    // Insert into StationSongSpin
    let created = 0;
    let skipped = 0;

    for (const [key, song] of songMap) {
      try {
        // Check if song already exists
        const existing = await prisma.stationSongSpin.findFirst({
          where: {
            stationId: song.stationId,
            artistNorm: song.artist,
            titleNorm: song.title
          }
        });

        if (existing) {
          // Update play count
          await prisma.stationSongSpin.update({
            where: { id: existing.id },
            data: {
              playCount: existing.playCount + song.plays,
              lastPlayedAt: song.lastPlayed
            }
          });
          skipped++;
        } else {
          // Create new song spin
          await prisma.stationSongSpin.create({
            data: {
              stationId: song.stationId,
              artistNorm: song.artist,
              titleNorm: song.title,
              artistLast: song.artist,
              titleLast: song.title,
              playCount: song.plays,
              firstPlayedAt: song.firstPlayed,
              lastPlayedAt: song.lastPlayed
            }
          });
          created++;
        }
      } catch (error) {
        // Skip errors
      }
    }

    console.log(`[✓] Sync Complete!`);
    console.log(`    Created: ${created} new logged songs`);
    console.log(`    Updated: ${skipped} existing songs\n`);

    // Show results by station
    const stations = await prisma.station.findMany({
      where: { country: 'Zambia', isActive: true }
    });

    console.log('[*] Logged Songs Summary:\n');

    for (const station of stations.sort((a, b) => a.name.localeCompare(b.name))) {
      const songCount = await prisma.stationSongSpin.count({
        where: { stationId: station.id }
      });

      const totalPlays = await prisma.stationSongSpin.aggregate({
        where: { stationId: station.id },
        _sum: { playCount: true }
      });

      if (songCount > 0) {
        console.log(`${station.name.padEnd(40)} | ${songCount.toString().padStart(3)} songs | ${(totalPlays._sum.playCount || 0).toString().padStart(4)} plays`);
      }
    }

  } catch (error) {
    console.error('[!] Error:', error.message);
  } finally {
    await prisma.$disconnect();
  }
}

syncDetectionsToSpins();

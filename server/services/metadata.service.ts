import { spawn } from 'child_process';
import { logger } from '../lib/logger.js';
import { NormalizedMetadata } from '../types.js';

export class MetadataService {
  /**
   * Attempts to read ICY metadata from a stream using ffprobe.
   */
  static async readStreamMetadata(url: string): Promise<NormalizedMetadata | null> {
    logger.debug({ url }, "Reading stream metadata via ffprobe");

    return new Promise((resolve) => {
      // ffprobe -v quiet -show_format -show_streams -print_format json -i <url>
      // For ICY metadata, we often need to wait a few seconds or use specific flags
      const ffprobe = spawn('ffprobe', [
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-i', url
      ]);

      let stdout = '';
      const timeout = setTimeout(() => {
        ffprobe.kill();
        logger.warn({ url }, "ffprobe metadata read timed out");
        resolve(null);
      }, 10000);

      ffprobe.stdout.on('data', (data) => {
        stdout += data;
      });

      ffprobe.on('close', (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          logger.debug({ url, code }, "ffprobe exited with non-zero code");
          resolve(null);
          return;
        }

        try {
          const data = JSON.parse(stdout);
          const tags = data.format?.tags;
          
          if (!tags) {
            resolve(null);
            return;
          }

          // ICY/Shoutcast metadata usually comes in StreamTitle
          const streamTitle = tags.StreamTitle || tags.title;
          if (!streamTitle) {
            resolve(null);
            return;
          }

          const normalized = this.parseStreamTitle(streamTitle);
          resolve(normalized);
        } catch (error) {
          logger.error({ error, stdout }, "Failed to parse ffprobe output");
          resolve(null);
        }
      });
    });
  }

  private static parseStreamTitle(title: string): NormalizedMetadata {
    // Common formats: "Artist - Title", "Artist / Title", "Title - Artist"
    // We'll keep it simple for now as requested
    let artist = '';
    let song = '';

    const separators = [' - ', ' / ', ' – '];
    for (const sep of separators) {
      if (title.includes(sep)) {
        const parts = title.split(sep);
        artist = parts[0].trim();
        song = parts.slice(1).join(sep).trim();
        break;
      }
    }

    if (!artist && !song) {
      song = title.trim();
    }

    return {
      rawArtist: artist,
      rawTitle: song,
      combinedRaw: title,
      sourceType: 'stream_metadata'
    };
  }

  static isMetadataTrustworthy(metadata: NormalizedMetadata, lastMetadata?: string): { trusted: boolean, reason?: string } {
    if (!metadata.combinedRaw) return { trusted: false, reason: 'empty' };

    const text = metadata.combinedRaw.toLowerCase();
    
    // Station branding patterns
    const brandingPatterns = [
      'radio', 'fm', 'playing now', 'tune in', 'live', 'on air', 
      'streaming', 'advertising', 'news', 'weather'
    ];

    if (brandingPatterns.some(p => text.includes(p) && text.length < 30)) {
      return { trusted: false, reason: 'branding' };
    }

    if (lastMetadata === metadata.combinedRaw) {
      // This logic should be handled by the caller with a timestamp check
      // But we can flag it here as "potentially stale"
      return { trusted: true }; // Caller will decide based on time
    }

    return { trusted: true };
  }
}

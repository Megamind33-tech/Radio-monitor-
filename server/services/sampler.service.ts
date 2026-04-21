import { spawn } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import { logger } from '../lib/logger.js';

export class SamplerService {
  /**
   * Captures a short audio clip from a stream and saves it to a temp file.
   */
  static async captureSample(url: string, durationSeconds: number = 20): Promise<string | null> {
    const tempDir = process.env.TEMP_AUDIO_DIR || '/tmp/radio_monitor';
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }

    const filename = `sample_${Date.now()}_${Math.random().toString(36).substring(7)}.wav`;
    const outputPath = path.join(tempDir, filename);

    logger.info({ url, durationSeconds, outputPath }, "Capturing audio sample");

    return new Promise((resolve) => {
      // ffmpeg -i <url> -t <duration> -vn -ac 1 -ar 11025 -f wav <output>
      // We use lower sample rate for fingerprinting (Chromaprint usually uses 11025 or 44100)
      const ffmpeg = spawn('ffmpeg', [
        '-i', url,
        '-t', durationSeconds.toString(),
        '-vn',              // No video
        '-ac', '1',         // Mono
        '-ar', '11025',     // Sample rate
        '-f', 'wav',
        '-y',               // Overwrite
        outputPath
      ]);

      const timeout = setTimeout(() => {
        ffmpeg.kill();
        logger.warn({ url }, "ffmpeg sample capture timed out");
        if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
        resolve(null);
      }, (durationSeconds + 10) * 1000);

      ffmpeg.on('close', (code) => {
        clearTimeout(timeout);
        if (code === 0 && fs.existsSync(outputPath) && fs.statSync(outputPath).size > 1000) {
          logger.info({ outputPath }, "Sample captured successfully");
          resolve(outputPath);
        } else {
          logger.warn({ code }, "ffmpeg failed to capture sample");
          if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
          resolve(null);
        }
      });
    });
  }

  static cleanup(filePath: string) {
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }
    } catch (error) {
      logger.error({ error, filePath }, "Failed to delete temp audio file");
    }
  }
}

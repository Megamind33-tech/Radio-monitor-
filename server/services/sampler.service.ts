import { spawn } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import { logger } from '../lib/logger.js';
import { fingerprintPipelineGate } from '../lib/fingerprint-pipeline-gate.js';

export class SamplerService {
  /**
   * Captures an audio clip from a stream for Chromaprint/AcoustID fingerprinting.
   *
   * AcoustID uses up to the first 120 seconds of a track for optimal identification.
   * Pass durationSeconds=120 (or set FINGERPRINT_SAMPLE_SECONDS=120) for best results.
   * Shorter clips (≥30 s) work for local fingerprint cache hits but may miss AcoustID matches.
   *
   * Each capture acquires a slot in the global FingerprintPipelineGate (max 2 concurrent,
   * max 2 per second) so that simultaneous station polls never flood AcoustID or overload
   * the host CPU/network.
   *
   * @param delaySeconds Optional wait before opening the stream (lets encoder advance for retry windows).
   */
  static async captureSample(
    url: string,
    durationSeconds: number = 20,
    delaySeconds: number = 0
  ): Promise<string | null> {
    const tempDir = process.env.TEMP_AUDIO_DIR || '/tmp/radio_monitor';
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir, { recursive: true });
    }

    // Rate-gate: max 2 concurrent captures, max 2 starts per second (see fingerprint-pipeline-gate.ts).
    const release = await fingerprintPipelineGate.acquire();

    const delayMs = Math.min(30_000, Math.max(0, Math.round((delaySeconds || 0) * 1000)));
    if (delayMs > 0) {
      await new Promise<void>((r) => setTimeout(r, delayMs));
    }

    const filename = `sample_${Date.now()}_${Math.random().toString(36).substring(7)}.wav`;
    const outputPath = path.join(tempDir, filename);

    logger.info({ url, durationSeconds, delaySeconds: delayMs / 1000, outputPath, pipelineStatus: fingerprintPipelineGate.getStatus() }, "Capturing audio sample");

    return new Promise((resolve) => {
      let settled = false;
      const done = (result: string | null) => {
        if (settled) return;
        settled = true;
        resolve(result);
      };

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
        release();
        done(null);
      }, (durationSeconds + 10) * 1000);

      ffmpeg.on('error', (error) => {
        clearTimeout(timeout);
        logger.warn({ error, url }, "ffmpeg failed to start");
        if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
        release();
        done(null);
      });

      ffmpeg.on('close', (code) => {
        clearTimeout(timeout);
        release();
        if (code === 0 && fs.existsSync(outputPath) && fs.statSync(outputPath).size > 1000) {
          logger.info({ outputPath }, "Sample captured successfully");
          done(outputPath);
        } else {
          logger.warn({ code }, "ffmpeg failed to capture sample");
          if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
          done(null);
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

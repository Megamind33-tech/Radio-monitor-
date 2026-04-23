import { spawn } from 'child_process';
import { logger } from '../lib/logger.js';
import { FingerprintResult } from '../types.js';

export class FingerprintService {
  private static async generateWithFpcalc(filePath: string): Promise<FingerprintResult | null> {
    return new Promise((resolve) => {
      let settled = false;
      const done = (result: FingerprintResult | null) => {
        if (settled) return;
        settled = true;
        resolve(result);
      };

      const fpcalc = spawn('fpcalc', ['-json', filePath]);
      let stdout = '';
      const timeout = setTimeout(() => {
        fpcalc.kill();
        logger.warn({ filePath }, "fpcalc timed out");
        done(null);
      }, 15000);

      fpcalc.stdout.on('data', (data) => {
        stdout += data;
      });

      fpcalc.on('error', (error) => {
        clearTimeout(timeout);
        logger.warn({ error, filePath }, "fpcalc failed to start");
        done(null);
      });

      fpcalc.on('close', (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          logger.warn({ code, filePath }, "fpcalc exited with error");
          done(null);
          return;
        }
        try {
          const data = JSON.parse(stdout);
          if (!data.fingerprint || !data.duration) {
            done(null);
            return;
          }
          done({
            duration: Math.round(data.duration),
            fingerprint: data.fingerprint,
            backendUsed: 'chromaprint_fpcalc'
          });
        } catch (error) {
          logger.error({ error, stdout }, "Failed to parse fpcalc output");
          done(null);
        }
      });
    });
  }

  /**
   * Secondary free fingerprint backend:
   * ffmpeg chromaprint muxer (when fpcalc is unavailable).
   */
  private static async generateWithFfmpegChromaprint(filePath: string): Promise<FingerprintResult | null> {
    logger.debug({ filePath }, "Generating fingerprint via ffmpeg chromaprint");
    return new Promise((resolve) => {
      let settled = false;
      const done = (result: FingerprintResult | null) => {
        if (settled) return;
        settled = true;
        resolve(result);
      };

      const ffmpeg = spawn('ffmpeg', [
        '-v', 'error',
        '-i', filePath,
        '-f', 'chromaprint',
        '-fp_format', 'base64',
        '-'
      ]);

      const chunks: Buffer[] = [];
      let stderr = '';
      const timeout = setTimeout(() => {
        ffmpeg.kill();
        logger.warn({ filePath }, "ffmpeg chromaprint timed out");
        done(null);
      }, 20000);

      ffmpeg.stdout.on('data', (data) => {
        chunks.push(Buffer.from(data));
      });
      ffmpeg.stderr.on('data', (data) => {
        stderr += String(data);
      });
      ffmpeg.on('error', (error) => {
        clearTimeout(timeout);
        logger.warn({ error, filePath }, "ffmpeg chromaprint failed to start");
        done(null);
      });
      ffmpeg.on('close', async (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          logger.warn({ code, filePath, stderr }, "ffmpeg chromaprint exited with error");
          done(null);
          return;
        }
        const out = Buffer.concat(chunks);
        if (!out.length) {
          done(null);
          return;
        }
        const fp = out.toString('utf8').trim();
        if (!fp) {
          done(null);
          return;
        }
        // Get duration from ffprobe to keep AcoustID request valid.
        const ffprobe = spawn('ffprobe', [
          '-v', 'quiet',
          '-print_format', 'json',
          '-show_format',
          '-i', filePath
        ]);
        let meta = '';
        const probeTimeout = setTimeout(() => {
          ffprobe.kill();
          done(null);
        }, 8000);
        ffprobe.stdout.on('data', (data) => {
          meta += String(data);
        });
        ffprobe.on('close', () => {
          clearTimeout(probeTimeout);
          try {
            const data = JSON.parse(meta);
            const dur = Number(data?.format?.duration);
            if (!Number.isFinite(dur) || dur <= 0) {
              done(null);
              return;
            }
            done({
              duration: Math.round(dur),
              fingerprint: fp,
              backendUsed: 'chromaprint_ffmpeg'
            });
          } catch {
            done(null);
          }
        });
        ffprobe.on('error', () => {
          clearTimeout(probeTimeout);
          done(null);
        });
      });
    });
  }

  /**
   * Generates a Chromaprint fingerprint using fpcalc.
   */
  static async generateFingerprint(filePath: string): Promise<FingerprintResult | null> {
    logger.debug({ filePath }, "Generating fingerprint via fpcalc");
    const primary = await this.generateWithFpcalc(filePath);
    if (primary) return primary;

    logger.warn({ filePath }, "Falling back to ffmpeg chromaprint backend");
    return this.generateWithFfmpegChromaprint(filePath);
  }
}

import { spawn } from 'child_process';
import { logger } from '../lib/logger.js';
import { FingerprintResult } from '../types.js';

export class FingerprintService {
  /**
   * Generates a Chromaprint fingerprint using fpcalc.
   */
  static async generateFingerprint(filePath: string): Promise<FingerprintResult | null> {
    logger.debug({ filePath }, "Generating fingerprint via fpcalc");

    return new Promise((resolve) => {
      let settled = false;
      const done = (result: FingerprintResult | null) => {
        if (settled) return;
        settled = true;
        resolve(result);
      };

      // fpcalc -json <filePath>
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

      // Important: when fpcalc is missing (ENOENT), Node emits "error" and would
      // otherwise crash the process if not handled.
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
}

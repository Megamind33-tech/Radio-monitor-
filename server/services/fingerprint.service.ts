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
      // fpcalc -json <filePath>
      const fpcalc = spawn('fpcalc', ['-json', filePath]);

      let stdout = '';
      const timeout = setTimeout(() => {
        fpcalc.kill();
        logger.warn({ filePath }, "fpcalc timed out");
        resolve(null);
      }, 15000);

      fpcalc.stdout.on('data', (data) => {
        stdout += data;
      });

      fpcalc.on('close', (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          logger.warn({ code, filePath }, "fpcalc exited with error");
          resolve(null);
          return;
        }

        try {
          const data = JSON.parse(stdout);
          if (!data.fingerprint || !data.duration) {
            resolve(null);
            return;
          }

          resolve({
            duration: Math.round(data.duration),
            fingerprint: data.fingerprint,
            backendUsed: 'chromaprint_fpcalc'
          });
        } catch (error) {
          logger.error({ error, stdout }, "Failed to parse fpcalc output");
          resolve(null);
        }
      });
    });
  }
}

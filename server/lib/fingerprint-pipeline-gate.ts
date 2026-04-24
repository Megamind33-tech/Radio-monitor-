/**
 * Global rate gate for the fingerprint pipeline.
 *
 * Rules enforced (matching AcoustID's published 3 req/s limit with safety margin):
 *   - MAX_CONCURRENT = 2  — at most 2 capture+lookup operations run simultaneously
 *   - MIN_GAP_MS     = 500 — new operations start at most every 500 ms (2/second)
 *
 * Applied to the recording stage (audio capture) so that even with dozens of
 * active stations the system never floods AcoustID or overloads the host CPU.
 * The AcoustID service has its own identical 500 ms per-call throttle; this gate
 * is the upstream chokepoint that shapes the entire capture→fingerprint→lookup chain.
 */

function parseEnvInt(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : fallback;
}

/** Default 2 concurrent captures; raise cautiously (CPU/IO). */
const MAX_CONCURRENT = Math.min(8, Math.max(1, parseEnvInt("FINGERPRINT_PIPELINE_MAX_CONCURRENT", 2)));
/** Gap between starts: default 750ms ≈ 1.33/s so AcoustID (500ms throttle) has headroom across stations. */
const MIN_GAP_MS = Math.min(5000, Math.max(200, parseEnvInt("FINGERPRINT_PIPELINE_MIN_GAP_MS", 750)));

class FingerprintPipelineGate {
  private active = 0;
  private lastStartMs = 0;

  /**
   * Acquire a slot in the pipeline. Resolves when it is safe to start a new
   * fingerprint capture. The caller MUST call the returned release function
   * when the operation completes (match or no-match).
   */
  async acquire(): Promise<() => void> {
    for (;;) {
      const now = Date.now();
      const gapRemaining = this.lastStartMs + MIN_GAP_MS - now;
      if (this.active < MAX_CONCURRENT && gapRemaining <= 0) {
        this.active++;
        this.lastStartMs = Date.now();
        let released = false;
        return () => {
          if (!released) {
            released = true;
            this.active = Math.max(0, this.active - 1);
          }
        };
      }
      // Wait until the gap clears or a concurrent slot opens.
      const waitMs = Math.max(50, gapRemaining > 0 ? gapRemaining : 50);
      await new Promise<void>((r) => setTimeout(r, waitMs));
    }
  }

  /** Snapshot for health / metrics endpoints. */
  getStatus() {
    return {
      active: this.active,
      maxConcurrent: MAX_CONCURRENT,
      minGapMs: MIN_GAP_MS,
    };
  }
}

export const fingerprintPipelineGate = new FingerprintPipelineGate();

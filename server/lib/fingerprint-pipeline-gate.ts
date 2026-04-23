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

const MAX_CONCURRENT = 2;
const MIN_GAP_MS = 500; // 2 starts per second

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

import axios from "axios";
import fs from "fs";
import FormData from "form-data";
import { logger } from "../lib/logger.js";
import type { MatchResult } from "../types.js";

const DEFAULT_BASE = "https://api.audd.io/";

/** AudD standard API recommends ≤20s / <1MB; hard cap 10MB / 25s per their errors list. */
const MAX_FILE_BYTES = 9 * 1024 * 1024;

export class AuddService {
  private static lastRequestAt = 0;
  private static readonly RATE_LIMIT_MS = 350;

  private static async throttle(): Promise<void> {
    const now = Date.now();
    const wait = this.lastRequestAt + this.RATE_LIMIT_MS - now;
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    this.lastRequestAt = Date.now();
  }

  private static baseUrl(): string {
    const u = (process.env.AUDD_API_URL || DEFAULT_BASE).trim();
    return u.endsWith("/") ? u : `${u}/`;
  }

  private static token(): string | null {
    const t = (process.env.AUDD_API_TOKEN || process.env.AUDD_TOKEN || "").trim();
    return t || null;
  }

  /**
   * Recognize music from a local audio file (e.g. WAV from ffmpeg sampler).
   * Uses multipart `file` upload per https://docs.audd.io/
   */
  static async recognizeFile(filePath: string): Promise<MatchResult | null> {
    const token = this.token();
    if (!token) {
      logger.debug("AudD: no AUDD_API_TOKEN configured");
      return null;
    }
    if (!filePath || !fs.existsSync(filePath)) return null;

    let stat: fs.Stats;
    try {
      stat = fs.statSync(filePath);
    } catch {
      return null;
    }
    if (stat.size < 512) {
      logger.debug({ filePath, size: stat.size }, "AudD: file too small");
      return null;
    }
    if (stat.size > MAX_FILE_BYTES) {
      logger.warn({ filePath, size: stat.size }, "AudD: file over cap, skipping");
      return null;
    }

    await this.throttle();

    const form = new FormData();
    form.append("api_token", token);
    form.append("return", "musicbrainz,isrc");
    form.append("file", fs.createReadStream(filePath));

    try {
      logger.info({ filePath, bytes: stat.size }, "Querying AudD API");
      const res = await axios.post<unknown>(this.baseUrl(), form, {
        headers: form.getHeaders(),
        timeout: Math.min(45_000, parseInt(process.env.AUDD_TIMEOUT_MS || "28000", 10) || 28_000),
        maxContentLength: MAX_FILE_BYTES + 256_000,
        maxBodyLength: MAX_FILE_BYTES + 256_000,
      });

      const data = res.data as Record<string, unknown>;
      if (data?.status !== "success") {
        const err = (data?.error as { code?: number; message?: string }) || {};
        logger.debug({ status: data?.status, err }, "AudD: non-success response");
        return null;
      }

      const result = data.result as Record<string, unknown> | null | undefined;
      if (!result || typeof result !== "object") {
        logger.debug("AudD: empty result");
        return null;
      }

      const artist = String(result.artist ?? "").trim();
      const title = String(result.title ?? "").trim();
      if (!title && !artist) return null;

      const album = String(result.album ?? "").trim() || undefined;
      const releaseDate = String(result.release_date ?? "").trim() || undefined;
      const label = String(result.label ?? "").trim() || undefined;

      let recordingId: string | undefined;
      const mb = result.musicbrainz as Record<string, unknown> | undefined;
      if (mb && typeof mb === "object") {
        const rec = mb.recordings as unknown;
        if (Array.isArray(rec) && rec[0] && typeof rec[0] === "object") {
          const id = (rec[0] as { id?: string }).id;
          if (typeof id === "string" && id.length > 10) recordingId = id;
        }
        if (!recordingId && typeof mb.id === "string" && mb.id.length > 10) {
          recordingId = mb.id;
        }
      }

      const isrcRaw = result.isrc as unknown;
      let isrcs: string[] | undefined;
      if (typeof isrcRaw === "string" && isrcRaw.trim()) {
        isrcs = [isrcRaw.trim()];
      } else if (Array.isArray(isrcRaw)) {
        isrcs = isrcRaw.filter((x): x is string => typeof x === "string" && x.trim().length > 0);
      }

      const score = 0.88;
      const match: MatchResult = {
        score,
        confidence: score,
        title,
        artist: artist || undefined,
        releaseTitle: album,
        releaseDate,
        genre: label,
        recordingId,
        sourceProvider: "audd",
        reasonCode: "fingerprint_audd",
        isrcs: isrcs?.length ? isrcs : undefined,
      };

      return match;
    } catch (error) {
      logger.warn({ error, filePath }, "AudD API request failed");
      return null;
    }
  }
}

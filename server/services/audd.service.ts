import axios from "axios";
import fs from "fs";
import FormData from "form-data";
import { logger } from "../lib/logger.js";
import { MatchResult } from "../types.js";

type AuddTrack = {
  title?: string;
  artist?: string;
  album?: string;
  release_date?: string;
  timecode?: string;
  duration?: number;
  score?: number;
  isrc?: string;
  label?: string;
};

type AuddResponse = {
  status?: string;
  result?: AuddTrack | null;
  error?: { error_code?: number; error_message?: string };
};

function parseEnvBool(key: string, fallback = false): boolean {
  const v = process.env[key];
  if (!v) return fallback;
  const t = String(v).trim().toLowerCase();
  return t === "1" || t === "true" || t === "yes" || t === "on";
}

function parseEnvFloat(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

export class AuddService {
  static isEnabled(): boolean {
    return !!process.env.AUDD_API_TOKEN || parseEnvBool("AUDD_ALLOW_TOKENLESS", false);
  }

  static async lookupSample(filePath: string): Promise<MatchResult | null> {
    const token = process.env.AUDD_API_TOKEN || "";
    const tokenless = parseEnvBool("AUDD_ALLOW_TOKENLESS", false);
    if (!token && !tokenless) {
      return null;
    }
    if (!fs.existsSync(filePath)) {
      return null;
    }

    const minScore = parseEnvFloat("AUDD_MIN_SCORE", 0.55);
    const form = new FormData();
    if (token) form.append("api_token", token);
    form.append("return", "spotify,apple_music,deezer");
    form.append("file", fs.createReadStream(filePath));

    try {
      const response = await axios.post<AuddResponse>("https://api.audd.io/", form, {
        headers: form.getHeaders(),
        timeout: 25000,
        maxBodyLength: Infinity,
      });
      const payload = response.data;
      if (!payload || payload.status !== "success" || !payload.result) {
        logger.debug({ payload }, "AudD returned no result");
        return null;
      }

      const row = payload.result;
      const rawScore = Number(row.score ?? 0);
      const normalizedScore = rawScore > 1 ? Math.min(1, rawScore / 100) : Math.max(0, rawScore);
      const confidence = normalizedScore || 0.7;
      if (confidence < minScore) {
        logger.debug({ confidence, minScore, title: row.title, artist: row.artist }, "AudD match below threshold");
        return null;
      }

      return {
        score: confidence,
        confidence,
        title: row.title,
        artist: row.artist,
        releaseTitle: row.album,
        releaseDate: row.release_date,
        isrcs: row.isrc ? [row.isrc] : undefined,
        durationMs: typeof row.duration === "number" && row.duration > 0 ? row.duration * 1000 : undefined,
        sourceProvider: "audd",
        reasonCode: "fingerprint_audd",
      };
    } catch (error) {
      logger.warn({ error, filePath }, "AudD lookup failed");
      return null;
    }
  }

  static async lookupFromFingerprint(filePath: string): Promise<MatchResult | null> {
    return this.lookupSample(filePath);
  }
}

import crypto from "node:crypto";
import fs from "fs";
import FormData from "form-data";
import axios from "axios";
import { logger } from "../lib/logger.js";
import { MatchResult } from "../types.js";

function parseEnvFloat(key: string, fallback: number): number {
  const v = process.env[key];
  if (!v) return fallback;
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : fallback;
}

/**
 * ACRCloud Identify Protocol v1 (audio upload).
 * https://docs.acrcloud.com/reference/identification-api/identification-api.md
 *
 * Set ACRCLOUD_HOST (e.g. identify-eu-west-1.acrcloud.com), ACRCLOUD_ACCESS_KEY,
 * ACRCLOUD_ACCESS_SECRET. Optional ACRCLOUD_MIN_SCORE (0-100, default 55).
 */
export class AcrcloudService {
  static isEnabled(): boolean {
    const host = (process.env.ACRCLOUD_HOST || "").trim();
    const key = (process.env.ACRCLOUD_ACCESS_KEY || "").trim();
    const secret = (process.env.ACRCLOUD_ACCESS_SECRET || "").trim();
    return !!(host && key && secret);
  }

  private static sign(stringToSign: string, secret: string): string {
    return crypto.createHmac("sha1", secret).update(stringToSign, "utf8").digest("base64");
  }

  static async identifyAudioFile(filePath: string): Promise<MatchResult | null> {
    if (!this.isEnabled()) return null;
    if (!fs.existsSync(filePath)) return null;

    const host = (process.env.ACRCLOUD_HOST || "").trim().replace(/^https?:\/\//i, "");
    const accessKey = (process.env.ACRCLOUD_ACCESS_KEY || "").trim();
    const accessSecret = (process.env.ACRCLOUD_ACCESS_SECRET || "").trim();
    const dataType = (process.env.ACRCLOUD_DATA_TYPE || "audio").trim() === "fingerprint" ? "fingerprint" : "audio";
    const httpMethod = "POST";
    const httpUri = "/v1/identify";
    const signatureVersion = "1";
    const timestamp = Math.floor(Date.now() / 1000).toString();

    const stringToSign = [httpMethod, httpUri, accessKey, dataType, signatureVersion, timestamp].join("\n");
    const signature = this.sign(stringToSign, accessSecret);

    const buf = fs.readFileSync(filePath);
    const sampleBytes = buf.length;
    if (sampleBytes < 256) return null;

    const form = new FormData();
    form.append("sample", buf, { filename: "sample.wav", contentType: "application/octet-stream" });
    form.append("sample_bytes", String(sampleBytes));
    form.append("access_key", accessKey);
    form.append("data_type", dataType);
    form.append("signature_version", signatureVersion);
    form.append("signature", signature);
    form.append("timestamp", timestamp);

    const url = `https://${host}${httpUri}`;
    const minScore = parseEnvFloat("ACRCLOUD_MIN_SCORE", 55);

    try {
      const response = await axios.post(url, form, {
        headers: form.getHeaders(),
        timeout: 25000,
        maxBodyLength: Infinity,
      });
      const data = response.data as {
        status?: { code?: number; msg?: string };
        metadata?: { music?: Array<Record<string, unknown>> };
      };
      const code = data?.status?.code;
      if (code !== 0 && code !== undefined) {
        logger.debug({ code, msg: data?.status?.msg }, "ACRCloud identify returned non-success");
        return null;
      }
      const music = data?.metadata?.music;
      if (!Array.isArray(music) || music.length === 0) {
        return null;
      }
      const m = music[0] as {
        title?: string;
        artists?: Array<{ name?: string }>;
        album?: { name?: string };
        score?: number;
        duration_ms?: number;
        genres?: Array<{ name?: string }>;
        release_date?: string;
        external_ids?: { isrc?: string };
        external_metadata?: { musicbrainz?: Array<{ track?: { id?: string } }> };
        label?: string;
      };
      const scoreRaw = typeof m.score === "number" ? m.score : 0;
      if (scoreRaw < minScore) {
        logger.debug({ scoreRaw, minScore, title: m.title }, "ACRCloud match below threshold");
        return null;
      }
      const artist = m.artists?.[0]?.name?.trim();
      const title = (m.title ?? "").trim();
      if (!title) return null;

      const mbid = m.external_metadata?.musicbrainz?.[0]?.track?.id;
      const isrc = m.external_ids?.isrc;
      const confidence = Math.min(1, scoreRaw > 1 ? scoreRaw / 100 : scoreRaw);

      return {
        score: confidence,
        confidence,
        title,
        artist: artist || undefined,
        releaseTitle: m.album?.name,
        releaseDate: m.release_date,
        genre: m.genres?.[0]?.name,
        labelName: typeof m.label === "string" ? m.label : undefined,
        durationMs: typeof m.duration_ms === "number" && m.duration_ms > 0 ? m.duration_ms : undefined,
        recordingId: mbid,
        isrcs: isrc ? [isrc] : undefined,
        sourceProvider: "acrcloud",
        reasonCode: "fingerprint_acrcloud",
      };
    } catch (error) {
      logger.warn({ error, filePath }, "ACRCloud identify failed");
      return null;
    }
  }
}

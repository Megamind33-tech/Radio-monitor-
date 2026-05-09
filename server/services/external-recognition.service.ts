import fs from "fs";
import axios from "axios";
import FormData from "form-data";
import { prisma } from "../lib/prisma.js";

export type ExternalProvider = "local_fingerprint" | "shazam_api_experimental";

export class ExternalRecognitionService {
  static isEnabled() { return String(process.env.EXTERNAL_RECOGNITION_ENABLED || "false").toLowerCase() === "true"; }
  static dailyLimit() { return Number(process.env.MAX_EXTERNAL_RECOGNITIONS_PER_DAY || 20); }
  static runLimit() { return Number(process.env.MAX_EXTERNAL_RECOGNITIONS_PER_RUN || 5); }

  static async testUnknownSample(input: { unknownSampleId: string; provider: ExternalProvider; dryRun?: boolean }) {
    if (!this.isEnabled()) return { ok: false, reason: "external_recognition_disabled" };
    const dryRun = input.dryRun !== false;
    const sample = await prisma.unresolvedSample.findUnique({ where: { id: input.unknownSampleId } });
    if (!sample) return { ok: false, reason: "unknown_sample_not_found" };
    if (!sample.filePath || !fs.existsSync(sample.filePath)) return { ok: false, reason: "audio_file_not_found" };

    const existing = await prisma.recognitionSuggestion.findUnique({ where: { unknownSampleId_provider: { unknownSampleId: sample.id, provider: input.provider } } });
    if (existing) return { ok: true, reused: true, suggestion: existing };

    const since = new Date(Date.now() - 86400000);
    const countDay = await prisma.recognitionSuggestion.count({ where: { provider: input.provider, createdAt: { gte: since } } });
    if (countDay >= this.dailyLimit()) return { ok: false, reason: "daily_limit_reached" };

    let suggestion: any = null;
    if (input.provider === "shazam_api_experimental") {
      const key = process.env.SHAZAM_API_KEY;
      if (!key) return { ok: false, reason: "missing_shazam_api_key" };
      const form = new FormData();
      form.append("file", fs.createReadStream(sample.filePath));
      const res = await axios.post("https://api.shazam-api.com/song/recognize", form, { headers: { ...form.getHeaders(), "x-api-key": key }, timeout: 30000 });
      const d = res.data || {};
      suggestion = {
        provider: input.provider,
        suggestedArtist: d?.artist || d?.track?.subtitle || null,
        suggestedTitle: d?.title || d?.track?.title || null,
        suggestedAlbum: d?.album || null,
        externalUrl: d?.url || d?.track?.url || null,
        rawResponseJson: JSON.stringify(d),
        confidence: Number(d?.confidence || 0.6),
      };
    }
    if (!suggestion) return { ok: false, reason: "no_suggestion" };
    if (!dryRun) {
      await prisma.recognitionSuggestion.create({ data: { unknownSampleId: sample.id, provider: suggestion.provider, suggestedArtist: suggestion.suggestedArtist, suggestedTitle: suggestion.suggestedTitle, suggestedAlbum: suggestion.suggestedAlbum, externalUrl: suggestion.externalUrl, rawResponseJson: suggestion.rawResponseJson, confidence: suggestion.confidence } });
    }
    return { ok: true, dryRun, suggestion };
  }

  static async summary() {
    const total = await prisma.recognitionSuggestion.count();
    const byStatus = await prisma.recognitionSuggestion.groupBy({ by: ["status"], _count: { _all: true } });
    return { total, byStatus };
  }
}

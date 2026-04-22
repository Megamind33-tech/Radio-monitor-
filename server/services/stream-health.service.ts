import { spawn, spawnSync } from "child_process";
import axios from "axios";
import { logger } from "../lib/logger.js";
import { StreamHealthSnapshot } from "../types.js";

function parseContentType(v: unknown): string | null {
  if (typeof v !== "string") return null;
  return v.split(";")[0]?.trim().toLowerCase() || null;
}

function isPlaylistLikeContentType(contentType: string | null): boolean {
  if (!contentType) return false;
  return (
    contentType.includes("application/vnd.apple.mpegurl") ||
    contentType.includes("application/x-mpegurl") ||
    contentType.includes("audio/mpegurl") ||
    contentType.includes("application/dash+xml")
  );
}

function isAudioContentType(contentType: string | null): boolean {
  if (!contentType) return false;
  return contentType.startsWith("audio/");
}

function parseIntFromRegex(s: string, re: RegExp): number | null {
  const m = re.exec(s);
  if (!m?.[1]) return null;
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) ? n : null;
}

async function probeWithFfprobe(url: string): Promise<{ ok: boolean; codec?: string; bitrate?: number; reason?: string }> {
  return new Promise((resolve) => {
    let stderr = "";
    let stdout = "";
    const p = spawn("ffprobe", [
      "-v",
      "error",
      "-show_entries",
      "stream=codec_name,bit_rate",
      "-show_entries",
      "format=bit_rate",
      "-print_format",
      "json",
      "-i",
      url,
    ]);

    const timeout = setTimeout(() => {
      p.kill();
      resolve({ ok: false, reason: "ffprobe_timeout" });
    }, 12_000);

    p.stdout.on("data", (d) => {
      stdout += String(d);
    });
    p.stderr.on("data", (d) => {
      stderr += String(d);
    });

    p.on("error", () => {
      clearTimeout(timeout);
      resolve({ ok: false, reason: "ffprobe_spawn_error" });
    });
    p.on("close", (code) => {
      clearTimeout(timeout);
      if (code !== 0) {
        const lower = stderr.toLowerCase();
        if (lower.includes("404")) return resolve({ ok: false, reason: "decoder_404" });
        if (lower.includes("timed out")) return resolve({ ok: false, reason: "decoder_timeout" });
        return resolve({ ok: false, reason: "decoder_nonzero_exit" });
      }
      try {
        const j = JSON.parse(stdout);
        const stream = Array.isArray(j.streams) ? j.streams[0] : undefined;
        const codec = stream?.codec_name ? String(stream.codec_name) : undefined;
        const bitrate =
          (typeof stream?.bit_rate === "string" ? parseInt(stream.bit_rate, 10) : null) ||
          (typeof j.format?.bit_rate === "string" ? parseInt(j.format.bit_rate, 10) : null) ||
          undefined;
        return resolve({ ok: true, codec, bitrate });
      } catch {
        return resolve({ ok: false, reason: "decoder_parse_error" });
      }
    });
  });
}

export class StreamHealthService {
  private static ffprobeAvailable: boolean | null = null;

  private static hasFfprobe(): boolean {
    if (this.ffprobeAvailable != null) return this.ffprobeAvailable;
    try {
      const r = spawnSync("ffprobe", ["-version"], { stdio: "ignore" });
      this.ffprobeAvailable = r.status === 0;
    } catch {
      this.ffprobeAvailable = false;
    }
    return this.ffprobeAvailable;
  }

  static async validateStream(url: string): Promise<StreamHealthSnapshot> {
    const start = Date.now();
    try {
      const response = await axios.get(url, {
        maxRedirects: 8,
        timeout: 12_000,
        responseType: "stream",
        validateStatus: () => true,
        headers: { "User-Agent": "RadioMonitor/1.0 stream-health" },
      });

      const resolvedUrl = String(response.request?.res?.responseUrl || url);
      const status = Number(response.status || 0);
      const contentType = parseContentType(response.headers?.["content-type"]);

      if (status < 200 || status >= 400) {
        return {
          reachable: false,
          audioFlowing: false,
          decoderOk: false,
          degraded: false,
          reason: `http_${status}`,
          resolvedUrl,
          contentTypeHeader: contentType,
          latencyMs: Date.now() - start,
        };
      }

      // Confirm bytes are flowing within a short window.
      let totalBytes = 0;
      let chunkCount = 0;
      const stream: NodeJS.ReadableStream = response.data;
      const byteResult = await new Promise<{ flowing: boolean; reason?: string }>((resolve) => {
        let done = false;
        const finish = (r: { flowing: boolean; reason?: string }) => {
          if (done) return;
          done = true;
          resolve(r);
        };
        const timer = setTimeout(() => finish({ flowing: totalBytes > 0, reason: "byte_timeout" }), 8_000);
        stream.on("data", (chunk: Buffer) => {
          totalBytes += chunk.length;
          chunkCount += 1;
          if (totalBytes >= 48_000 || chunkCount >= 3) {
            clearTimeout(timer);
            finish({ flowing: true });
          }
        });
        stream.on("error", () => {
          clearTimeout(timer);
          finish({ flowing: false, reason: "stream_read_error" });
        });
        stream.on("end", () => {
          clearTimeout(timer);
          finish({ flowing: totalBytes > 0, reason: totalBytes > 0 ? undefined : "stream_empty_end" });
        });
      });
      try {
        // Best effort close.
        (stream as any).destroy?.();
      } catch {
        // ignore
      }

      const playlistLike = isPlaylistLikeContentType(contentType);
      const audioLike = isAudioContentType(contentType);

      if (!audioLike && !playlistLike) {
        return {
          reachable: true,
          audioFlowing: byteResult.flowing,
          decoderOk: false,
          degraded: true,
          reason: `unsupported_content_type:${contentType || "unknown"}`,
          resolvedUrl,
          contentTypeHeader: contentType,
          latencyMs: Date.now() - start,
        };
      }

      if (!byteResult.flowing) {
        return {
          reachable: true,
          audioFlowing: false,
          decoderOk: false,
          degraded: true,
          reason: byteResult.reason || "no_audio_bytes",
          resolvedUrl,
          contentTypeHeader: contentType,
          latencyMs: Date.now() - start,
        };
      }

      // Deeper decode check (if ffprobe exists). Absence should not force station INACTIVE.
      const deep = this.hasFfprobe()
        ? await probeWithFfprobe(resolvedUrl)
        : { ok: true, reason: "ffprobe_unavailable" };
      const degraded = !deep.ok;
      return {
        reachable: true,
        audioFlowing: true,
        decoderOk: deep.ok,
        degraded,
        reason: deep.ok ? null : deep.reason || "decoder_failed",
        resolvedUrl,
        contentTypeHeader: contentType,
        codec: deep.codec,
        bitrate: deep.bitrate,
        latencyMs: Date.now() - start,
      };
    } catch (error) {
      logger.warn({ error, url }, "validateStream failed");
      return {
        reachable: false,
        audioFlowing: false,
        decoderOk: false,
        degraded: false,
        reason: "request_exception",
        resolvedUrl: url,
        latencyMs: Date.now() - start,
      };
    }
  }
}

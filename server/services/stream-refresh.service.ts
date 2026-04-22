import { spawnSync } from "child_process";
import * as path from "path";
import { fileURLToPath } from "url";
import axios from "axios";
import { logger } from "../lib/logger.js";

const UA =
  process.env.STREAM_REFRESH_UA ||
  "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0 Safari/537.36";

const ORB_ORIGIN = "https://onlineradiobox.com";

function dirnameFromImportMeta(): string {
  return path.dirname(fileURLToPath(import.meta.url));
}

function mytunerScriptPath(): string {
  return path.join(dirnameFromImportMeta(), "../../scripts/mytuner_refresh_stream.py");
}

/**
 * Try to obtain a fresh direct stream URL from harvest-time hints (MyTuner page,
 * OnlineRadioBox slug, Streema profile path). Returns null if nothing changed or fetch failed.
 */
export class StreamRefreshService {
  static async refreshFromSourceHints(
    sourceIdsJson: string | null | undefined,
    currentStreamUrl: string
  ): Promise<string | null> {
    if (!sourceIdsJson) return null;
    let ids: Record<string, string>;
    try {
      ids = JSON.parse(sourceIdsJson) as Record<string, string>;
    } catch {
      return null;
    }

    const mytuner = ids.mytuner?.trim();
    if (mytuner?.startsWith("http")) {
      const next = this.refreshMytunerPage(mytuner);
      if (next && next !== currentStreamUrl) return next;
    }

    const orb = ids.onlineradiobox?.trim();
    if (orb) {
      const next = await this.refreshOrbSlug(orb);
      if (next && next !== currentStreamUrl) return next;
    }

    const streema = ids.streema?.trim();
    if (streema) {
      const next = await this.refreshStreemaPath(streema);
      if (next && next !== currentStreamUrl) return next;
    }

    return null;
  }

  private static refreshMytunerPage(pageUrl: string): string | null {
    const script = mytunerScriptPath();
    const r = spawnSync(process.env.PYTHON || "python3", [script, pageUrl], {
      encoding: "utf8",
      timeout: 90_000,
      maxBuffer: 4096,
    });
    if (r.status !== 0 || !r.stdout?.trim()) {
      logger.debug({ pageUrl, status: r.status }, "MyTuner stream refresh: no URL");
      return null;
    }
    const url = r.stdout.trim().split(/\r?\n/)[0]?.trim();
    return url?.startsWith("http") ? url : null;
  }

  /** ORB `radioId` may be `zm.phoenix` while the site path is `/zm/phoenix/`. */
  private static orbCandidatePaths(radioIdOrSlug: string): string[] {
    const raw = radioIdOrSlug.trim();
    const out: string[] = [];
    const add = (p: string) => {
      if (p && !out.includes(p)) out.push(p);
    };

    if (raw.startsWith("/")) {
      add(raw.endsWith("/") ? raw : `${raw}/`);
      return out;
    }

    const noZm = raw.replace(/^zm\./i, "").replace(/^zm_/i, "");
    const segments = [raw, noZm, raw.replace(/^zm\./i, "")].filter(Boolean);
    for (const seg of segments) {
      const slug = seg.replace(/^\/+|\/+$/g, "");
      if (!slug) continue;
      add(`/zm/${slug}/`);
    }
    return out;
  }

  private static async refreshOrbSlug(radioIdOrSlug: string): Promise<string | null> {
    for (const p of this.orbCandidatePaths(radioIdOrSlug)) {
      const url = p.startsWith("http") ? p : `${ORB_ORIGIN}${p}`;
      try {
        const res = await axios.get<string>(url, {
          timeout: 25_000,
          headers: { "User-Agent": UA, "Accept-Language": "en-US,en;q=0.9" },
          validateStatus: (s) => s === 200,
        });
        const html = typeof res.data === "string" ? res.data : "";
        const stream = this.extractOrbStreamFromHtml(html);
        if (stream) return stream;
      } catch {
        // try next path variant
      }
    }
    return null;
  }

  private static extractOrbStreamFromHtml(html: string): string | null {
    const re =
      /class="[^"]*station_play[^"]*"([^>]*)>/gi;
    let m: RegExpExecArray | null;
    while ((m = re.exec(html)) !== null) {
      const attrs = m[1];
      const sm = /stream="(https?:\/\/[^"]+)"/i.exec(attrs);
      if (sm?.[1]?.startsWith("http")) return sm[1].trim();
    }
    return null;
  }

  private static async refreshStreemaPath(stationPath: string): Promise<string | null> {
    const p = stationPath.startsWith("http")
      ? stationPath
      : `https://streema.com${stationPath.startsWith("/") ? stationPath : `/${stationPath}`}`;
    try {
      const res = await axios.get<string>(p, {
        timeout: 25_000,
        headers: { "User-Agent": UA, "Accept-Language": "en-US,en;q=0.9" },
        validateStatus: (s) => s === 200,
      });
      const html = typeof res.data === "string" ? res.data : "";
      const dm = /id="source-stream"[^>]*data-src="(https?:\/\/[^"]+)"/i.exec(html);
      if (dm?.[1]?.startsWith("http")) return dm[1].trim();
    } catch {
      return null;
    }
    return null;
  }
}

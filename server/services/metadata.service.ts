import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { logger } from '../lib/logger.js';
import { NormalizedMetadata } from '../types.js';

interface MetadataSplitRule {
  pattern: string;
  artistIndex: number;
  titleIndex: number;
  flags?: string;
}

interface MetadataSplitConfig {
  separators?: string[];
  rules?: MetadataSplitRule[];
  swapHeuristics?: string[];
}

export class MetadataService {
  private static splitConfig: MetadataSplitConfig | null = null;

  /**
   * Attempts to read ICY metadata from a stream using ffprobe.
   */
  static async readStreamMetadata(url: string): Promise<NormalizedMetadata | null> {
    logger.debug({ url }, "Reading stream metadata via ffprobe");

    return new Promise((resolve) => {
      let settled = false;
      const done = (value: NormalizedMetadata | null) => {
        if (settled) return;
        settled = true;
        resolve(value);
      };

      // ffprobe -v quiet -show_format -show_streams -print_format json -i <url>
      // For ICY metadata, we often need to wait a few seconds or use specific flags
      const ffprobe = spawn('ffprobe', [
        '-v', 'quiet',
        '-print_format', 'json',
        '-show_format',
        '-i', url
      ]);

      let stdout = '';
      const timeout = setTimeout(() => {
        ffprobe.kill();
        logger.warn({ url }, "ffprobe metadata read timed out");
        done(null);
      }, 10000);

      ffprobe.stdout.on('data', (data) => {
        stdout += data;
      });

      ffprobe.on('error', (error) => {
        clearTimeout(timeout);
        logger.warn({ url, error }, "ffprobe failed to start");
        done(null);
      });

      ffprobe.on('close', (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          logger.debug({ url, code }, "ffprobe exited with non-zero code");
          done(null);
          return;
        }

        try {
          const data = JSON.parse(stdout);
          const tags = data.format?.tags;
          
          if (!tags) {
            done(null);
            return;
          }

          // ICY/Shoutcast metadata usually comes in StreamTitle
          const streamTitle = tags.StreamTitle || tags.title;
          if (!streamTitle) {
            done(null);
            return;
          }

          const normalized = this.parseStreamTitle(streamTitle);
          done(normalized);
        } catch (error) {
          logger.error({ error, stdout }, "Failed to parse ffprobe output");
          done(null);
        }
      });
    });
  }

  /**
   * Provider fallback for streams that carry no in-band ICY StreamTitle.
   * Supports Fastcast proxy paths that expose stats JSON / current song endpoints.
   */
  static async readProviderNowPlayingMetadata(url: string): Promise<NormalizedMetadata | null> {
    const fastcastBase = this.extractFastcastProxyBase(url);
    if (!fastcastBase) return null;

    const statsUrl = `${fastcastBase}/stats?json=1`;
    try {
      const stats = await axios.get(statsUrl, { timeout: 6000 });
      const title = this.pickProviderSongTitle(stats.data);
      if (title) {
        return this.parseStreamTitle(title);
      }
    } catch (error) {
      logger.debug({ error, statsUrl }, 'Fastcast stats metadata fallback failed');
    }

    const currentSongUrl = `${fastcastBase}/currentsong`;
    try {
      const now = await axios.get(currentSongUrl, { timeout: 6000, responseType: 'text' });
      const text = String(now.data || '').trim();
      if (this.isUsefulSongText(text)) {
        return this.parseStreamTitle(text);
      }
    } catch (error) {
      logger.debug({ error, currentSongUrl }, 'Fastcast currentsong metadata fallback failed');
    }

    return null;
  }

  private static extractFastcastProxyBase(url: string): string | null {
    const m = String(url || '').match(/^(https?:\/\/[^/]+\/proxy\/[^/?#]+)/i);
    return m?.[1] ?? null;
  }

  private static pickProviderSongTitle(payload: unknown): string | null {
    const data = payload && typeof payload === 'object' ? (payload as Record<string, unknown>) : null;
    if (!data) return null;
    const candidates = [
      data.songtitle,
      data.songtitle_raw,
      data.song,
      data.nowplaying,
      data.current_song,
      data.currentsong,
    ];
    for (const c of candidates) {
      const text = typeof c === 'string' ? c.trim() : '';
      if (this.isUsefulSongText(text)) return text;
    }
    return null;
  }

  private static isUsefulSongText(text: string): boolean {
    const t = String(text || '').trim();
    if (!t) return false;
    const low = t.toLowerCase();
    if (low === '-' || low === 'n/a') return false;
    if (low === 'no name' || low === 'unknown') return false;
    if (low === 'offline' || low === 'not available') return false;
    return true;
  }

  private static parseStreamTitle(title: string): NormalizedMetadata {
    const cleanTitle = title.trim();
    const config = this.loadSplitConfig();
    let artist = '';
    let song = '';
    let splitRuleApplied: string | undefined;
    let splitConfidence: number | undefined;

    // JSON-driven regex split rules (more robust than naive separator split).
    const rules = Array.isArray(config.rules) ? config.rules : [];
    for (const rule of rules) {
      try {
        const re = new RegExp(rule.pattern, rule.flags || 'i');
        const match = cleanTitle.match(re);
        if (!match) continue;
        const fromMatch = (idx: number) => (idx >= 0 && idx < match.length ? String(match[idx] || '').trim() : '');
        const candidateArtist = fromMatch(rule.artistIndex);
        const candidateTitle = fromMatch(rule.titleIndex);
        if (candidateArtist && candidateTitle) {
          artist = candidateArtist;
          song = candidateTitle;
          splitRuleApplied = `regex:${rule.pattern}`;
          splitConfidence = 0.95;
          break;
        }
      } catch (error) {
        logger.warn({ error, rule }, 'Invalid metadata split regex rule');
      }
    }

    if (artist && song) {
      const normalized = this.normalizeSplitPair(artist, song, config);
      artist = normalized.artist;
      song = normalized.title;
    }

    // Separator fallback.
    const separators = Array.isArray(config.separators) && config.separators.length > 0
      ? config.separators
      : [' - ', ' / ', ' – '];
    for (const sep of separators) {
      if (artist && song) break;
      if (cleanTitle.includes(sep)) {
        const parts = cleanTitle.split(sep);
        const left = parts[0]?.trim() || '';
        const right = parts.slice(1).join(sep).trim();
        if (left && right) {
          const normalized = this.normalizeSplitPair(left, right, config);
          artist = normalized.artist;
          song = normalized.title;
          splitRuleApplied = `separator:${sep.trim()}`;
          splitConfidence = 0.8;
        }
        break;
      }
    }

    if (!artist && !song) {
      song = cleanTitle;
    }

    return {
      rawArtist: artist,
      rawTitle: song,
      combinedRaw: cleanTitle,
      sourceType: 'stream_metadata',
      splitRuleApplied,
      splitConfidence,
    };
  }

  private static normalizeSplitPair(
    first: string,
    second: string,
    config: MetadataSplitConfig
  ): { artist: string; title: string } {
    let artist = first;
    let title = second;

    const artistHints = Array.isArray(config.swapHeuristics) && config.swapHeuristics.length > 0
      ? config.swapHeuristics
      : ['radio', 'fm', 'live', 'session', 'mix', 'remix', 'feat.', 'ft.'];
    const titleLooksLikeArtist = artistHints.some((hint) =>
      title.toLowerCase().includes(hint.toLowerCase())
    );

    const firstHasManyWords = first.split(/\s+/).filter(Boolean).length >= 4;
    const secondHasFewWords = second.split(/\s+/).filter(Boolean).length <= 2;
    if (titleLooksLikeArtist || (firstHasManyWords && secondHasFewWords)) {
      artist = second;
      title = first;
    }

    return { artist: artist.trim(), title: title.trim() };
  }

  private static loadSplitConfig(): MetadataSplitConfig {
    if (this.splitConfig) return this.splitConfig;
    const fallback: MetadataSplitConfig = {
      separators: [' - ', ' / ', ' – '],
      rules: [],
      swapHeuristics: ['radio', 'fm', 'live', 'session', 'mix', 'remix', 'feat.', 'ft.'],
    };
    try {
      const configPath = path.join(process.cwd(), 'server', 'config', 'metadata_split_rules.json');
      const raw = fs.readFileSync(configPath, 'utf8');
      const parsed = JSON.parse(raw) as MetadataSplitConfig;
      this.splitConfig = {
        separators: Array.isArray(parsed.separators) ? parsed.separators : fallback.separators,
        rules: Array.isArray(parsed.rules) ? parsed.rules : fallback.rules,
        swapHeuristics: Array.isArray(parsed.swapHeuristics) ? parsed.swapHeuristics : fallback.swapHeuristics,
      };
      return this.splitConfig;
    } catch (error) {
      logger.warn({ error }, 'Failed to load metadata split config, using defaults');
      this.splitConfig = fallback;
      return this.splitConfig;
    }
  }

  static isMetadataTrustworthy(metadata: NormalizedMetadata, lastMetadata?: string): { trusted: boolean, reason?: string } {
    if (!metadata.combinedRaw) return { trusted: false, reason: 'empty' };

    const text = metadata.combinedRaw.toLowerCase();
    
    // Station branding patterns
    const brandingPatterns = [
      'radio', 'fm', 'playing now', 'tune in', 'live', 'on air', 
      'streaming', 'advertising', 'news', 'weather'
    ];

    if (brandingPatterns.some(p => text.includes(p) && text.length < 30)) {
      return { trusted: false, reason: 'branding' };
    }

    if (lastMetadata === metadata.combinedRaw) {
      // This logic should be handled by the caller with a timestamp check
      // But we can flag it here as "potentially stale"
      return { trusted: true }; // Caller will decide based on time
    }

    return { trusted: true };
  }
}

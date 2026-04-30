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

interface MetadataRegexPattern {
  name?: string;
  regex: string;
  flags?: string;
  orientation?: 'artist_title' | 'title_artist';
}

interface MetadataSplitConfig {
  separators?: string[];
  rules?: MetadataSplitRule[];
  regexPatterns?: MetadataRegexPattern[];
  swapHeuristics?: string[];
  stripPrefixes?: string[];
  stripSuffixes?: string[];
  noiseTokens?: string[];
  titleHintTokens?: string[];
  artistHintTokens?: string[];
}

const METADATA_UA = process.env.STREAM_METADATA_UA || 'RadioMonitor/1.0 metadata';

export class MetadataService {
  private static splitConfig: MetadataSplitConfig | null = null;

  /**
   * Reads realtime stream metadata. Prefer direct ICY/Shoutcast blocks, because
   * many stations only emit StreamTitle after the client asks for Icy-MetaData.
   */
  static async readStreamMetadata(url: string): Promise<NormalizedMetadata | null> {
    logger.debug({ url }, 'Reading stream metadata');

    const icy = await this.readIcyHttpMetadata(url);
    if (icy) return icy;

    return new Promise((resolve) => {
      let settled = false;
      const done = (value: NormalizedMetadata | null) => {
        if (settled) return;
        settled = true;
        resolve(value);
      };

      const ffprobe = spawn('ffprobe', [
        '-v', 'quiet',
        '-user_agent', METADATA_UA,
        '-rw_timeout', '15000000',
        '-print_format', 'json',
        '-show_format',
        '-i', url,
      ]);

      let stdout = '';
      const timeout = setTimeout(() => {
        ffprobe.kill();
        logger.warn({ url }, 'ffprobe metadata read timed out');
        done(null);
      }, 15000);

      ffprobe.stdout.on('data', (data) => {
        stdout += data;
      });

      ffprobe.on('error', (error) => {
        clearTimeout(timeout);
        logger.warn({ url, error }, 'ffprobe failed to start');
        done(null);
      });

      ffprobe.on('close', (code) => {
        clearTimeout(timeout);
        if (code !== 0) {
          logger.debug({ url, code }, 'ffprobe exited with non-zero code');
          done(null);
          return;
        }

        try {
          const data = JSON.parse(stdout);
          const tags = data.format?.tags;
          const streamTitle = tags?.StreamTitle || tags?.title || tags?.icy_title;
          if (!streamTitle) {
            done(null);
            return;
          }
          done(this.parseStreamTitle(streamTitle));
        } catch (error) {
          logger.error({ error, stdout }, 'Failed to parse ffprobe output');
          done(null);
        }
      });
    });
  }

  /**
   * Realtime ICY/Shoutcast metadata reader. This opts in with Icy-MetaData: 1,
   * reads the advertised metaint, and parses the first useful StreamTitle block.
   */
  private static async readIcyHttpMetadata(url: string): Promise<NormalizedMetadata | null> {
    if (!String(url || '').startsWith('http')) return null;
    let stream: NodeJS.ReadableStream | null = null;
    try {
      const res = await axios.get<NodeJS.ReadableStream>(url, {
        maxRedirects: 8,
        timeout: 15000,
        responseType: 'stream',
        validateStatus: (s) => s >= 200 && s < 400,
        headers: {
          'User-Agent': METADATA_UA,
          'Icy-MetaData': '1',
          Accept: '*/*',
        },
      });
      stream = res.data;
      const rawMetaInt = Array.isArray(res.headers?.['icy-metaint'])
        ? res.headers['icy-metaint'][0]
        : res.headers?.['icy-metaint'];
      const metaInt = parseInt(String(rawMetaInt || ''), 10);
      if (!Number.isFinite(metaInt) || metaInt <= 0 || metaInt > 8_000_000) return null;

      const blocks = await this.readIcyMetadataBlocks(stream, metaInt, 3, 20000);
      for (const block of blocks) {
        const streamTitle = this.extractStreamTitleFromIcyBlock(block);
        if (streamTitle && this.isUsefulSongText(streamTitle)) {
          return this.parseStreamTitle(streamTitle);
        }
      }
      return null;
    } catch (error) {
      logger.debug({ error, url }, 'Direct ICY metadata read failed');
      return null;
    } finally {
      try {
        (stream as any)?.destroy?.();
      } catch {
        // ignore best-effort close
      }
    }
  }

  private static readIcyMetadataBlocks(
    stream: NodeJS.ReadableStream,
    metaInt: number,
    maxBlocks: number,
    timeoutMs: number
  ): Promise<string[]> {
    return new Promise((resolve) => {
      let settled = false;
      let audioRemaining = metaInt;
      let metadataLength = -1;
      let metadata = Buffer.alloc(0);
      const blocks: string[] = [];
      let onData: (chunk: Buffer) => void = () => undefined;

      const cleanup = () => {
        stream.off('data', onData);
        stream.off('error', onError);
        stream.off('end', onEnd);
        clearTimeout(timer);
      };
      const done = () => {
        if (settled) return;
        settled = true;
        cleanup();
        resolve(blocks);
      };
      const onError = () => done();
      const onEnd = () => done();
      const timer = setTimeout(() => done(), timeoutMs);

      onData = (chunk: Buffer) => {
        let offset = 0;
        while (offset < chunk.length) {
          if (audioRemaining > 0) {
            const skip = Math.min(audioRemaining, chunk.length - offset);
            audioRemaining -= skip;
            offset += skip;
            continue;
          }

          if (metadataLength < 0) {
            metadataLength = (chunk[offset] || 0) * 16;
            offset += 1;
            if (metadataLength === 0) {
              blocks.push('');
              if (blocks.length >= maxBlocks) return done();
              audioRemaining = metaInt;
              metadataLength = -1;
              metadata = Buffer.alloc(0);
            }
            continue;
          }

          const need = metadataLength - metadata.length;
          const take = Math.min(need, chunk.length - offset);
          metadata = Buffer.concat([metadata, chunk.subarray(offset, offset + take)]);
          offset += take;
          if (metadata.length >= metadataLength) {
            blocks.push(metadata.toString('utf8').replace(/\0+$/g, '').trim());
            if (blocks.length >= maxBlocks) return done();
            audioRemaining = metaInt;
            metadataLength = -1;
            metadata = Buffer.alloc(0);
          }
        }
      };

      stream.on('data', onData);
      stream.once('error', onError);
      stream.once('end', onEnd);
    });
  }

  private static extractStreamTitleFromIcyBlock(block: string | null): string | null {
    const raw = String(block || '').trim();
    if (!raw) return null;
    const m = /StreamTitle=(['"])([\s\S]*?)\1\s*;?/i.exec(raw) || /StreamTitle=([^;]*)/i.exec(raw);
    const title = (m?.[2] || m?.[1] || '').trim();
    if (!title) return null;
    return this.decodeXmlEntities(title);
  }

  /**
   * When `sourceIdsJson` contains `tunein` (RadioTime guide id, e.g. `s131737`), fetch TuneIn OPML
   * search results and read `playing=` / `subtext` for that station. Many streams have no ICY but
   * TuneIn still exposes a current title string for catalog lookup.
   */
  static async readTuneInStubMetadata(
    sourceIdsJson: string | null | undefined,
    stationNameHint: string | null | undefined
  ): Promise<NormalizedMetadata | null> {
    const raw = String(sourceIdsJson || '').trim();
    if (!raw) return null;
    let ids: Record<string, unknown>;
    try {
      ids = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      return null;
    }
    const tune = ids.tunein;
    const guide =
      typeof tune === 'string' && tune.trim()
        ? tune.trim()
        : typeof tune === 'number'
          ? String(tune)
          : '';
    if (!guide) return null;
    const want = guide.toLowerCase();
    const nameQ = String(stationNameHint || '').trim() || guide;
    const queries = nameQ.toLowerCase() === want ? [nameQ] : [nameQ, guide];

    for (const query of queries) {
      const searchUrl = `https://opml.radiotime.com/Search.ashx?${new URLSearchParams({ query }).toString()}`;
      try {
        const res = await axios.get(searchUrl, {
          timeout: 12000,
          responseType: 'text',
          headers: { 'User-Agent': 'RadioMonitor/1.0 (tunein-opml-stub)' },
        });
        const xml = String(res.data || '');
        const outlines = [...xml.matchAll(/<outline\s+([^>]+)>/gi)];
        for (const m of outlines) {
          const attrs = m[1] || '';
          const gid = (attrs.match(/\bguide_id="([^"]*)"/i) || attrs.match(/\bguide_id='([^']*)'/i))?.[1]?.trim() || '';
          const urlM = (attrs.match(/\bURL="([^"]*)"/i) || [])[1] || '';
          const type = (attrs.match(/\btype="([^"]*)"/i) || [])[1]?.toLowerCase() || '';
          const item = (attrs.match(/\bitem="([^"]*)"/i) || [])[1]?.toLowerCase() || '';
          if (type !== 'audio' || item !== 'station') continue;
          const idInUrl = urlM.toLowerCase().includes(`id=${want}`) || urlM.toLowerCase().includes(`id=${encodeURIComponent(guide).toLowerCase()}`);
          if (gid.toLowerCase() !== want && !idInUrl) continue;

          const playing = (attrs.match(/\bplaying="([^"]*)"/i) || [])[1]?.trim();
          const subtext = (attrs.match(/\bsubtext="([^"]*)"/i) || [])[1]?.trim();
          const text = (attrs.match(/\btext="([^"]*)"/i) || [])[1]?.trim();
          const decoded = this.decodeXmlEntities(playing || subtext || text || '');
          if (!decoded || !this.isUsefulSongText(decoded)) continue;
          return this.parseStreamTitle(decoded);
        }
      } catch (error) {
        logger.debug({ error, searchUrl }, 'TuneIn OPML stub metadata failed');
      }
    }
    return null;
  }

  static async readProviderNowPlayingMetadata(url: string): Promise<NormalizedMetadata | null> {
    const fastcastBase = this.extractFastcastProxyBase(url);
    if (fastcastBase) {
      const statsUrl = `${fastcastBase}/stats?json=1`;
      try {
        const stats = await axios.get(statsUrl, { timeout: 6000 });
        const title = this.pickProviderSongTitle(stats.data);
        if (title) return this.parseStreamTitle(title);
      } catch (error) {
        logger.debug({ error, statsUrl }, 'Fastcast stats metadata fallback failed');
      }

      const currentSongUrl = `${fastcastBase}/currentsong`;
      try {
        const now = await axios.get(currentSongUrl, { timeout: 6000, responseType: 'text' });
        const text = this.decodeXmlEntities(String(now.data || '').trim());
        if (this.isUsefulSongText(text)) return this.parseStreamTitle(text);
      } catch (error) {
        logger.debug({ error, currentSongUrl }, 'Fastcast currentsong metadata fallback failed');
      }
    }

    return (
      (await this.readIcecastStatusMetadata(url)) ||
      (await this.readShoutcastStatsMetadata(url)) ||
      (await this.readShoutcastSevenHtmlMetadata(url))
    );
  }

  private static async readIcecastStatusMetadata(streamUrl: string): Promise<NormalizedMetadata | null> {
    let statusUrl = '';
    try {
      const u = new URL(streamUrl);
      const mountPath = u.pathname.replace(/\/+$/, '');
      statusUrl = `${u.protocol}//${u.host}/status-json.xsl`;
      const res = await axios.get(statusUrl, {
        timeout: 6000,
        headers: { 'User-Agent': METADATA_UA },
        validateStatus: (s) => s === 200,
      });
      const source = res.data?.icestats?.source;
      const sources = Array.isArray(source) ? source : source ? [source] : [];
      const ordered = [...sources].sort((a, b) => {
        const aUrl = String(a?.listenurl || '');
        const bUrl = String(b?.listenurl || '');
        const aMatch = mountPath && aUrl.includes(mountPath) ? 1 : 0;
        const bMatch = mountPath && bUrl.includes(mountPath) ? 1 : 0;
        return bMatch - aMatch;
      });
      for (const row of ordered) {
        const separate = [row?.artist, row?.title].map((x) => (typeof x === 'string' ? x.trim() : '')).filter(Boolean);
        const title = separate.length === 2 ? `${separate[0]} - ${separate[1]}` : this.pickProviderSongTitle(row);
        if (title) return this.parseStreamTitle(title);
      }
    } catch (error) {
      logger.debug({ error, statusUrl }, 'Icecast status-json metadata fallback failed');
    }
    return null;
  }

  private static async readShoutcastStatsMetadata(streamUrl: string): Promise<NormalizedMetadata | null> {
    let statusUrl = '';
    try {
      const u = new URL(streamUrl);
      const base = `${u.protocol}//${u.host}`;
      const endpoints = [
        `${base}/currentsong?sid=1`,
        `${base}/stats?sid=1&json=1`,
        `${base}/statistics?json=1`,
      ];
      for (const endpoint of endpoints) {
        statusUrl = endpoint;
        const res = await axios.get(endpoint, {
          timeout: 6000,
          responseType: endpoint.includes('json=1') ? 'json' : 'text',
          headers: { 'User-Agent': METADATA_UA },
          validateStatus: (s) => s === 200,
        });
        const title =
          typeof res.data === 'string'
            ? this.decodeXmlEntities(res.data.trim())
            : this.pickProviderSongTitle(res.data);
        if (title && this.isUsefulSongText(title)) return this.parseStreamTitle(title);
      }
    } catch (error) {
      logger.debug({ error, statusUrl }, 'Shoutcast stats metadata fallback failed');
    }
    return null;
  }

  private static async readShoutcastSevenHtmlMetadata(streamUrl: string): Promise<NormalizedMetadata | null> {
    let statusUrl = '';
    try {
      const u = new URL(streamUrl);
      statusUrl = `${u.protocol}//${u.host}/7.html`;
      const res = await axios.get<string>(statusUrl, {
        timeout: 6000,
        responseType: 'text',
        headers: { 'User-Agent': METADATA_UA },
        validateStatus: (s) => s === 200,
      });
      const text = String(res.data || '').replace(/<[^>]+>/g, '').trim();
      const parts = text.split(',');
      const title = this.decodeXmlEntities(parts.slice(6).join(',').trim());
      if (this.isUsefulSongText(title)) return this.parseStreamTitle(title);
    } catch (error) {
      logger.debug({ error, statusUrl }, 'Shoutcast 7.html metadata fallback failed');
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
      data.title,
      data.streamtitle,
      data.songtitle,
      data.songtitle_raw,
      data.song,
      data.nowplaying,
      data.current_song,
      data.currentsong,
      data.yp_currently_playing,
    ];
    for (const c of candidates) {
      const text = typeof c === 'string' ? c.trim() : '';
      if (this.isUsefulSongText(text)) return text;
    }
    return null;
  }

  private static decodeXmlEntities(text: string): string {
    return String(text || '')
      .replace(/&apos;/g, "'")
      .replace(/&#39;/g, "'")
      .replace(/&quot;/g, '"')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .trim();
  }

  private static isUsefulSongText(text: string): boolean {
    const t = String(text || '').trim();
    if (!t) return false;
    const low = t.toLowerCase();
    if (low === '-' || low === 'n/a') return false;
    if (low === 'no name' || low === 'unknown') return false;
    if (low === 'offline' || low === 'not available') return false;
    if (/^[_\s-]{6,}$/.test(t)) return false;
    if (/^[-=_*.!·•\s]{6,}$/.test(t)) return false;
    if (/^(online|live|replay|feel the power)$/i.test(t)) return false;
    if (/^[\s'"`.,:;|/\\()[\]{}<>~+=_*#-]+$/.test(t)) return false;
    const compact = t.replace(/\s+/g, '');
    if (compact.length >= 6) {
      const lettersOrDigits = (compact.match(/[A-Za-z0-9]/g) ?? []).length;
      const nonLatin = (compact.match(/[^\x00-\x7F]/g) ?? []).length;
      const bracketNoise = (compact.match(/[⫷⫸⫹⫺ꢂꢃꢄꢊ]/g) ?? []).length;
      if (lettersOrDigits / compact.length < 0.25) return false;
      if (bracketNoise >= 2 || nonLatin / compact.length > 0.45) return false;
    }
    if (/(['"`]\s*){5,}/.test(t)) return false;
    return true;
  }

  private static parseStreamTitle(title: string): NormalizedMetadata {
    const config = this.loadSplitConfig();
    const cleanTitle = this.cleanConfiguredTitle(title, config);
    let artist = '';
    let song = '';
    let splitRuleApplied: string | undefined;
    let splitConfidence: number | undefined;

    const regexPatterns = Array.isArray(config.regexPatterns) ? config.regexPatterns : [];
    for (const rule of regexPatterns) {
      try {
        const re = new RegExp(rule.regex, rule.flags || 'i');
        const match = cleanTitle.match(re);
        if (!match) continue;
        const groups = match.groups || {};
        const candidateArtist = String(groups.artist || '').trim();
        const candidateTitle = String(groups.title || '').trim();
        if (candidateArtist && candidateTitle) {
          artist = candidateArtist;
          song = candidateTitle;
          splitRuleApplied = `regex:${rule.name || rule.regex}`;
          splitConfidence = 0.95;
          break;
        }
      } catch (error) {
        logger.warn({ error, rule }, 'Invalid metadata split regex pattern');
      }
    }

    const rules = Array.isArray(config.rules) ? config.rules : [];
    for (const rule of rules) {
      if (artist && song) break;
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

    if (!artist && !song) song = cleanTitle;

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

  private static cleanConfiguredTitle(title: string, config: MetadataSplitConfig): string {
    let out = this.decodeXmlEntities(String(title || '')).trim();
    const lower = () => out.toLowerCase();

    for (const token of config.noiseTokens || []) {
      const t = token.toLowerCase().trim();
      if (t && lower() === t) return '';
    }
    for (const prefix of config.stripPrefixes || []) {
      const p = prefix.toLowerCase().trim();
      if (p && lower().startsWith(p)) {
        out = out.slice(prefix.length).replace(/^\s*[-:|]\s*/, '').trim();
      }
    }
    for (const suffix of config.stripSuffixes || []) {
      const s = suffix.toLowerCase().trim();
      if (s && lower().endsWith(s)) {
        out = out.slice(0, Math.max(0, out.length - suffix.length)).trim();
      }
    }
    return out;
  }

  private static loadSplitConfig(): MetadataSplitConfig {
    if (this.splitConfig) return this.splitConfig;
    const fallback: MetadataSplitConfig = {
      separators: [' - ', ' / ', ' – '],
      rules: [],
      regexPatterns: [],
      swapHeuristics: ['radio', 'fm', 'live', 'session', 'mix', 'remix', 'feat.', 'ft.'],
      stripPrefixes: [],
      stripSuffixes: [],
      noiseTokens: [],
    };
    try {
      const configPath = path.join(process.cwd(), 'server', 'config', 'metadata_split_rules.json');
      const raw = fs.readFileSync(configPath, 'utf8');
      const parsed = JSON.parse(raw) as MetadataSplitConfig;
      this.splitConfig = {
        separators: Array.isArray(parsed.separators) ? parsed.separators : fallback.separators,
        rules: Array.isArray(parsed.rules) ? parsed.rules : fallback.rules,
        regexPatterns: Array.isArray(parsed.regexPatterns) ? parsed.regexPatterns : fallback.regexPatterns,
        swapHeuristics: Array.isArray(parsed.swapHeuristics) ? parsed.swapHeuristics : fallback.swapHeuristics,
        stripPrefixes: Array.isArray(parsed.stripPrefixes) ? parsed.stripPrefixes : fallback.stripPrefixes,
        stripSuffixes: Array.isArray(parsed.stripSuffixes) ? parsed.stripSuffixes : fallback.stripSuffixes,
        noiseTokens: Array.isArray(parsed.noiseTokens) ? parsed.noiseTokens : fallback.noiseTokens,
        titleHintTokens: Array.isArray(parsed.titleHintTokens) ? parsed.titleHintTokens : undefined,
        artistHintTokens: Array.isArray(parsed.artistHintTokens) ? parsed.artistHintTokens : undefined,
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
    const brandingPatterns = [
      'radio', 'fm', 'playing now', 'tune in', 'live', 'on air',
      'streaming', 'advertising', 'news', 'weather',
    ];

    const looksLikeSongLine = /[-–—/]/.test(metadata.combinedRaw || '') || /\bfeat\.?\b|\bft\.?\b/i.test(text);
    if (brandingPatterns.some((p) => text.includes(p) && text.length < 72 && !looksLikeSongLine)) {
      return { trusted: false, reason: 'branding' };
    }

    if (lastMetadata === metadata.combinedRaw) return { trusted: true };
    return { trusted: true };
  }
}

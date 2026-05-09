#!/usr/bin/env bash
set -euo pipefail

LOCK_FILE="/tmp/radiobox-metadata-wake.lock"
exec 9>"$LOCK_FILE"
flock -n 9 || exit 0

APP_DIR="/opt/radio-monitor"
LOG_FILE="$APP_DIR/logs/radiobox-metadata-wake.log"

cd "$APP_DIR"

echo "==== $(date -Is) RadioBox / now-playing metadata wake started ====" >> "$LOG_FILE"

# Keep main service alive
if ! systemctl is-active --quiet mostify-monitor.service; then
  echo "$(date -Is) mostify-monitor not active, restarting..." >> "$LOG_FILE"
  systemctl restart mostify-monitor.service || true
fi

# Run lightweight metadata wake using Prisma + stream headers + RadioBox URLs where available
node <<'NODE' >> "$LOG_FILE" 2>&1
const { PrismaClient } = require('@prisma/client');
const http = require('http');
const https = require('https');

const prisma = new PrismaClient();

function textHasRadioBox(v) {
  return typeof v === 'string' && /onlineradiobox\.com|radio\s*box|radiobox/i.test(v);
}

function hasNowPlayingHints(st) {
  const joined = JSON.stringify(st).toLowerCase();
  return (
    joined.includes('icy') ||
    joined.includes('nowplaying') ||
    joined.includes('now playing') ||
    joined.includes('songidentified') ||
    joined.includes('songidentifiedlast') ||
    joined.includes('metadata') ||
    joined.includes('onlineradiobox.com')
  );
}

function extractUrlsFromStation(st) {
  const urls = new Set();

  for (const [k, v] of Object.entries(st)) {
    if (typeof v === 'string') {
      const matches = v.match(/https?:\/\/[^\s"'<>]+/g);
      if (matches) matches.forEach(u => urls.add(u));
    }
  }

  try {
    for (const [k, v] of Object.entries(st)) {
      if (typeof v === 'string' && (v.trim().startsWith('{') || v.trim().startsWith('['))) {
        const parsed = JSON.parse(v);
        const s = JSON.stringify(parsed);
        const matches = s.match(/https?:\/\/[^\s"'<>]+/g);
        if (matches) matches.forEach(u => urls.add(u));
      }
    }
  } catch {}

  return [...urls];
}

function requestUrl(url, timeoutMs = 12000) {
  return new Promise((resolve) => {
    const lib = url.startsWith('https') ? https : http;
    let settled = false;
    let body = '';

    function finish(result) {
      if (settled) return;
      settled = true;
      clearTimeout(killTimer);
      resolve(result);
    }

    const killTimer = setTimeout(() => {
      finish({ ok: false, status: 0, error: 'hard_timeout', body: body.slice(0, 120000), headers: {} });
      try { req.destroy(); } catch {}
    }, timeoutMs);

    const req = lib.get(url, {
      headers: {
        'User-Agent': 'MostifyRadioMonitor/1.0 metadata-wake',
        'Icy-MetaData': '1',
        'Accept': 'text/html,application/json,audio/*,*/*'
      }
    }, (res) => {
      const contentType = String(res.headers['content-type'] || '').toLowerCase();
      const isAudio = contentType.includes('audio') || res.headers['icy-metaint'] || res.headers['icy-name'];

      if (isAudio) {
        setTimeout(() => {
          finish({
            ok: res.statusCode >= 200 && res.statusCode < 400,
            status: res.statusCode,
            headers: res.headers,
            body: body.slice(0, 20000)
          });
          try { req.destroy(); } catch {}
        }, 1500);
      }

      res.on('data', (chunk) => {
        if (body.length < 120000) body += chunk.toString('utf8', 0, Math.min(chunk.length, 20000));
        if (body.length >= 120000) {
          finish({
            ok: res.statusCode >= 200 && res.statusCode < 400,
            status: res.statusCode,
            headers: res.headers,
            body: body.slice(0, 120000)
          });
          try { req.destroy(); } catch {}
        }
      });

      res.on('end', () => {
        finish({
          ok: res.statusCode >= 200 && res.statusCode < 400,
          status: res.statusCode,
          headers: res.headers,
          body: body.slice(0, 120000)
        });
      });
    });

    req.on('error', (err) => {
      finish({ ok: false, status: 0, error: err.message, headers: {}, body: body.slice(0, 120000) });
    });
  });
}

function extractNowPlaying(text) {
  if (!text) return null;

  const cleaned = text
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&quot;/g, '"')
    .replace(/&#039;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/\s+/g, ' ')
    .trim();

  const patterns = [
    /on the air\s+(.{5,180})/i,
    /now playing\s+(.{5,180})/i,
    /current(?:ly)? playing\s+(.{5,180})/i,
    /artist["']?\s*:\s*["']([^"']{2,80})["'][\s\S]{0,120}?title["']?\s*:\s*["']([^"']{2,100})["']/i,
    /title["']?\s*:\s*["']([^"']{2,100})["'][\s\S]{0,120}?artist["']?\s*:\s*["']([^"']{2,80})["']/i
  ];

  for (const p of patterns) {
    const m = cleaned.match(p);
    if (m) return m.slice(1).join(' - ').trim();
  }

  return null;
}

async function main() {
  const stations = await prisma.station.findMany({
    where: { isActive: true },
    take: Number(process.env.WAKE_LIMIT || 80)
  });

  const candidates = stations.filter(st => {
    const urls = extractUrlsFromStation(st);
    const hasRadioBox = urls.some(textHasRadioBox) || Object.values(st).some(textHasRadioBox);
    return hasRadioBox || hasNowPlayingHints(st);
  });

  console.log(`stations=${stations.length} candidates=${candidates.length}`);

  let checked = 0;
  let radioboxHits = 0;
  let streamMetadataHits = 0;

  for (const st of candidates) {
    const urls = extractUrlsFromStation(st);
    const radioBoxUrls = urls.filter(textHasRadioBox);
    const streamUrl = st.streamUrl || st.preferredStreamUrl || st.lastResolvedStreamUrl || null;

    console.log(`\nSTATION ${st.id} ${st.name || ''}`);

    // Wake stream metadata / ICY endpoint
    if (streamUrl) {
      const r = await requestUrl(streamUrl, 10000);
      checked++;
      const icyTitle = r.headers && (r.headers['icy-name'] || r.headers['icy-description'] || r.headers['icy-genre'] || r.headers['icy-metaint']);
      if (icyTitle) streamMetadataHits++;
      console.log(`STREAM status=${r.status} icy=${icyTitle ? 'yes' : 'no'} url=${streamUrl}`);
    }

    // Wake RadioBox public page / playlist pages where URLs exist
    for (const url of radioBoxUrls.slice(0, 2)) {
      const testUrls = [
        url,
        url.replace(/\/?$/, '/playlist/')
      ];

      for (const u of [...new Set(testUrls)]) {
        const r = await requestUrl(u, 12000);
        checked++;
        const np = extractNowPlaying(r.body || '');
        if (np) radioboxHits++;
        console.log(`RADIOBOX status=${r.status} nowPlaying=${np ? np.slice(0, 140) : 'none'} url=${u}`);
      }
    }

    // Avoid hammering external services
    await new Promise(res => setTimeout(res, 1200));
  }

  console.log(`done checked=${checked} streamMetadataHits=${streamMetadataHits} radioboxHits=${radioboxHits}`);
}

main()
  .catch(err => {
    console.error('WAKE_SCRIPT_ERROR', err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
NODE

echo "==== $(date -Is) RadioBox / now-playing metadata wake finished ====" >> "$LOG_FILE"

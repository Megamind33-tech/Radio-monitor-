# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Zambian Airplay Monitor ("Radio Pulse Monitor" / "MOSTIFY") — a radio station monitoring platform that scrapes ICY metadata from internet radio streams and identifies songs via a multi-tier audio fingerprint pipeline (local library → AcoustID → AudD → ACRCloud → MusicBrainz/iTunes/Deezer text catalog).

- **Backend**: TypeScript/Express (`server/main.ts`), Prisma ORM, SQLite (dev)
- **Frontend**: React 19, Vite, TailwindCSS 4 (`src/`)
- **System binaries required**: `ffmpeg`, `ffprobe`, `fpcalc` (libchromaprint-tools)

### Quick commands

| Action | Command |
|--------|---------|
| Dev server | `npm run dev` (serves on port 3000) |
| Lint / type-check | `npm run lint` |
| Unit tests | `npm run test:station-health && npm run test:stream-url-guard && npm run test:local-fingerprint && npm run test:audio-id-merge` |
| Full bootstrap (idempotent) | `bash scripts/setup_env.sh` |
| Build (production) | `npm run build` |

### Non-obvious caveats

- The dev server (`npm run dev`) uses `tsx` to run `server/main.ts` directly. It starts the Express API and the Vite dev middleware for the React frontend together on port 3000.
- The SQLite database lives at `prisma/dev.db`. If it gets corrupted, delete it and re-run `bash scripts/setup_env.sh` to recreate it.
- The `.env` file is created from `.env.example` automatically by `scripts/setup_env.sh`. No external secrets (Supabase, AcoustID keys, etc.) are required for the dashboard to start; the app gracefully degrades when API keys are absent.
- The monitor actively connects to live radio streams on startup. This means the dev server will produce stream connection logs/errors if stations are offline — this is normal and expected.
- Node.js 20.x is the target runtime. Install via `curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && apt-get install -y nodejs` if missing.
- System deps (`ffmpeg`, `ffprobe`, `fpcalc`) are needed for the audio fingerprint pipeline. Install: `apt-get install -y ffmpeg libchromaprint-tools`.
- Python 3 + `requirements.txt` are only needed for the standalone Python harvest/audit scripts, not for the Node dashboard.

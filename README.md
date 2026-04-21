# Radio Pulse Monitor - Production-grade MVP

A robust, full-stack service for monitoring online radio streams and identifying "now playing" tracks using ICY metadata extraction and AcoustID fingerprinting as an automated fallback.

## 🚀 Priority Order
1. **Stream Metadata**: Priority extraction using ICY/Shoutcast metadata.
2. **Audio Fingerprinting**: Short capture (20s) fallback when metadata is missing, stale, or untrusted.
3. **AcoustID matching**: High-confidence fingerprint matching.
4. **MusicBrainz Enrichment**: Data normalization and MBID enrichment.
5. **Persistence**: Structured logs and real-time state in PostgreSQL (SQLite used for local MVP).

## 🛠 Tech Stack
- **Backend**: Node.js, Express, TypeScript, Prisma (ORM), SQLite/Postgres.
- **Frontend**: React, Vite, Tailwind CSS, Lucide, Recharts, Framer Motion.
- **Processing**: FFmpeg + FFprobe, Chromaprint (fpcalc).
- **APIs**: AcoustID v2, MusicBrainz WS v2.

## 📦 Setup & Installation

### Prerequisites
- Node.js 18+
- FFmpeg and FFprobe installed on system PATH
- Chromaprint (fpcalc) installed on system PATH

### Local Environment
1. Clone the repository.
2. Install dependencies:
   ```bash
   npm install
   ```
3. Initialize the database:
   ```bash
   npx prisma generate
   npx prisma db push
   ```
4. Update `.env` with your API keys:
   - `ACOUSTID_API_KEY`: Get one at [acoustid.org](https://acoustid.org/applications).
   - `MUSICBRAINZ_USER_AGENT`: Set a custom one (e.g., `MyRadioApp/1.0.0 ( contact@example.com )`).

### Running the App
```bash
npm run dev
```
The server will be available at `http://localhost:3000`.

## ⚠️ Known Limitations
- **Non-Commercial Only**: Usage of public AcoustID and MusicBrainz services is strictly for non-commercial or development purposes as per their terms.
- **Catalog Coverage**: Public catalogs may lack coverage for local niche stations, unreleased tracks, or long-tail content.
- **Probabilistic Matching**: Fingerprinting from short clips is probabilistic; factors like noise, DJ talkover, or low bitrates can reduce confidence.
- **Rate Limits**: Service implements internal throttling to respect AcoustID (3 req/s) and MusicBrainz (1 req/s) limits.

## 🛣 Next Upgrade Path: Enterprise Scale
To transition from this MVP to a full commercial production service:
1. **Replace Public Backends**: Deploy a private AcoustID instance or a custom fingerprint database (Chromaprint/Echofon).
2. **PostgreSQL Migration**: Switch `prisma.schema` to `provider = "postgresql"` and use a managed DB service.
3. **Redis Task Queue**: Replace simple `node-cron` with BullMQ or Celery for better horizontal scaling across multiple worker nodes.
4. **Cloud Recording**: Move temporary audio storage to S3/GCS buckets if clip retention is required for legal/archival purposes.

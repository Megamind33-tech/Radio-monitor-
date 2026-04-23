#!/usr/bin/env bash
# Bootstrap a fresh working environment for the Zambian Airplay Monitor app.
#
# Safe to re-run. Does NOT touch .env if it already exists.
# - Installs Node dependencies (and regenerates Prisma client via postinstall)
# - Installs Python dependencies used by the Python harvest scripts
# - Ensures the SQLite DATABASE_URL file exists and has all Prisma migrations applied
#   (baselining pre-existing databases so prisma migrate deploy keeps working)
# - Runs the TypeScript lint / type-check and the small unit test scripts
#
# Usage: bash scripts/setup_env.sh

set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Using Node $(node -v) and npm $(npm -v)"

if [ ! -f .env ]; then
  echo "==> No .env found; copying .env.example -> .env"
  cp .env.example .env
fi

# shellcheck disable=SC1091
set -a; . ./.env; set +a

echo "==> Installing Node dependencies (generates Prisma client)"
npm install --no-fund --no-audit

if [ -f requirements.txt ]; then
  echo "==> Installing Python dependencies"
  python3 -m pip install --quiet --user -r requirements.txt || true
fi

SQLITE_PATH=""
case "${DATABASE_URL:-}" in
  file:*)
    rel="${DATABASE_URL#file:}"
    case "$rel" in
      /*) SQLITE_PATH="$rel" ;;
      *)  SQLITE_PATH="prisma/${rel#./}" ;;
    esac
    ;;
esac

if [ -n "$SQLITE_PATH" ]; then
  echo "==> SQLite DATABASE_URL resolves to $SQLITE_PATH"
  mkdir -p "$(dirname "$SQLITE_PATH")"
  if [ ! -s "$SQLITE_PATH" ]; then
    echo "==> Creating fresh SQLite database via prisma db push"
    # The repo's first migration is a table redefine migration that assumes
    # the base tables already exist. For a brand-new DB, push the full schema
    # first and then mark every migration as applied.
    npx prisma db push --accept-data-loss >/dev/null
    for dir in prisma/migrations/*/; do
      name="$(basename "$dir")"
      npx prisma migrate resolve --applied "$name" >/dev/null 2>&1 || true
    done
  else
    echo "==> Existing SQLite database detected — ensuring migrations are tracked"
    # Attempt a normal deploy first; if Prisma reports P3005 (non-empty, unbaselined),
    # baseline every migration as already applied then re-run.
    if ! npx prisma migrate deploy 2>deploy.err; then
      if grep -q "P3005" deploy.err; then
        echo "==> Baselining existing database (marking all migrations as applied)"
        for dir in prisma/migrations/*/; do
          name="$(basename "$dir")"
          npx prisma migrate resolve --applied "$name" >/dev/null 2>&1 || true
        done
        # Then reconcile any ALTER-TABLE columns that the schema expects but may be
        # missing in an older sqlite file (common when the dev.db was baselined
        # without the additive migrations applied first).
        echo "==> Re-syncing schema with the database (prisma db push)"
        npx prisma db push --accept-data-loss >/dev/null
      else
        cat deploy.err
        rm -f deploy.err
        exit 1
      fi
    fi
    rm -f deploy.err
  fi
fi

echo "==> Type checking the project"
npm run lint

echo "==> Running unit test scripts"
npm run test:station-health
npm run test:stream-url-guard

echo
echo "Environment is ready. Start the dev server with: npm run dev"

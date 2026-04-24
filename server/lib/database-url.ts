import fs from "fs";
import path from "path";

/**
 * SQLite `file:` URLs are resolved relative to the process cwd. A common mistake is
 * `DATABASE_URL=file:./dev.db` at repo root while the real populated DB lives at
 * `prisma/dev.db`, leaving an empty `dev.db` at the root and an empty Station table.
 * If the configured file is missing or zero bytes and `prisma/dev.db` has content,
 * prefer the prisma path so local dev and agents see the same catalog.
 */
export function resolveDatabaseUrl(raw: string | undefined): string {
  const fallback = "file:./prisma/dev.db";
  const urlStr = (raw && String(raw).trim()) || fallback;

  if (!urlStr.startsWith("file:")) {
    return urlStr;
  }

  const withoutProtocol = urlStr.slice("file:".length);
  const abs =
    withoutProtocol.startsWith("/") || /^[a-zA-Z]:/.test(withoutProtocol)
      ? withoutProtocol
      : path.resolve(process.cwd(), withoutProtocol.replace(/^\.\//, ""));

  let st: fs.Stats | null = null;
  try {
    st = fs.statSync(abs);
  } catch {
    st = null;
  }

  const prismaDev = path.join(process.cwd(), "prisma", "dev.db");
  let prismaSt: fs.Stats | null = null;
  try {
    prismaSt = fs.statSync(prismaDev);
  } catch {
    prismaSt = null;
  }

  const configuredEmpty = !st || st.size === 0;
  const prismaHasData = prismaSt && prismaSt.size > 0;
  if (configuredEmpty && prismaHasData && path.normalize(abs) !== path.normalize(prismaDev)) {
    return `file:${prismaDev}`;
  }

  return urlStr;
}

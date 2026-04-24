/**
 * Parse "feat." / "ft." style credits from a combined artist string or title.
 * Returns primary billing name, optional clean title, and featured names.
 */

const FEAT_SPLIT =
  /\b(?:feat\.?|ft\.?|featuring|with)\s+/i;

export function parseFeaturedFromArtist(artistLine: string | null | undefined): {
  primaryArtist: string;
  featured: string[];
} {
  const raw = String(artistLine ?? "").trim();
  if (!raw) return { primaryArtist: "", featured: [] };
  const parts = raw.split(FEAT_SPLIT);
  const primaryArtist = (parts[0] ?? "").replace(/\s+/g, " ").trim();
  const featured: string[] = [];
  for (let i = 1; i < parts.length; i++) {
    const seg = (parts[i] ?? "")
      .split(/[,;&]|(?=\s+vs\.?\s+)/i)
      .map((s) => s.replace(/\s+/g, " ").trim())
      .filter(Boolean);
    for (const name of seg) {
      if (name.length >= 2 && !featured.includes(name)) featured.push(name);
    }
  }
  return { primaryArtist: primaryArtist || raw, featured };
}

export function titleWithoutFeaturing(title: string | null | undefined): string {
  const t = String(title ?? "").trim();
  if (!t) return "";
  const cut = t.split(FEAT_SPLIT)[0]?.trim() ?? t;
  return cut.replace(/\s*\(\s*$/u, "").trim() || t;
}

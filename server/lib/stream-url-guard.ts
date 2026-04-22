export type StreamUrlValidationResult = {
  accepted: boolean;
  reason: string;
  canonicalUrl: string;
};

function normalize(url: string): string {
  return String(url || "").trim();
}

const BAD_HOST_HINTS = [
  "localhost",
  "127.0.0.1",
  "0.0.0.0",
];

const BAD_PATH_HINTS = [
  "/search",
  "/discover",
  "/ads",
];

export function validateCandidateStreamUrl(url: string): StreamUrlValidationResult {
  const canonicalUrl = normalize(url);
  if (!canonicalUrl.startsWith("http")) {
    return { accepted: false, reason: "non_http_url", canonicalUrl };
  }
  try {
    const u = new URL(canonicalUrl);
    const host = u.hostname.toLowerCase();
    const path = u.pathname.toLowerCase();
    if (BAD_HOST_HINTS.includes(host)) {
      return { accepted: false, reason: "invalid_host", canonicalUrl };
    }
    if (BAD_PATH_HINTS.some((x) => path.includes(x))) {
      return { accepted: false, reason: "non_stream_path", canonicalUrl };
    }
    return { accepted: true, reason: "ok", canonicalUrl };
  } catch {
    return { accepted: false, reason: "invalid_url", canonicalUrl };
  }
}


type DetectionLike = {
  manuallyTagged?: boolean | null;
  sourceProvider?: string | null;
  verifiedTrackId?: string | null;
  titleFinal?: string | null;
  artistFinal?: string | null;
  manualTaggedAt?: Date | null;
};

export function isHumanVerifiedDetectionLog(log: DetectionLike | null | undefined): boolean {
  if (!log) return false;
  return Boolean(
    log.manuallyTagged === true ||
      log.sourceProvider === "human_review" ||
      (log.verifiedTrackId && String(log.verifiedTrackId).trim())
  );
}

/**
 * Prevent weak/automatic updates from overwriting human-reviewed royalty fields.
 * Manual update paths should pass `isManualOverride=true`.
 */
export function buildSafeDetectionLogUpdate(
  existing: DetectionLike,
  incoming: Record<string, unknown>,
  isManualOverride = false
): Record<string, unknown> {
  if (isManualOverride || !isHumanVerifiedDetectionLog(existing)) {
    return incoming;
  }

  const out = { ...incoming };
  delete out.titleFinal;
  delete out.artistFinal;
  delete out.verifiedTrackId;
  if (
    typeof existing.sourceProvider === "string" &&
    existing.sourceProvider.trim().toLowerCase() === "human_review"
  ) {
    out.sourceProvider = "human_review";
  }
  if (existing.manuallyTagged === true) {
    out.manuallyTagged = true;
    out.manualTaggedAt = existing.manualTaggedAt ?? out.manualTaggedAt;
  }
  if (existing.verifiedTrackId) {
    out.verifiedTrackId = existing.verifiedTrackId;
  }
  return out;
}

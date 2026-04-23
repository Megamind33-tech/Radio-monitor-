-- Track when the current stream text (ICY metadata) was first observed.
-- Used as the staleness anchor instead of updatedAt, which resets on every poll.
-- Without this field the stale timer was ineffective because updatedAt was
-- refreshed on every sameSpin guard touch, so stations with unchanged ICY
-- never triggered re-fingerprinting and stayed "stuck" on the same song.
ALTER TABLE "CurrentNowPlaying" ADD COLUMN "streamTextChangedAt" DATETIME;

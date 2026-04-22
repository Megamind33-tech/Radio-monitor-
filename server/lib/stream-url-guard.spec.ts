import { validateCandidateStreamUrl } from "./stream-url-guard.js";

function assert(condition: boolean, message: string) {
  if (!condition) throw new Error(message);
}

function run() {
  const ok = validateCandidateStreamUrl("https://stream.zeno.fm/abcd1234");
  assert(ok.accepted, "valid stream URL should be accepted");

  const badHost = validateCandidateStreamUrl("http://localhost:8000/live");
  assert(!badHost.accepted, "localhost URL should be rejected");

  const badPath = validateCandidateStreamUrl("https://example.com/search?q=stream");
  assert(!badPath.accepted, "search/discovery URL should be rejected");

  const badProtocol = validateCandidateStreamUrl("file:///tmp/audio.mp3");
  assert(!badProtocol.accepted, "non-http URL should be rejected");

  console.log("stream-url-guard.spec: ok");
}

run();

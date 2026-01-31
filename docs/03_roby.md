You are GPT-5.2 Thinking, acting as a senior Rust video/networking engineer.

Context:
- Project: `sentinel_rtp_cam`
- Input: RTSP SETUP/PLAY, receiving H264 over RTP via UDP (and sometimes TCP interleaved). ONVIF is used for motion events.
- Current behavior: on motion, code waits for an IDR and writes a short clip. We currently try to produce MP4 via ffmpeg, but ffmpeg is too heavy on Raspberry Pi and often leaves `.part` files unfinished if interrupted/crashes.
- Constraints:
  - DO NOT add any extra always-on services besides our Rust binary (no MediaMTX, no Docker).
  - Raspberry Pi is resource constrained; prefer zero/low CPU work.
  - Reliability is more important than fancy output containers.
  - Clips must be decodable and should avoid “datamosh”, “non-existing PPS”, or corrupted frames.

Goal (change request):
- Remove ffmpeg from the Raspberry Pi clip pipeline.
- On the Pi, record motion clips in a crash-tolerant format that requires no finalization:
  - Preferred: raw H264 Annex-B `.h264` (start codes 00 00 00 01) with sidecar JSON metadata.
  - Optional alternative: MPEG-TS `.ts` segments if we decide it’s worth the extra mux work.
- On the backend (cloud), we can later remux `.h264` into `.mp4` for browser playback, but Pi must not do container finalization.

Hard technical requirements (must implement):
1) Keyframe alignment:
   - A new clip must start at a clean access unit:
     - Must have SPS + PPS available, then start at an IDR (NAL type 5).
   - Always write SPS/PPS before the first IDR of a clip.
   - Maintain cached SPS/PPS:
     - Prefer to parse from SDP `fmtp` `sprop-parameter-sets` if present, not only in-band.
     - Also update cache when in-band SPS (7) / PPS (8) appear.

2) RTP robustness (this is likely why we see “datamosh” / unplayable dumps):
   - Handle packet loss and reordering:
     - Track RTP sequence numbers.
     - If a sequence gap occurs, reset FU-A assembly and drop partial NAL state.
     - Mark stream “unsynced” and drop output until next SPS+PPS+IDR.
   - Implement STAP-A (NAL type 24) aggregation unpacking.
   - Keep the implementation minimal and correct. Do not attempt full RTCP timing; just enough to avoid corrupted NALs.

3) File safety:
   - Write to `*.part` while recording.
   - Flush periodically.
   - On successful clip end, close and atomically rename to final `.h264`.
   - On startup, scan clip dir and delete stale `.part` older than a threshold.
   - Clip length: use a configured duration (pre/post roll rules optional), but do not use size-based truncation.

4) Maintain current architecture:
   - Keep existing modules: `rtsp`, `rtp`, `h264_depacketize`, `sdp`, `onvif_motion`.
   - Provide a small “clip writer” module that can be used by both UDP and TCP-interleaved receivers.

What to produce:
A) A clear refactor plan (steps), explaining what changes go where.
B) A concrete list of new/updated Rust types and functions (signatures).
C) Pseudocode or real Rust snippets for:
   - STAP-A unpacking
   - loss detection + depacketizer reset
   - clip gating logic (SPS/PPS/IDR sync gate)
   - safe `.part` → final rename flow
D) A short test plan:
   - unit tests for depacketizer (FU-A, STAP-A, single NAL)
   - simulated packet loss/reorder tests
   - integration test using ffmpeg testsrc → RTSP server (e.g. mediamtx) on dev machine ONLY (not on Pi), then run our receiver and validate produced `.h264` plays with ffplay.

Important style rules:
- Be explicit about NAL types and how to parse them from Annex-B.
- Use `anyhow` for errors as the project already does.
- Prefer straightforward, maintainable code over micro-optimizations.
- Do not suggest adding a permanent external service on the Pi.
- Ask no more than 1 clarifying question; if unclear, choose sensible defaults and proceed.

Start by summarizing the design change in 5–10 bullet points, then deliver (A)–(D).

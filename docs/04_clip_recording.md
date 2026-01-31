You are Codex (GPT-5.2 level). Implement the “Pi Agent → Server” RTP forwarding architecture in Rust in the existing repo `sentinel_rtp_cam`.

High-level goals:
- Raspberry Pi Agent connects to camera via RTSP, receives H264 RTP (UDP or TCP interleaved), and forwards packets to a cloud server over ONE persistent reliable TCP connection.
- Server accepts agent connections, routes packets by stream_id, depacketizes H264 from RTP, maintains SPS/PPS cache + sync gate, buffers recent access units for preroll, and assembles motion-triggered clips as raw Annex-B .h264 with atomic .part→final rename.
- No ffmpeg on Pi. No always-on 3rd party services.

Constraints:
- Keep changes minimal and incremental. Prefer small modules with clear ownership.
- Use tokio + anyhow already in repo.
- Must support multiple cameras (multiple stream_id) concurrently.
- Protocol must be explicit and framed (length-delimited) to avoid partial reads.
- Do not introduce new heavy dependencies; small ones are fine if needed.

PART 0 — Repository assumptions
- There is an existing crate `sentinel_rtp_cam` with modules: rtsp, rtp, sdp, h264_depacketize, interleaved, onvif_motion (or similar).
- There is an EventBus system used by ONVIF poller.
If these do not exist, create minimal versions consistent with current code style.

PART 1 — Implement a framed TCP wire protocol (Pi→Server)
Create `src/proto.rs`:
- Define a fixed 16-byte header:
  - magic u32 = 0x534E5450
  - version u16 = 1
  - msg_type u16
  - stream_id u32
  - len u32 (payload length)
- Implement async encode/decode:
  - `async fn write_msg<W: AsyncWriteExt + Unpin>(w: &mut W, msg: &Msg) -> Result<()>`
  - `async fn read_msg<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<Msg>`
- Msg types:
  - HELLO=1 (payload JSON string bytes)
  - RTP=2 (payload raw RTP packet bytes)
  - GAP=3 (payload struct: last_seq u16, new_seq u16)
  - MOTION=4 (payload JSON string bytes)
  - PING=5, PONG=6 (optional)
- Validate magic/version; enforce max len (e.g. 256KB).
- Provide helpers to parse GAP payload.

PART 2 — Agent uplink client (persistent connection with reconnect)
Create `src/agent_uplink.rs`:
- `struct Uplink { server_addr: String, token: String, agent_id: String, stream_map: HashMap<u32,String> }`
- `async fn connect_and_run(...)` that:
  - opens TcpStream to server
  - sends HELLO JSON {agent_id, token, streams:[{stream_id,name}]}
  - exposes async methods:
    - `send_rtp(stream_id, rtp_bytes)`
    - `send_gap(stream_id, last_seq, new_seq)`
    - `send_motion(stream_id, rule, active, ts)`
  - reconnects on failure with exponential backoff
- Ensure calls are non-blocking: use an mpsc channel so camera receive loops do not await network writes directly.
- Add periodic PING with simple stats.

PART 3 — Agent camera RTSP ingest and forwarding
Create `src/bin/agent_forward.rs` (new binary):
- Reads config from env:
  - SERVER_ADDR, AGENT_TOKEN, AGENT_ID
  - CAM1_RTSP_URL, CAM1_STREAM_ID, optional CAM1_ONVIF creds
  - same for CAM2...
- For each camera:
  - connect RTSP (reuse existing RtspClient)
  - choose transport:
    - default: TCP interleaved if possible
    - allow env override to UDP
  - receive RTP packets:
    - if TCP interleaved: use existing read_interleaved_frame
    - if UDP: recv_from on socket
  - forward each raw RTP packet bytes using uplink.send_rtp(stream_id,...)
  - implement UDP seq gap detection:
    - track expected_seq
    - if mismatch: uplink.send_gap(...)
- Also integrate existing ONVIF poller:
  - on motion edge: uplink.send_motion(stream_id,...)
  - If ONVIF is per-camera, map motion to correct stream_id.
If ONVIF integration is too big, stub MOTION forwarding with a minimal placeholder event generator and add TODO.

PART 4 — Server listener and routing
Create `src/bin/server_ingest.rs` (new binary):
- Listens on TCP (SERVER_BIND env default 0.0.0.0:9000)
- Reads HELLO, validates token (SERVER_TOKEN env); disconnect if invalid.
- Maintains `HashMap<u32, StreamHandle>` where StreamHandle is an mpsc sender into per-stream pipeline task.
- For each RTP msg: forward payload bytes to pipeline.
- For each GAP msg: forward gap signal to pipeline.
- For each MOTION msg: forward motion signal to pipeline.

PART 5 — Server per-stream pipeline: depacketize + sync gate + ring buffer + clip writer
Create `src/server_pipeline.rs` with:
- `enum StreamMsg { Rtp(Vec<u8>), Gap{last:u16,new:u16}, Motion{rule:String,active:bool,ts:String} }`
- `struct StreamState { dep: H264Depacketizer, synced: bool, sps: Option<Vec<u8>>, pps: Option<Vec<u8>>, ring: RingBuffer<AccessUnit>, clip: Option<ClipWriterState>, expected_seq: Option<u16> }`
- Parse RTP using existing RtpPacket::parse.
- On seq discontinuity or Gap msg:
  - dep.reset(); synced=false; expected_seq=None; drop until SPS+PPS+IDR.
- Implement STAP-A if not already:
  - depacketizer should output individual Annex-B NALs.
- Access unit grouping:
  - use RTP marker bit if available: collect NALs until marker true then emit AccessUnit.
  - If marker not reliable, still push NALs through but clip cutting should start at IDR AU boundary.
- SPS/PPS cache:
  - Update on NAL 7/8
  - If SDP parsing is available, allow injecting SPS/PPS from fmtp sprop-parameter-sets (optional; TODO acceptable).
- Sync gate:
  - do nothing until sps+pps present and an IDR AU appears.
  - when syncing, prepend sps+pps before first IDR.
- Ring buffer:
  - keep last N seconds worth of AccessUnits (store by arrival Instant).
  - implement minimal ring with VecDeque; prune by time.
- Clip writer:
  - When Motion(active=true): start clip if not started:
    - create file path under CLIP_DIR/stream_id
    - write `.h264.part`
    - write preroll from ring buffer (but ensure first AU begins with SPS+PPS+IDR; if preroll begins mid-GOP, drop until next IDR inside preroll window or just start at first IDR after trigger)
    - then continue writing AUs for fixed duration POST_ROLL_SECS (env)
  - When clip ends: close + rename `.part` to `.h264` atomically.
  - Also write a `.json` sidecar with metadata (stream_id, start/end times, rule, etc.).
- Provide “startup cleanup”: delete stale `.part` older than STALE_PART_SECS.

PART 6 — HTTP API (optional but small)
If easy, add a minimal `src/bin/server_http.rs` or integrate into server_ingest:
- List clips, download `.h264` and `.json`.
No browser streaming yet.

PART 7 — Tests
- Unit tests for proto framing encode/decode.
- Unit tests for depacketizer STAP-A + FU-A + loss reset behavior (simulate sequence gaps).
- Integration test instructions (dev machine only): use mediamtx + ffmpeg testsrc to push RTSP, run agent_forward + server_ingest, verify produced `.h264` plays with ffplay.

Deliverables:
- New files: src/proto.rs, src/agent_uplink.rs, src/server_pipeline.rs
- New bins: src/bin/agent_forward.rs, src/bin/server_ingest.rs
- Update Cargo.toml bins if needed.
- Provide README section: how to run agent and server with env vars.

Implementation notes:
- Keep logs concise; add ONVIF_DEBUG support if present.
- Protect against unbounded memory: cap ring buffer duration and message sizes.
- Use `tokio::select!` in stream pipeline to handle timers for clip expiration.
- Follow existing code style and error handling with anyhow::Result.

Proceed to implement now. Output only code diffs (file contents) and brief run instructions at the end.

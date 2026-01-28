# spec/requirements.md — Protocol requirement matrix (template)

> Use this file as the single checklist that proves spec compliance.
> For each row: add the exact spec section (and page for ONVIF PDF), the code location, and at least one test that enforces it.

## Legend
- **Level**: MUST / SHOULD / MAY
- **Status**: TODO / IN-PROGRESS / DONE
- **Spec Ref**: `RFC#### §x.y` or `ONVIF Core v25.12 p.<page> §<section>` (use the copies in `docs/specs/`)
- **Proof**: test name(s) or interop vector(s)

---

## 1) RTSP (RFC 2326) — Core session + message handling

| ID | Level | Requirement | Spec Ref | Code Location | Proof (tests/vectors) | Status |
|---|---|---|---|---|---|---|
| RTSP-MSG-001 | MUST | Parse RTSP start-line for requests and responses; reject malformed lines. | RFC2326 §<fill> | `src/rtsp/parser.*` | `tests/unit/rtsp/test_start_line.*` | TODO |
| RTSP-MSG-002 | MUST | Header names are case-insensitive; allow OWS around `:`; preserve header values exactly. | RFC2326 §<fill> | `src/rtsp/headers.*` | `tests/unit/rtsp/test_headers_case_ows.*` | TODO |
| RTSP-MSG-003 | MUST | Respect `Content-Length`: read exactly N bytes of body; reject mismatch/underflow/overflow. | RFC2326 §<fill> | `src/rtsp/io.*` | `tests/unit/rtsp/test_content_length.*` | TODO |
| RTSP-CSEQ-001 | MUST | Echo `CSeq` in responses; client increments CSeq per request. | RFC2326 §<fill> | `src/rtsp/session.*` | `tests/unit/rtsp/test_cseq_echo.*` | TODO |
| RTSP-SESS-001 | MUST | After SETUP, include `Session` header in subsequent requests; keep stable until TEARDOWN/timeout. | RFC2326 §<fill> | `src/rtsp/session.*` | `tests/interop/rtsp/test_session_lifecycle.*` | TODO |
| RTSP-METHOD-001 | MUST | Implement OPTIONS and respond with supported methods (Public header if used). | RFC2326 §<fill> | `src/rtsp/server.*` | `spec/vectors/rtsp/options.txt` + checker | TODO |
| RTSP-METHOD-002 | MUST | Implement DESCRIBE: return SDP with correct Content-Type and Content-Length. | RFC2326 §<fill> | `src/rtsp/server.*` | `tests/interop/rtsp/test_describe_sdp.*` | TODO |
| RTSP-METHOD-003 | MUST | Implement SETUP: parse Transport; establish RTP/RTCP transport parameters. | RFC2326 §<fill> | `src/rtsp/transport.*` | `tests/unit/rtsp/test_transport_parse.*` | TODO |
| RTSP-METHOD-004 | MUST | Implement PLAY to start streaming for a Session and track(s). | RFC2326 §<fill> | `src/rtsp/server.*` | `tests/interop/rtsp/test_play_streams.*` | TODO |
| RTSP-METHOD-005 | MUST | Implement TEARDOWN to stop streaming and release session resources. | RFC2326 §<fill> | `src/rtsp/server.*` | `tests/interop/rtsp/test_teardown.*` | TODO |
| RTSP-URI-001 | SHOULD | Build correct track URLs using SDP `a=control:` (relative/absolute handling). | RFC2326 §<fill> + RFC4566 §<fill> | `src/rtsp/url.*` | `tests/unit/rtsp/test_control_url.*` | TODO |

### RTSP Transport (UDP)
| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| RTSP-TR-UDP-001 | MUST | Parse `Transport: RTP/AVP;unicast;client_port=RTP-RTCP` and validate port pairs. | RFC2326 §<fill> | `src/rtsp/transport.*` | `tests/unit/rtsp/test_transport_udp_client_port.*` | TODO |
| RTSP-TR-UDP-002 | SHOULD | Support `server_port` in response (if acting as server) and store mapping. | RFC2326 §<fill> | `src/rtsp/transport.*` | `tests/unit/rtsp/test_transport_udp_server_port.*` | TODO |

### RTSP Interleaved (RTP/RTCP over TCP)
| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| RTSP-TR-TCP-001 | MUST | Implement `$` interleaved framing: `$ <channel> <len:16> <payload>`. | RFC2326 §<fill> | `src/rtsp/interleaved.*` | `tests/unit/rtsp/test_interleaved_framing.*` | TODO |
| RTSP-TR-TCP-002 | MUST | Validate `len` boundaries; reject truncated/oversized frames safely. | RFC2326 §<fill> | `src/rtsp/interleaved.*` | `tests/property/rtsp/test_interleaved_bounds.*` | TODO |
| RTSP-TR-TCP-003 | SHOULD | Map channels to RTP vs RTCP according to SETUP `interleaved=a-b`. | RFC2326 §<fill> | `src/rtsp/transport.*` | `tests/unit/rtsp/test_interleaved_channel_map.*` | TODO |

---

## 2) SDP (RFC 4566) — Parsing + media mapping

| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| SDP-CORE-001 | MUST | Line-based parse; accept `\r\n` and `\n`; ignore trailing whitespace safely. | RFC4566 §<fill> | `src/sdp/parser.*` | `tests/unit/sdp/test_line_endings.*` | TODO |
| SDP-CORE-002 | MUST | Support multiple `m=` sections; scope `a=` attributes to session vs media level. | RFC4566 §<fill> | `src/sdp/model.*` | `tests/unit/sdp/test_media_scoping.*` | TODO |
| SDP-ATTR-001 | MUST | Parse `a=rtpmap:<pt> <enc>/<clock>[/<ch>]` and associate with PT. | RFC4566 §<fill> | `src/sdp/attrs.*` | `tests/unit/sdp/test_rtpmap.*` | TODO |
| SDP-ATTR-002 | MUST | Parse `a=fmtp:<pt> ...` and associate with PT. | RFC4566 §<fill> | `src/core/sdp.rs` | `src/core/sdp.rs::tests::parses_sprop_parameter_sets` | IN-PROGRESS |
| SDP-ATTR-003 | SHOULD | Preserve unknown `a=` attributes for roundtrip. | RFC4566 §<fill> | `src/sdp/parser.*` | `tests/unit/sdp/test_unknown_attrs_roundtrip.*` | TODO |
| SDP-CTRL-001 | MUST | Parse `a=control:` and expose value to RTSP layer (track URL composition). | RFC4566 §<fill> | `src/sdp/attrs.*` | `tests/unit/sdp/test_control_attr.*` | TODO |

---

## 3) RTP/RTCP Core — Packet parsing/encoding + invariants

> Back this section with your local core RTP/RTCP spec (typically RFC 3550). If not present, add to `docs/specs/`.

### RTP
| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| RTP-HDR-001 | MUST | Parse/encode RTP header fields correctly (V/P/X/CC/M/PT/seq/timestamp/SSRC/CSRC). | RFC3550 §<fill> | `src/rtp/packet.*` | `tests/unit/rtp/test_header_roundtrip.*` | TODO |
| RTP-HDR-002 | MUST | Enforce V=2; reject other versions. | RFC3550 §<fill> | `src/rtp/packet.*` | `tests/unit/rtp/test_version.*` | TODO |
| RTP-HDR-003 | MUST | Validate CC vs CSRC list length; reject truncation/overflow. | RFC3550 §<fill> | `src/rtp/packet.*` | `tests/property/rtp/test_csrc_bounds.*` | TODO |
| RTP-EXT-001 | MUST | If X=1 parse header extension and validate length (32-bit words). | RFC3550 §<fill> | `src/rtp/extensions.*` | `tests/unit/rtp/test_extension_parse.*` | TODO |
| RTP-PAD-001 | MUST | If P=1 validate padding length; ensure packet length is consistent. | RFC3550 §<fill> | `src/rtp/packet.*` | `tests/property/rtp/test_padding_bounds.*` | TODO |
| RTP-SEQ-001 | MUST | Handle 16-bit sequence wraparound in receiver reordering logic. | RFC3550 §<fill> | `src/rtp/reorder.*` | `tests/unit/rtp/test_seq_wrap.*` | TODO |

### RTCP
| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| RTCP-CORE-001 | MUST | Parse compound RTCP packets (multiple RTCP packets per datagram). | RFC3550 §<fill> | `src/rtcp/parser.*` | `tests/unit/rtcp/test_compound_parse.*` | TODO |
| RTCP-LEN-001 | MUST | Validate RTCP length field (32-bit words) and bounds; reject malformed packets. | RFC3550 §<fill> | `src/rtcp/parser.*` | `tests/property/rtcp/test_length_bounds.*` | TODO |
| RTCP-TYPES-001 | SHOULD | Support SR and RR parsing/encoding if RTCP is enabled. | RFC3550 §<fill> | `src/rtcp/types.*` | `tests/unit/rtcp/test_sr_rr.*` | TODO |
| RTCP-SDES-001 | SHOULD | Support SDES parsing/encoding for compound packets. | RFC3550 §<fill> | `src/rtcp/types.*` | `tests/unit/rtcp/test_sdes.*` | TODO |

---

## 4) H.264 over RTP (RFC 6184)

| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| H264-SINGLE-001 | MUST | Support Single NAL Unit packetization and depacketization. | RFC6184 §<fill> | `src/core/h264_depacketize.rs` | `src/core/h264_depacketize.rs::tests::single_nal_packet` | IN-PROGRESS |
| H264-STAP-001 | SHOULD | Support STAP-A aggregation packet parsing (SPS/PPS bundling common). | RFC6184 §<fill> | `src/core/h264_depacketize.rs` | `src/core/h264_depacketize.rs::tests::stap_a_unpacking` | IN-PROGRESS |
| H264-FUA-001 | MUST | Support FU-A fragmentation; reconstruct byte-exact NAL units. | RFC6184 §<fill> | `src/core/h264_depacketize.rs` | `src/core/h264_depacketize.rs::tests::fu_a_reassembly` | IN-PROGRESS |
| H264-FUA-002 | MUST | If any FU-A fragment is missing, drop the whole reconstructed NAL (no partial output). | RFC6184 §<fill> | `src/core/h264_depacketize.rs` | `src/core/h264_depacketize.rs::tests::fu_a_continuation_without_start_is_error` | IN-PROGRESS |
| H264-SDP-001 | MUST | Parse fmtp: `packetization-mode`, `profile-level-id`, `sprop-parameter-sets`. | RFC6184 §<fill> | `src/core/sdp.rs` | `src/core/sdp.rs::tests::parses_sprop_parameter_sets` | IN-PROGRESS |

---

## 5) HEVC over RTP (RFC 7798)

| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| HEVC-SINGLE-001 | MUST | Support Single NAL Unit packetization and depacketization. | RFC7798 §<fill> | `src/rtp/hevc.*` | `spec/vectors/hevc/single_nal.json` + test | TODO |
| HEVC-AP-001 | SHOULD | Support AP aggregation packet parsing. | RFC7798 §<fill> | `src/rtp/hevc_ap.*` | `spec/vectors/hevc/ap.json` + test | TODO |
| HEVC-FU-001 | MUST | Support FU fragmentation; reconstruct byte-exact NAL units. | RFC7798 §<fill> | `src/rtp/hevc_fu.*` | `spec/vectors/hevc/fu.json` + test | TODO |
| HEVC-FU-002 | MUST | If FU fragments missing, drop the reconstructed NAL (no partial output). | RFC7798 §<fill> | `src/rtp/hevc_fu.*` | `tests/unit/hevc/test_fu_loss_drop.*` | TODO |
| HEVC-SDP-001 | SHOULD | Parse HEVC fmtp parameters used by target devices and bind to PT. | RFC7798 §<fill> | `src/sdp/fmtp_hevc.*` | `tests/unit/sdp/test_hevc_fmtp.*` | TODO |

---

## 6) ONVIF interoperability (Core Spec)

| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| ONVIF-RTSP-001 | MUST | Support ONVIF-typical RTSP flow: DESCRIBE→SETUP (per track)→PLAY. | ONVIF Core v25.12 p.<fill> §<fill> | `src/rtsp/*` | `tests/interop/onvif/test_onvif_flow.*` | TODO |
| ONVIF-AUTH-001 | SHOULD | Support Digest authentication as used by ONVIF devices (incl. SHA-256 where required). | ONVIF Core v25.12 p.<fill> §<fill> | `src/rtsp/auth_digest.*` | `tests/interop/onvif/test_digest_auth.*` | TODO |
| ONVIF-KEEPALIVE-001 | SHOULD | Implement GET_PARAMETER keepalive and handle server keepalive expectations. | ONVIF Core v25.12 p.<fill> §<fill> | `src/rtsp/keepalive.*` | `tests/interop/onvif/test_keepalive.*` | TODO |

---

## 7) End-to-end acceptance (merge gate)

| ID | Level | Requirement | Spec Ref | Code Location | Proof | Status |
|---|---|---|---|---|---|---|
| E2E-RTSP-UDP-001 | MUST | End-to-end session over UDP: OPTIONS, DESCRIBE, SETUP, PLAY, receive RTP/RTCP, TEARDOWN. | RFC2326 + RFC4566 | `tests/interop/rtsp/*` | `tests/interop/rtsp/test_e2e_udp.*` | TODO |
| E2E-RTSP-TCP-001 | SHOULD | End-to-end session over TCP interleaved with `$` framing. | RFC2326 | `tests/interop/rtsp/*` | `tests/interop/rtsp/test_e2e_tcp_interleaved.*` | TODO |
| E2E-NET-ROBUST-001 | MUST | Under loss/reorder/dup simulation, no crash/hang; bounded memory; no partial-frame output. | RFC3550 + payload RFC | `tests/interop/net_sim/*` | `tests/interop/test_network_impairments.*` | TODO |

---

## 8) Notes / decision log pointers
- Record ambiguities and interoperability decisions in `spec/notes.md`.
- Every deviation or “tolerant parser” rule must be justified with a spec reference and a test.

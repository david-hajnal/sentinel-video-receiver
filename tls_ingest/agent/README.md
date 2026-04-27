# TLS ingest agent (local-lab tool)

This binary is intended for local development and protocol testing.

## TLS requirements

- Certificate verification is always enabled.
- You must provide a CA certificate with `--ca` or `AGENT_CA_CERT`.
- `--insecure` is intentionally not supported.

## Usage

```bash
cargo run -p ingest_agent -- \
  --server 127.0.0.1:7443 \
  --agent-id dev-agent \
  --token dev-token \
  --ca /path/to/ca.pem \
  --streams stream1,stream2
```

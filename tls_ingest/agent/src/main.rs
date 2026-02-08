use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ingest_proto::{read_record, write_record, ErrorPayload, EventPayload, Hello, HelloOk, RecordType};
use rand::RngCore;
use tokio::io::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

struct Settings {
    server_addr: String,
    agent_id: String,
    token: String,
    streams: Vec<String>,
    ca_cert: Option<PathBuf>,
    insecure: bool,
    ping_interval: u64,
    rtp_interval_ms: u64,
    event_interval_sec: u64,
}

struct StreamState {
    seq: u16,
    ts: u32,
    ssrc: u32,
    active: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let settings = load_settings()?;

    let (host, port) = split_host_port(&settings.server_addr)?;
    let server_name = make_server_name(&host)?;

    let tls_config = build_tls_config(settings.insecure, settings.ca_cert.as_deref())?;
    let connector = TlsConnector::from(Arc::new(tls_config));

    let tcp = TcpStream::connect(format!("{host}:{port}")).await?;
    let tls = connector.connect(server_name, tcp).await?;

    let (mut read_half, write_half) = tokio::io::split(tls);
    let writer = Arc::new(Mutex::new(write_half));
    let last_recv = Arc::new(Mutex::new(Instant::now()));

    let hello = Hello {
        agent_id: settings.agent_id.clone(),
        token: settings.token.clone(),
        streams: settings.streams.clone(),
        timestamp_unix: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64,
        nonce: random_nonce(),
    };
    let hello_payload = serde_json::to_vec(&hello)?;
    write_record(
        &mut *writer.lock().await,
        RecordType::Hello as u8,
        0,
        &hello_payload,
    )
    .await?;

    let hello_ok = wait_for_hello_ok(&mut read_half, settings.ping_interval).await?;
    info!(session_id = %hello_ok.session_id, "HELLO_OK received");

    let stop_flag = Arc::new(Mutex::new(false));
    spawn_read_loop(
        read_half,
        writer.clone(),
        last_recv.clone(),
        stop_flag.clone(),
        settings.ping_interval,
    );

    spawn_rtp_loop(writer.clone(), stop_flag.clone(), &settings)?;
    spawn_event_loop(writer.clone(), stop_flag.clone(), &settings)?;

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if *stop_flag.lock().await {
            break;
        }
    }

    Ok(())
}

async fn wait_for_hello_ok(
    read_half: &mut tokio::io::ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    ping_interval: u64,
) -> Result<HelloOk, Box<dyn std::error::Error>> {
    let record = timeout(
        Duration::from_secs(ping_interval * 3),
        read_record(read_half),
    )
    .await
    .map_err(|_| "timeout waiting for HELLO_OK")??;

    match record.record_type {
        x if x == RecordType::HelloOk as u8 => {
            let ok: HelloOk = serde_json::from_slice(&record.payload)?;
            Ok(ok)
        }
        x if x == RecordType::Error as u8 => {
            let err: ErrorPayload = serde_json::from_slice(&record.payload)?;
            Err(format!("server error: {}", err.message).into())
        }
        _ => Err("unexpected record before HELLO_OK".into()),
    }
}

fn spawn_read_loop(
    mut read_half: tokio::io::ReadHalf<tokio_rustls::client::TlsStream<TcpStream>>,
    writer: Arc<Mutex<WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>>>,
    last_recv: Arc<Mutex<Instant>>,
    stop_flag: Arc<Mutex<bool>>,
    ping_interval: u64,
) {
    tokio::spawn(async move {
        loop {
            let record = match timeout(
                Duration::from_secs(ping_interval * 3),
                read_record(&mut read_half),
            )
            .await
            {
                Ok(Ok(rec)) => rec,
                Ok(Err(e)) => {
                    warn!(error = %e, "read error");
                    break;
                }
                Err(_) => {
                    warn!("read timeout, closing");
                    break;
                }
            };

            *last_recv.lock().await = Instant::now();

            match record.record_type {
                x if x == RecordType::Ping as u8 => {
                    debug!("PING received");
                    let _ = write_record(
                        &mut *writer.lock().await,
                        RecordType::Ping as u8,
                        0,
                        &[],
                    )
                    .await;
                }
                x if x == RecordType::Close as u8 => {
                    info!("Server requested close");
                    break;
                }
                x if x == RecordType::Error as u8 => {
                    if let Ok(err) = serde_json::from_slice::<ErrorPayload>(&record.payload) {
                        warn!(code = %err.code, message = %err.message, "Server error");
                    }
                    break;
                }
                _ => {}
            }
        }

        *stop_flag.lock().await = true;
        let _ = write_record(&mut *writer.lock().await, RecordType::Close as u8, 0, &[]).await;
    });
}

fn spawn_rtp_loop(
    writer: Arc<Mutex<WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>>>,
    stop_flag: Arc<Mutex<bool>>,
    settings: &Settings,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream_state = HashMap::new();
    for stream in &settings.streams {
        stream_state.insert(
            stream.clone(),
            StreamState {
                seq: 0,
                ts: 0,
                ssrc: rand::random::<u32>(),
                active: false,
            },
        );
    }

    let streams = settings.streams.clone();
    let rtp_interval = settings.rtp_interval_ms;

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(rtp_interval));
        loop {
            ticker.tick().await;
            if *stop_flag.lock().await {
                break;
            }

            for stream in &streams {
                if let Some(state) = stream_state.get_mut(stream) {
                    let packet = build_rtp_packet(state);
                    if let Err(e) = write_record(
                        &mut *writer.lock().await,
                        RecordType::Rtp as u8,
                        0,
                        &packet,
                    )
                    .await
                    {
                        warn!(error = %e, "Failed to send RTP");
                        *stop_flag.lock().await = true;
                        return;
                    }
                }
            }
        }
    });

    Ok(())
}

fn spawn_event_loop(
    writer: Arc<Mutex<WriteHalf<tokio_rustls::client::TlsStream<TcpStream>>>>,
    stop_flag: Arc<Mutex<bool>>,
    settings: &Settings,
) -> Result<(), Box<dyn std::error::Error>> {
    let streams = settings.streams.clone();
    let interval = settings.event_interval_sec;
    let mut states: HashMap<String, bool> = streams.iter().map(|s| (s.clone(), false)).collect();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval));
        loop {
            ticker.tick().await;
            if *stop_flag.lock().await {
                break;
            }

            for stream in &streams {
                let next_state = !states.get(stream).copied().unwrap_or(false);
                states.insert(stream.clone(), next_state);

                let event = EventPayload {
                    stream_id: stream.clone(),
                    event_type: "motion".to_string(),
                    state: if next_state { "start".to_string() } else { "stop".to_string() },
                    event_ts_unix_ms: unix_millis(),
                    confidence: 0.9,
                };
                if let Ok(payload) = serde_json::to_vec(&event) {
                    let _ = write_record(
                        &mut *writer.lock().await,
                        RecordType::Event as u8,
                        0,
                        &payload,
                    )
                    .await;
                }
            }
        }
    });

    Ok(())
}

fn build_rtp_packet(state: &mut StreamState) -> Vec<u8> {
    let payload_len = 160usize;
    let mut buf = vec![0u8; 12 + payload_len];
    buf[0] = 0x80; // v=2
    buf[1] = 96; // payload type
    state.seq = state.seq.wrapping_add(1);
    state.ts = state.ts.wrapping_add(3000);
    buf[2..4].copy_from_slice(&state.seq.to_be_bytes());
    buf[4..8].copy_from_slice(&state.ts.to_be_bytes());
    buf[8..12].copy_from_slice(&state.ssrc.to_be_bytes());

    rand::thread_rng().fill_bytes(&mut buf[12..]);
    buf
}

fn unix_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(now) => (now.as_secs() as i64) * 1000 + (now.subsec_millis() as i64),
        Err(e) => {
            warn!("System clock error: {}, using -1 as timestamp", e);
            -1
        }
    }
}

fn random_nonce() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    base64_encode(&buf)
}

fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut i = 0;
    while i < input.len() {
        let b0 = input[i];
        let b1 = if i + 1 < input.len() { input[i + 1] } else { 0 };
        let b2 = if i + 2 < input.len() { input[i + 2] } else { 0 };

        let n0 = b0 >> 2;
        let n1 = ((b0 & 0x03) << 4) | (b1 >> 4);
        let n2 = ((b1 & 0x0f) << 2) | (b2 >> 6);
        let n3 = b2 & 0x3f;

        out.push(TABLE[n0 as usize] as char);
        out.push(TABLE[n1 as usize] as char);
        if i + 1 < input.len() {
            out.push(TABLE[n2 as usize] as char);
        } else {
            out.push('=');
        }
        if i + 2 < input.len() {
            out.push(TABLE[n3 as usize] as char);
        } else {
            out.push('=');
        }

        i += 3;
    }
    out
}

fn split_host_port(addr: &str) -> Result<(String, u16), Box<dyn std::error::Error>> {
    let mut parts = addr.rsplitn(2, ':');
    let port = parts
        .next()
        .ok_or("missing port")?
        .parse::<u16>()?;
    let host = parts.next().ok_or("missing host")?;
    Ok((host.to_string(), port))
}

fn make_server_name(host: &str) -> Result<tokio_rustls::rustls::pki_types::ServerName<'static>, Box<dyn std::error::Error>> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(tokio_rustls::rustls::pki_types::ServerName::IpAddress(ip.into()));
    }
    Ok(tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_string())?)
}

fn build_tls_config(
    insecure: bool,
    ca_cert: Option<&std::path::Path>,
) -> Result<tokio_rustls::rustls::ClientConfig, Box<dyn std::error::Error>> {
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    if !insecure {
        let ca_path = ca_cert.ok_or("--ca is required unless --insecure")?;
        let mut reader = std::io::BufReader::new(std::fs::File::open(ca_path)?);
        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()?;
        root_store.add_parsable_certificates(certs);
    }

    let mut config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    if insecure {
        config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoVerifier));
    }

    Ok(config)
}

#[derive(Debug)]
struct NoVerifier;

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error> {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        use tokio_rustls::rustls::SignatureScheme::*;
        vec![
            ECDSA_NISTP256_SHA256,
            ECDSA_NISTP384_SHA384,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            RSA_PKCS1_SHA256,
            RSA_PKCS1_SHA384,
            RSA_PKCS1_SHA512,
            ED25519,
        ]
    }
}

fn load_settings() -> Result<Settings, Box<dyn std::error::Error>> {
    let mut server_addr = None;
    let mut agent_id = None;
    let mut token = None;
    let mut streams = None;
    let mut ca_cert = None;
    let mut insecure = false;
    let mut ping_interval = 15u64;
    let mut rtp_interval_ms = 100u64;
    let mut event_interval_sec = 10u64;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--server" => server_addr = args.next(),
            "--agent-id" => agent_id = args.next(),
            "--token" => token = args.next(),
            "--streams" => streams = args.next(),
            "--ca" => ca_cert = args.next().map(PathBuf::from),
            "--insecure" => insecure = true,
            "--ping" => {
                if let Some(v) = args.next() {
                    ping_interval = v.parse().unwrap_or(ping_interval)
                }
            }
            "--rtp-ms" => {
                if let Some(v) = args.next() {
                    rtp_interval_ms = v.parse().unwrap_or(rtp_interval_ms)
                }
            }
            "--event-sec" => {
                if let Some(v) = args.next() {
                    event_interval_sec = v.parse().unwrap_or(event_interval_sec)
                }
            }
            _ => {}
        }
    }

    let server_addr = server_addr
        .or_else(|| std::env::var("SERVER_ADDR").ok())
        .ok_or("--server or SERVER_ADDR is required")?;
    let agent_id = agent_id
        .or_else(|| std::env::var("AGENT_ID").ok())
        .ok_or("--agent-id or AGENT_ID is required")?;
    let token = token
        .or_else(|| std::env::var("AGENT_TOKEN").ok())
        .ok_or("--token or AGENT_TOKEN is required")?;
    let streams = streams
        .or_else(|| std::env::var("STREAMS").ok())
        .unwrap_or_else(|| "stream1".to_string())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();

    Ok(Settings {
        server_addr,
        agent_id,
        token,
        streams,
        ca_cert,
        insecure,
        ping_interval,
        rtp_interval_ms,
        event_interval_sec,
    })
}

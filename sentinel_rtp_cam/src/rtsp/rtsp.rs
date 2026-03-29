use anyhow::{bail, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct RtspResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

fn header_get(headers: &[(String, String)], key: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map(|(_, v)| v.clone())
}

pub struct RtspClient {
    pub stream: TcpStream,
    pub cseq: u32,
    pub session: Option<String>,
    pub leftover: Vec<u8>, // ✅ NEW: bytes we read "too far"
}

impl RtspClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        debug!(host = %host, port = port, "Connecting to RTSP server");
        let stream = TcpStream::connect((host, port)).await?;
        Ok(Self {
            stream,
            cseq: 1,
            session: None,
            leftover: Vec::new(),
        })
    }

    pub async fn request(
        &mut self,
        method: &str,
        url: &str,
        extra_headers: &[(&str, &str)],
        body: Option<&[u8]>,
    ) -> Result<RtspResponse> {
        let mut req = String::new();
        req.push_str(&format!("{method} {url} RTSP/1.0\r\n"));
        req.push_str(&format!("CSeq: {}\r\n", self.cseq));
        self.cseq += 1;

        if let Some(sess) = &self.session {
            req.push_str(&format!("Session: {}\r\n", sess));
        }

        for (k, v) in extra_headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }

        if let Some(b) = body {
            req.push_str(&format!("Content-Length: {}\r\n", b.len()));
        }
        req.push_str("\r\n");

        self.stream.write_all(req.as_bytes()).await?;
        if let Some(b) = body {
            self.stream.write_all(b).await?;
        }
        self.read_response().await
    }

    async fn read_response(&mut self) -> Result<RtspResponse> {
        let mut head = Vec::new();
        let mut buf = [0u8; 4096];

        loop {
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                bail!("RTSP connection closed");
            }
            head.extend_from_slice(&buf[..n]);
            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if head.len() > 64 * 1024 {
                bail!("RTSP headers too large");
            }
        }

        let split = head.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let (head_bytes, rest) = head.split_at(split + 4);

        let head_str = String::from_utf8_lossy(head_bytes);
        let mut lines = head_str.split("\r\n").filter(|l| !l.is_empty());
        let status_line = lines
            .next()
            .ok_or_else(|| anyhow::anyhow!("Missing status line"))?;
        let mut parts = status_line.split_whitespace();
        let _proto = parts.next().unwrap_or("");
        let status: u16 = parts.next().unwrap_or("0").parse().unwrap_or(0);

        let mut headers = Vec::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(":") {
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }

        let content_len: usize = header_get(&headers, "Content-Length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        let mut body = Vec::new();
        if content_len > 0 {
            body.extend_from_slice(rest);
            while body.len() < content_len {
                let n = self.stream.read(&mut buf).await?;
                if n == 0 {
                    bail!("RTSP connection closed while reading body");
                }
                body.extend_from_slice(&buf[..n]);
            }
            body.truncate(content_len);
        }

        Ok(RtspResponse {
            status,
            headers,
            body,
        })
    }
    pub async fn read_exact_bytes(&mut self, n: usize) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        while out.len() < n && !self.leftover.is_empty() {
            let take = (n - out.len()).min(self.leftover.len());
            out.extend_from_slice(&self.leftover[..take]);
            self.leftover.drain(..take);
        }
        if out.len() < n {
            let mut buf = vec![0u8; n - out.len()];
            self.stream.read_exact(&mut buf).await?;
            out.extend_from_slice(&buf);
        }
        Ok(out)
    }
    pub fn set_session_from(&mut self, resp: &RtspResponse) {
        if let Some(sess) = header_get(&resp.headers, "Session") {
            let id = sess.split(';').next().unwrap_or(&sess).to_string();
            self.session = Some(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    async fn connected_streams() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_task = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server_stream, _) = listener.accept().await.unwrap();
        let client_stream = client_task.await.unwrap();

        (client_stream, server_stream)
    }

    #[test]
    fn header_get_is_case_insensitive() {
        let headers = vec![("Content-Length".to_string(), "12".to_string())];
        assert_eq!(
            header_get(&headers, "content-length"),
            Some("12".to_string())
        );
        assert_eq!(
            header_get(&headers, "CONTENT-LENGTH"),
            Some("12".to_string())
        );
    }

    #[tokio::test]
    async fn read_exact_bytes_consumes_leftover_then_stream() {
        let (client_stream, mut server_stream) = connected_streams().await;
        let writer = tokio::spawn(async move {
            server_stream.write_all(&[3u8, 4u8]).await.unwrap();
        });

        let mut client = RtspClient {
            stream: client_stream,
            cseq: 1,
            session: None,
            leftover: vec![1u8, 2u8],
        };

        let got = client.read_exact_bytes(4).await.unwrap();
        assert_eq!(got, vec![1u8, 2u8, 3u8, 4u8]);
        assert!(client.leftover.is_empty());
        writer.await.unwrap();
    }

    #[tokio::test]
    async fn set_session_from_strips_session_parameters() {
        let (client_stream, _server_stream) = connected_streams().await;
        let mut client = RtspClient {
            stream: client_stream,
            cseq: 1,
            session: None,
            leftover: Vec::new(),
        };

        client.set_session_from(&RtspResponse {
            status: 200,
            headers: vec![("Session".to_string(), "abc123;timeout=60".to_string())],
            body: Vec::new(),
        });

        assert_eq!(client.session.as_deref(), Some("abc123"));
    }
}

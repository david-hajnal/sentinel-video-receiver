use crate::rtsp::rtsp::RtspClient;
use anyhow::{bail, Result};

#[derive(Debug)]
pub enum InterleavedFrame {
    Rtp(Vec<u8>),
    Rtcp(Vec<u8>),
    Unknown(u8, Vec<u8>),
}

pub async fn read_interleaved_frame(
    c: &mut RtspClient,
    rtp_channel: u8,
    rtcp_channel: u8,
) -> Result<InterleavedFrame> {
    let b = c.read_exact_bytes(1).await?;
    if b[0] != 0x24 {
        bail!("Expected '$' (0x24), got 0x{:02x}", b[0]);
    }

    let hdr = c.read_exact_bytes(3).await?;
    let channel = hdr[0];
    let len = u16::from_be_bytes([hdr[1], hdr[2]]) as usize;

    let payload = c.read_exact_bytes(len).await?;

    Ok(if channel == rtp_channel {
        InterleavedFrame::Rtp(payload)
    } else if channel == rtcp_channel {
        InterleavedFrame::Rtcp(payload)
    } else {
        InterleavedFrame::Unknown(channel, payload)
    })
}

#[cfg(test)]
mod tests {
    use super::{read_interleaved_frame, InterleavedFrame};
    use crate::rtsp::rtsp::RtspClient;
    use std::io;
    use tokio::net::{TcpListener, TcpStream};

    async fn connected_streams() -> io::Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let client_task = tokio::spawn(async move { TcpStream::connect(addr).await });
        let (server_stream, _) = listener.accept().await?;
        let client_stream = client_task
            .await
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))??;

        Ok((client_stream, server_stream))
    }

    async fn client_with_leftover(leftover: Vec<u8>) -> io::Result<RtspClient> {
        let (client_stream, _server_stream) = connected_streams().await?;
        Ok(RtspClient {
            stream: client_stream,
            cseq: 1,
            session: None,
            leftover,
        })
    }

    #[tokio::test]
    async fn read_interleaved_frame_recognizes_rtp_channel() {
        let mut client = match client_with_leftover(vec![0x24, 0x00, 0x00, 0x03, 1, 2, 3]).await {
            Ok(client) => client,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("connected_streams failed: {error}"),
        };

        let frame = read_interleaved_frame(&mut client, 0, 1).await.unwrap();
        match frame {
            InterleavedFrame::Rtp(payload) => assert_eq!(payload, vec![1, 2, 3]),
            other => panic!("expected RTP frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_interleaved_frame_recognizes_rtcp_channel() {
        let mut client = match client_with_leftover(vec![0x24, 0x01, 0x00, 0x02, 9, 8]).await {
            Ok(client) => client,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("connected_streams failed: {error}"),
        };

        let frame = read_interleaved_frame(&mut client, 0, 1).await.unwrap();
        match frame {
            InterleavedFrame::Rtcp(payload) => assert_eq!(payload, vec![9, 8]),
            other => panic!("expected RTCP frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_interleaved_frame_maps_unknown_channel() {
        let mut client = match client_with_leftover(vec![0x24, 0x05, 0x00, 0x01, 0xAA]).await {
            Ok(client) => client,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("connected_streams failed: {error}"),
        };

        let frame = read_interleaved_frame(&mut client, 0, 1).await.unwrap();
        match frame {
            InterleavedFrame::Unknown(channel, payload) => {
                assert_eq!(channel, 5);
                assert_eq!(payload, vec![0xAA]);
            }
            other => panic!("expected unknown frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_interleaved_frame_rejects_non_dollar_prefix() {
        let mut client = match client_with_leftover(vec![0x21]).await {
            Ok(client) => client,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("connected_streams failed: {error}"),
        };

        let err = read_interleaved_frame(&mut client, 0, 1).await.unwrap_err();
        assert!(err.to_string().contains("Expected '$'"));
    }
}

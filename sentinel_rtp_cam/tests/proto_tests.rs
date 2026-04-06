use rand::RngCore;
use tokio::io::{duplex, AsyncWriteExt};

use sentinel_rtp_cam::proto::{
    decode_gap, encode_gap, read_msg, write_msg, Msg, HELLO, MAGIC, MAX_LEN, RTP, VERSION,
};

#[tokio::test]
async fn roundtrip_hello() {
    let (mut a, mut b) = duplex(1024);
    let msg = Msg {
        msg_type: HELLO,
        stream_id: 7,
        payload: b"hello".to_vec(),
    };

    tokio::spawn(async move {
        write_msg(&mut a, &msg).await.unwrap();
    });

    let got = read_msg(&mut b).await.unwrap();
    assert_eq!(got.msg_type, HELLO);
    assert_eq!(got.stream_id, 7);
    assert_eq!(got.payload, b"hello".to_vec());
}

#[tokio::test]
async fn roundtrip_rtp_random() {
    let (mut a, mut b) = duplex(4096);
    let mut payload = vec![0u8; 512];
    rand::thread_rng().fill_bytes(&mut payload);
    let msg = Msg {
        msg_type: RTP,
        stream_id: 42,
        payload: payload.clone(),
    };

    tokio::spawn(async move {
        write_msg(&mut a, &msg).await.unwrap();
    });

    let got = read_msg(&mut b).await.unwrap();
    assert_eq!(got.msg_type, RTP);
    assert_eq!(got.stream_id, 42);
    assert_eq!(got.payload, payload);
}

#[tokio::test]
async fn reject_wrong_magic() {
    let (mut a, mut b) = duplex(1024);
    let mut header = [0u8; 16];
    header[0..4].copy_from_slice(&0xDEADBEEFu32.to_be_bytes());
    header[4..6].copy_from_slice(&VERSION.to_be_bytes());
    header[6..8].copy_from_slice(&HELLO.to_be_bytes());
    header[8..12].copy_from_slice(&1u32.to_be_bytes());
    header[12..16].copy_from_slice(&0u32.to_be_bytes());

    a.write_all(&header).await.unwrap();
    let err = read_msg(&mut b).await.unwrap_err();
    assert!(err.to_string().contains("bad magic"));
}

#[tokio::test]
async fn reject_too_large_len() {
    let (mut a, mut b) = duplex(1024);
    let mut header = [0u8; 16];
    header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    header[4..6].copy_from_slice(&VERSION.to_be_bytes());
    header[6..8].copy_from_slice(&HELLO.to_be_bytes());
    header[8..12].copy_from_slice(&1u32.to_be_bytes());
    header[12..16].copy_from_slice(&((MAX_LEN as u32) + 1).to_be_bytes());

    a.write_all(&header).await.unwrap();
    let err = read_msg(&mut b).await.unwrap_err();
    assert!(err.to_string().contains("payload too large"));
}

#[tokio::test]
async fn reject_wrong_version() {
    let (mut a, mut b) = duplex(1024);
    let mut header = [0u8; 16];
    header[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    header[4..6].copy_from_slice(&(VERSION + 1).to_be_bytes());
    header[6..8].copy_from_slice(&HELLO.to_be_bytes());
    header[8..12].copy_from_slice(&1u32.to_be_bytes());
    header[12..16].copy_from_slice(&0u32.to_be_bytes());

    a.write_all(&header).await.unwrap();
    let err = read_msg(&mut b).await.unwrap_err();
    assert!(err.to_string().contains("bad version"));
}

#[test]
fn gap_payload_roundtrips() {
    let encoded = encode_gap(17, 23);
    assert_eq!(decode_gap(&encoded).unwrap(), (17, 23));
}

#[test]
fn decode_gap_rejects_wrong_payload_len() {
    let err = decode_gap(&[1, 2, 3]).unwrap_err();
    assert!(err.to_string().contains("gap payload len != 4"));
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use chrono::Duration;
use tokio::sync::oneshot;

use sentinel_rtp_cam::event::{Event, EventBus, MotionEvent};
use sentinel_rtp_cam::forward_agent::{forward_motion_event, MotionSender};
use sentinel_rtp_cam::motion_event_latch::MotionEventIdLatch;

#[derive(Clone, Debug)]
struct MotionCall {
    stream_id: u32,
    camera_id: String,
    event_id: String,
    active: bool,
}

#[derive(Default)]
struct MockSender {
    calls: Mutex<Vec<MotionCall>>,
}

impl MotionSender for MockSender {
    fn send_motion(
        &self,
        stream_id: u32,
        _rule: String,
        active: bool,
        _ts: String,
        camera_id: String,
        event_id: String,
    ) {
        self.calls.lock().unwrap().push(MotionCall {
            stream_id,
            camera_id,
            event_id,
            active,
        });
    }
}

#[tokio::test]
async fn event_bus_forwarding_normalizes_event_ids() {
    // Scenario: duplicate start events should be forwarded with a single, stable event_id.
    let bus = EventBus::new(8);
    let mut rx = bus.subscribe().await;

    let sender = Arc::new(MockSender::default());
    let sender_clone = sender.clone();
    let mut cam_map = HashMap::new();
    cam_map.insert("cam-1".to_string(), 1);

    let (done_tx, done_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut latch = MotionEventIdLatch::new();
        let mut count = 0u8;
        while let Some(ev) = rx.recv().await {
            let Event::Motion(m) = ev;
            let m = latch.normalize(m);
            forward_motion_event(&*sender_clone, &cam_map, &m);
            count += 1;
            if count >= 3 {
                break;
            }
        }
        let _ = done_tx.send(());
    });

    let ts = Utc::now();
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts,
        camera_id: "cam-1".to_string(),
        event_id: "event-a".to_string(),
    }))
    .await;
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts,
        camera_id: "cam-1".to_string(),
        event_id: "event-b".to_string(),
    }))
    .await;
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: false,
        ts,
        camera_id: "cam-1".to_string(),
        event_id: "event-c".to_string(),
    }))
    .await;

    let _ = done_rx.await;

    let calls = sender.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].event_id, "event-a");
    assert_eq!(calls[1].event_id, "event-a");
    assert_eq!(calls[2].event_id, "event-a");
    assert_eq!(calls[2].active, false);
    for call in calls.iter() {
        assert_eq!(call.stream_id, 1);
        assert_eq!(call.camera_id, "cam-1");
    }
}

#[tokio::test]
async fn event_bus_forwarding_respects_grace_window_latching() {
    // Scenario: a new motion start within the grace window reuses the prior event_id,
    // and a later start outside the grace window uses a new event_id.
    let bus = EventBus::new(8);
    let mut rx = bus.subscribe().await;

    let sender = Arc::new(MockSender::default());
    let sender_clone = sender.clone();
    let mut cam_map = HashMap::new();
    cam_map.insert("cam-1".to_string(), 1);

    let (done_tx, done_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut latch = MotionEventIdLatch::new_with_grace(Duration::seconds(3));
        let mut count = 0u8;
        while let Some(ev) = rx.recv().await {
            let Event::Motion(m) = ev;
            let m = latch.normalize(m);
            forward_motion_event(&*sender_clone, &cam_map, &m);
            count += 1;
            if count >= 5 {
                break;
            }
        }
        let _ = done_tx.send(());
    });

    let ts = Utc::now();
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts,
        camera_id: "cam-1".to_string(),
        event_id: "event-a".to_string(),
    }))
    .await;
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: false,
        ts: ts + Duration::seconds(2),
        camera_id: "cam-1".to_string(),
        event_id: "event-a".to_string(),
    }))
    .await;
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts: ts + Duration::seconds(3),
        camera_id: "cam-1".to_string(),
        event_id: "event-b".to_string(),
    }))
    .await;
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: false,
        ts: ts + Duration::seconds(4),
        camera_id: "cam-1".to_string(),
        event_id: "event-b".to_string(),
    }))
    .await;
    bus.publish(Event::Motion(MotionEvent {
        rule: "rule-1".to_string(),
        active: true,
        ts: ts + Duration::seconds(10),
        camera_id: "cam-1".to_string(),
        event_id: "event-c".to_string(),
    }))
    .await;

    let _ = done_rx.await;

    let calls = sender.calls.lock().unwrap();
    assert_eq!(calls.len(), 5);
    assert_eq!(calls[0].event_id, "event-a");
    assert_eq!(calls[1].event_id, "event-a");
    assert_eq!(calls[2].event_id, "event-a");
    assert_eq!(calls[3].event_id, "event-a");
    assert_eq!(calls[4].event_id, "event-c");
    assert_eq!(calls[4].active, true);
    for call in calls.iter() {
        assert_eq!(call.stream_id, 1);
        assert_eq!(call.camera_id, "cam-1");
    }
}

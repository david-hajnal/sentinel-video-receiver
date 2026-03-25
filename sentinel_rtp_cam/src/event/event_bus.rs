use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone)]
pub enum Event {
    Motion(MotionEvent),
}

#[derive(Debug, Clone)]
pub struct MotionEvent {
    pub rule: String,
    pub active: bool, // true=start/ongoing; but we'll emit edges only
    pub ts: DateTime<Utc>,
    pub camera_id: String,
    pub event_id: String, // ULID generated on motion start, reused for motion end
}

impl fmt::Display for MotionEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "camera_id={} rule={} event_id={} active={} ts={}",
            self.camera_id,
            self.rule,
            self.event_id,
            self.active,
            self.ts.to_rfc3339()
        )
    }
}

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Mutex<Vec<mpsc::Sender<Event>>>>,
    queue: usize,
}

impl EventBus {
    pub fn new(queue: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
            queue,
        }
    }

    pub async fn subscribe(&self) -> mpsc::Receiver<Event> {
        let (tx, rx) = mpsc::channel(self.queue);
        self.inner.lock().await.push(tx);
        rx
    }

    pub async fn publish(&self, ev: Event) {
        let subscribers = {
            let mut guard = self.inner.lock().await;

            // Fan-out only to live subscribers.
            guard.retain(|tx| !tx.is_closed());
            guard.clone()
        };

        for tx in subscribers {
            let _ = tx.send(ev.clone()).await;
        }

        self.inner.lock().await.retain(|tx| !tx.is_closed());
    }
}

// ============================================================================
// Motion State Bus (watch-based state synchronization)
// ============================================================================

/// Metadata for an active motion rule
#[derive(Debug, Clone)]
pub struct MotionMetadata {
    pub camera_id: String,
    pub event_id: String,
}

/// Motion state: maps rule names to their current active status and metadata
pub type MotionState = HashMap<String, MotionMetadata>;

/// MotionStateBus provides a watch channel for motion state.
/// Unlike EventBus (mpsc-based edge events), this provides current state
/// that can be observed by multiple subscribers without missing updates.
#[derive(Clone)]
pub struct MotionStateBus {
    tx: Arc<watch::Sender<MotionState>>,
}

impl MotionStateBus {
    /// Create a new MotionStateBus and return a receiver for the initial subscriber
    pub fn new() -> (Self, watch::Receiver<MotionState>) {
        let (tx, rx) = watch::channel(HashMap::new());
        (Self { tx: Arc::new(tx) }, rx)
    }

    /// Subscribe to motion state changes
    pub fn subscribe(&self) -> watch::Receiver<MotionState> {
        self.tx.subscribe()
    }

    /// Update motion state for a specific rule (idempotent)
    /// When active=true, adds rule with metadata. When active=false, removes rule.
    /// Only sends update if the state actually changes
    pub fn set(&self, rule: String, active: bool, metadata: Option<MotionMetadata>) {
        self.tx.send_if_modified(|state| {
            if active {
                if let Some(meta) = metadata {
                    // Add or update the rule with metadata
                    if state
                        .get(&rule)
                        .map(|m| m.event_id == meta.event_id)
                        .unwrap_or(false)
                    {
                        // Same event_id, no change
                        false
                    } else {
                        state.insert(rule, meta);
                        true
                    }
                } else {
                    // active=true but no metadata - should not happen
                    false
                }
            } else {
                // Remove the rule
                state.remove(&rule).is_some()
            }
        });
    }

    /// Get current snapshot of motion state
    pub fn current(&self) -> MotionState {
        self.tx.borrow().clone()
    }

    /// Clear active motion state for a specific camera.
    pub fn clear_camera(&self, camera_id: &str) {
        self.tx.send_if_modified(|state| {
            let before = state.len();
            state.retain(|_, metadata| metadata.camera_id != camera_id);
            state.len() != before
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{Event, EventBus, MotionEvent, MotionMetadata, MotionStateBus};
    use chrono::Utc;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn publish_waits_instead_of_dropping_when_queue_is_full() {
        let bus = EventBus::new(1);
        let mut rx = bus.subscribe().await;

        bus.publish(Event::Motion(MotionEvent {
            rule: "rule-1".to_string(),
            active: true,
            ts: Utc::now(),
            camera_id: "cam-1".to_string(),
            event_id: "evt-1".to_string(),
        }))
        .await;

        let bus_clone = bus.clone();
        let publish_task = tokio::spawn(async move {
            bus_clone
                .publish(Event::Motion(MotionEvent {
                    rule: "rule-1".to_string(),
                    active: false,
                    ts: Utc::now(),
                    camera_id: "cam-1".to_string(),
                    event_id: "evt-1".to_string(),
                }))
                .await;
        });

        tokio::task::yield_now().await;
        assert!(!publish_task.is_finished());

        let first_recv = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("first recv should complete")
            .expect("first event should exist");
        let Event::Motion(first_motion) = first_recv;
        assert!(first_motion.active);

        timeout(Duration::from_secs(1), publish_task)
            .await
            .expect("publish task should unblock")
            .expect("publish task should succeed");

        let second_recv = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("second recv should complete")
            .expect("second event should exist");
        let Event::Motion(second_motion) = second_recv;
        assert!(!second_motion.active);
    }

    #[test]
    fn clear_camera_removes_only_matching_motion_state() {
        let (bus, _rx) = MotionStateBus::new();

        bus.set(
            "rule-1".to_string(),
            true,
            Some(MotionMetadata {
                camera_id: "cam-1".to_string(),
                event_id: "evt-1".to_string(),
            }),
        );
        bus.set(
            "rule-2".to_string(),
            true,
            Some(MotionMetadata {
                camera_id: "cam-2".to_string(),
                event_id: "evt-2".to_string(),
            }),
        );

        bus.clear_camera("cam-1");

        let state = bus.current();
        assert!(!state.contains_key("rule-1"));
        assert_eq!(
            state
                .get("rule-2")
                .map(|metadata| metadata.camera_id.as_str()),
            Some("cam-2")
        );
    }
}

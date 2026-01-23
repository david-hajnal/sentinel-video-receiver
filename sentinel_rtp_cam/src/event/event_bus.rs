use chrono::{DateTime, Utc};
use std::collections::HashMap;
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
        let mut guard = self.inner.lock().await;

        // Fan-out; drop subscribers that are closed.
        guard.retain(|tx| !tx.is_closed());

        for tx in guard.iter() {
            // non-blocking: if subscriber queue is full, we drop the event for that subscriber
            let _ = tx.try_send(ev.clone());
        }
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
}

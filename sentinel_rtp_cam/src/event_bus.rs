use tokio::sync::mpsc;
use tokio::sync::Mutex;
use std::sync::Arc;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub enum Event {
    Motion(MotionEvent),
}

#[derive(Debug, Clone)]
pub struct MotionEvent {
    pub rule: String,
    pub active: bool, // true=start/ongoing; but we'll emit edges only
    pub ts: DateTime<Utc>,
}

#[derive(Clone)]
pub struct EventBus {
    inner: Arc<Mutex<Vec<mpsc::Sender<Event>>>>,
    queue: usize,
}

impl EventBus {
    pub fn new(queue: usize) -> Self {
        Self { inner: Arc::new(Mutex::new(Vec::new())), queue }
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

use std::collections::HashMap;

use crate::event::MotionEvent;
use chrono::{DateTime, Duration, Utc};
use tracing::debug;

#[derive(Default)]
pub struct MotionEventIdLatch {
    active: HashMap<(String, String), String>,
    ended: HashMap<(String, String), EndedMotion>,
    grace_window: Duration,
}

#[derive(Clone)]
struct EndedMotion {
    event_id: String,
    ended_at: DateTime<Utc>,
}

impl MotionEventIdLatch {
    pub fn new() -> Self {
        Self {
            active: HashMap::new(),
            ended: HashMap::new(),
            grace_window: Duration::zero(),
        }
    }

    pub fn new_with_grace(grace_window: Duration) -> Self {
        Self {
            active: HashMap::new(),
            ended: HashMap::new(),
            grace_window,
        }
    }

    /// Normalize a motion event so a continuous motion period retains a single event_id.
    ///
    /// On start events (`active=true`), the first event_id for a `(camera_id, rule)` pair
    /// is latched and reused for subsequent start events. On end events (`active=false`),
    /// the latched id is applied (if present) and the latch is cleared for the next motion.
    pub fn normalize(&mut self, mut event: MotionEvent) -> MotionEvent {
        let key = (event.camera_id.clone(), event.rule.clone());
        if event.active {
            if let Some(existing) = self.active.get(&key) {
                debug!(
                    camera_id = %event.camera_id,
                    rule = %event.rule,
                    event_id = %existing,
                    "Motion start normalized to latched event_id"
                );
                event.event_id = existing.clone();
            } else {
                if let Some(ended) = self.ended.get(&key) {
                    let delta = event.ts.signed_duration_since(ended.ended_at);
                    if delta >= Duration::zero() && delta <= self.grace_window {
                        debug!(
                            camera_id = %event.camera_id,
                            rule = %event.rule,
                            event_id = %ended.event_id,
                            delta_secs = delta.num_seconds(),
                            "Motion start reused event_id within grace window"
                        );
                        event.event_id = ended.event_id.clone();
                    }
                }
                self.ended.remove(&key);
                debug!(
                    camera_id = %event.camera_id,
                    rule = %event.rule,
                    event_id = %event.event_id,
                    "Motion start latched new event_id"
                );
                self.active.insert(key, event.event_id.clone());
            }
        } else {
            if let Some(existing) = self.active.remove(&key) {
                debug!(
                    camera_id = %event.camera_id,
                    rule = %event.rule,
                    event_id = %existing,
                    "Motion end normalized to latched event_id"
                );
                event.event_id = existing;
            }
            self.ended.insert(
                key,
                EndedMotion {
                    event_id: event.event_id.clone(),
                    ended_at: event.ts,
                },
            );
        }
        event
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::MotionEventIdLatch;
    use crate::event::MotionEvent;

    const CAMERA_ID: &str = "cam-1";
    const RULE_ID: &str = "rule-1";
    const EVENT_A: &str = "event-a";
    const EVENT_B: &str = "event-b";
    const EVENT_C: &str = "event-c";

    #[test]
    fn normalize_keeps_first_event_id_until_end() {
        // Scenario: duplicate start events should keep the first event_id until a matching end.
        let mut latch = MotionEventIdLatch::new();
        let ts = Utc::now();

        let start_a = MotionEvent {
            rule: RULE_ID.to_string(),
            active: true,
            ts,
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_A.to_string(),
        };
        let start_b = MotionEvent {
            rule: RULE_ID.to_string(),
            active: true,
            ts,
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_B.to_string(),
        };
        let end_a = MotionEvent {
            rule: RULE_ID.to_string(),
            active: false,
            ts,
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_C.to_string(),
        };

        let normalized_start_a = latch.normalize(start_a);
        let normalized_start_b = latch.normalize(start_b);
        let normalized_end = latch.normalize(end_a);

        assert_eq!(normalized_start_a.event_id, EVENT_A);
        assert_eq!(normalized_start_b.event_id, EVENT_A);
        assert_eq!(normalized_end.event_id, EVENT_A);
    }

    #[test]
    fn normalize_uses_new_event_id_after_end() {
        // Scenario: after an end event, a new start should use its own event_id.
        let mut latch = MotionEventIdLatch::new();
        let ts = Utc::now();

        let start_a = MotionEvent {
            rule: RULE_ID.to_string(),
            active: true,
            ts,
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_A.to_string(),
        };
        let end_a = MotionEvent {
            rule: RULE_ID.to_string(),
            active: false,
            ts: ts + Duration::seconds(1),
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_A.to_string(),
        };
        let start_b = MotionEvent {
            rule: RULE_ID.to_string(),
            active: true,
            ts: ts + Duration::seconds(3),
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_B.to_string(),
        };

        let _ = latch.normalize(start_a);
        let _ = latch.normalize(end_a);
        let normalized_start_b = latch.normalize(start_b);

        assert_eq!(normalized_start_b.event_id, EVENT_B);
    }

    #[test]
    fn normalize_reuses_event_id_within_grace_window() {
        // Scenario: if motion restarts shortly after an end, reuse the prior event_id.
        let mut latch = MotionEventIdLatch::new_with_grace(Duration::seconds(3));
        let ts = Utc::now();

        let start_a = MotionEvent {
            rule: RULE_ID.to_string(),
            active: true,
            ts,
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_A.to_string(),
        };
        let end_a = MotionEvent {
            rule: RULE_ID.to_string(),
            active: false,
            ts: ts + Duration::seconds(1),
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_A.to_string(),
        };
        let start_b = MotionEvent {
            rule: RULE_ID.to_string(),
            active: true,
            ts: ts + Duration::seconds(2),
            camera_id: CAMERA_ID.to_string(),
            event_id: EVENT_B.to_string(),
        };

        let _ = latch.normalize(start_a);
        let _ = latch.normalize(end_a);
        let normalized_start_b = latch.normalize(start_b);

        assert_eq!(normalized_start_b.event_id, EVENT_A);
    }
}

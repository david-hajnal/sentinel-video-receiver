use crate::event::Event;
use tokio::sync::mpsc;
use tracing::info;

pub fn spawn_logger(mut rx: mpsc::Receiver<Event>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            match ev {
                Event::Motion(m) => {
                    if m.active {
                        info!(
                            camera_id = %m.camera_id,
                            event_id = %m.event_id,
                            rule = %m.rule,
                            timestamp = %m.ts,
                            "Motion detected"
                        );
                    } else {
                        info!(
                            camera_id = %m.camera_id,
                            event_id = %m.event_id,
                            rule = %m.rule,
                            timestamp = %m.ts,
                            "Motion ended"
                        );
                    }
                }
            }
        }
    })
}

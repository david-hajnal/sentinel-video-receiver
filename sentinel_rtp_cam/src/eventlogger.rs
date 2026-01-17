use tokio::sync::mpsc;
use crate::event_bus::Event;

pub fn spawn_logger(mut rx: mpsc::Receiver<Event>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            match ev {
                Event::Motion(m) => {
                    if m.active {
                        println!("🚨 MOTION START rule={} ts={}", m.rule, m.ts);
                    } else {
                        println!("✅ MOTION END   rule={} ts={}", m.rule, m.ts);
                    }
                }
            }
        }
    })
}

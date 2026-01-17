use anyhow::{anyhow, Result};
use reqwest::{Response, StatusCode};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Copy, Debug)]
pub struct RetryCfg {
    pub max_attempts: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryCfg {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(3),
        }
    }
}

fn is_retryable_status(s: StatusCode) -> bool {
    s == StatusCode::TOO_MANY_REQUESTS || s.is_server_error()
}

fn is_retryable_reqwest_err(e: &reqwest::Error) -> bool {
    // These cover most transient network failures.
    e.is_timeout()
        || e.is_connect()
        || e.is_request() // includes some body/IO errors
        || e.to_string().contains("Connection refused")
        || e.to_string().contains("os error 61")
        || e.to_string().contains("connection reset")
}

fn exp_backoff(attempt: usize, base: Duration, max: Duration) -> Duration {
    // attempt is 0-based: 0 => base, 1 => 2x, 2 => 4x ...
    let mul = 2u64.saturating_pow(attempt.min(20) as u32);
    let d = base.saturating_mul(mul.try_into().unwrap());
    if d > max { max } else { d }
}

/// Retry a request builder factory.
///
/// We pass `make_req` (closure returning RequestBuilder) because reqwest builders are one-shot.
pub async fn retry_request<F>(cfg: RetryCfg, mut make_req: F) -> Result<Response>
where
    F: FnMut() -> reqwest::RequestBuilder,
{
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 0..cfg.max_attempts {
        let resp = make_req().send().await;

        match resp {
            Ok(r) => {
                if is_retryable_status(r.status()) {
                    let status = r.status();
                    let delay = exp_backoff(attempt, cfg.base_delay, cfg.max_delay);
                    last_err = Some(anyhow!("retryable HTTP status: {}", status));
                    // Drain body so connection can be reused cleanly (best-effort).
                    let _ = r.bytes().await;
                    sleep(delay).await;
                    continue;
                }
                return Ok(r);
            }
            Err(e) => {
                if is_retryable_reqwest_err(&e) {
                    let delay = exp_backoff(attempt, cfg.base_delay, cfg.max_delay);
                    last_err = Some(e.into());
                    sleep(delay).await;
                    continue;
                }
                return Err(e.into());
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("request failed after retries")))
}

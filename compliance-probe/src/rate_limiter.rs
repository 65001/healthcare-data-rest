//! Token-bucket rate limiting and per-domain backoff for probing hospital
//! websites politely, per Architecture.md's "Concurrency & Rate Limiting"
//! section. Fully implemented — this is self-contained logic that doesn't
//! depend on the still-undecided manifest format (see `probe.rs`).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Global token-bucket limiter. `PROBE_RATE_LIMIT_PER_SEC` tokens are
/// added per second, up to a burst capacity equal to that same rate
/// (i.e. at most ~1 second worth of burst).
pub struct RateLimiter {
    state: Mutex<BucketState>,
    rate_per_sec: f64,
    capacity: f64,
}

struct BucketState {
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(rate_per_sec: f64) -> Self {
        Self {
            state: Mutex::new(BucketState {
                tokens: rate_per_sec,
                last_refill: Instant::now(),
            }),
            rate_per_sec,
            capacity: rate_per_sec,
        }
    }

    fn refill(&self, state: &mut BucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        state.last_refill = now;
    }

    /// Blocks (async) until one token is available, then consumes it.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut state = self.state.lock().unwrap();
                self.refill(&mut state);
                if state.tokens >= 1.0 {
                    state.tokens -= 1.0;
                    None
                } else {
                    let deficit = 1.0 - state.tokens;
                    Some(Duration::from_secs_f64(deficit / self.rate_per_sec))
                }
            };
            match wait {
                None => return,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }
}

/// Per-domain exponential backoff for 429/503 responses: 1s → 2s → 4s →
/// … → 60s max, per Architecture.md.
pub struct DomainBackoff {
    state: Mutex<HashMap<String, DomainState>>,
    max_backoff: Duration,
}

struct DomainState {
    next_backoff: Duration,
    blocked_until: Instant,
}

impl Default for DomainBackoff {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

impl DomainBackoff {
    pub fn new(max_backoff: Duration) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            max_backoff,
        }
    }

    /// Record a 429/503 for `domain`, doubling its backoff (starting at
    /// 1s) up to `max_backoff`.
    pub fn record_failure(&self, domain: &str) {
        let mut state = self.state.lock().unwrap();
        let entry = state.entry(domain.to_string()).or_insert(DomainState {
            next_backoff: Duration::from_secs(1),
            blocked_until: Instant::now(),
        });
        entry.blocked_until = Instant::now() + entry.next_backoff;
        entry.next_backoff = (entry.next_backoff * 2).min(self.max_backoff);
    }

    /// Clear backoff state for `domain` after a successful request.
    pub fn record_success(&self, domain: &str) {
        self.state.lock().unwrap().remove(domain);
    }

    /// How long the caller should wait before probing `domain` again.
    /// `Duration::ZERO` if it isn't currently backed off.
    pub fn wait_for(&self, domain: &str) -> Duration {
        let state = self.state.lock().unwrap();
        match state.get(domain) {
            Some(s) if s.blocked_until > Instant::now() => s.blocked_until - Instant::now(),
            _ => Duration::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rate_limiter_allows_burst_up_to_capacity() {
        let limiter = RateLimiter::new(5.0);
        let start = Instant::now();
        for _ in 0..5 {
            limiter.acquire().await;
        }
        // 5 tokens were available immediately (capacity == rate).
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let backoff = DomainBackoff::new(Duration::from_secs(8));
        backoff.record_failure("example.com");
        let first = backoff.wait_for("example.com");
        assert!(first <= Duration::from_secs(1) && first > Duration::ZERO);

        backoff.record_failure("example.com");
        backoff.record_failure("example.com");
        backoff.record_failure("example.com");
        backoff.record_failure("example.com");
        // Fifth failure: backoff sequence is 1,2,4,8,8 (capped) seconds.
        let capped = backoff.wait_for("example.com");
        assert!(capped <= Duration::from_secs(8));
    }

    #[test]
    fn success_clears_backoff() {
        let backoff = DomainBackoff::default();
        backoff.record_failure("example.com");
        backoff.record_success("example.com");
        assert_eq!(backoff.wait_for("example.com"), Duration::ZERO);
    }
}

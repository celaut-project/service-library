//! One instance that this service started.

use tokio::time::Instant;

/// A short name for an instance token that is safe to log: the first 8 hex digits of the
/// hash of the token. The token is a credential (it stops the instance and reads its
/// balance), so errors and `Debug` output name the instance by this and never by the token.
pub fn token_tag(token: &str) -> String {
    let digest = crate::receipt::blake2b_256_hex(token.as_bytes());
    digest.chars().take(8).collect()
}

/// An instance of a dependency, with what the pool knows about how it behaves.
///
/// While an instance is out of the pool, the one that holds it must give it back or stop it.
/// An instance that nobody holds and nobody stops stays on the node, and bills, until the
/// service is removed.
#[derive(Clone)]
pub struct ServiceInstance {
    /// `ip:port` of the gRPC port of the instance.
    pub uri: String,
    /// The token of the instance. It is a bearer secret: it stops the instance.
    pub token: String,
    pub(crate) created: Instant,
    pub(crate) last_used: Instant,
    pub(crate) timeouts: u32,
    pub(crate) failures: u32,
}

impl std::fmt::Debug for ServiceInstance {
    /// The token is left out: it is a credential, and `Debug` output is logged.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceInstance")
            .field("uri", &self.uri)
            .field(
                "token",
                &format_args!("<redacted {}>", token_tag(&self.token)),
            )
            .field("created", &self.created)
            .field("last_used", &self.last_used)
            .field("timeouts", &self.timeouts)
            .field("failures", &self.failures)
            .finish()
    }
}

impl ServiceInstance {
    pub(crate) fn new(uri: String, token: String) -> Self {
        let now = Instant::now();
        ServiceInstance {
            uri,
            token,
            created: now,
            last_used: now,
            timeouts: 0,
            failures: 0,
        }
    }

    /// The calls that timed out since the last good one.
    pub fn timeouts(&self) -> u32 {
        self.timeouts
    }

    /// The calls that failed since the last good one.
    pub fn failures(&self) -> u32 {
        self.failures
    }

    /// When the instance started.
    pub fn created(&self) -> Instant {
        self.created
    }

    /// A call to the instance timed out.
    pub fn record_timeout(&mut self) {
        self.timeouts += 1;
    }

    /// A call to the instance failed.
    pub fn record_failure(&mut self) {
        self.failures += 1;
    }

    /// A call to the instance worked.
    pub fn record_success(&mut self) {
        self.timeouts = 0;
        self.failures = 0;
    }

    /// True if the instance is broken: it failed more than `max_failures` times, or it
    /// timed out more than `max_timeouts` times and `alive` is false (a slow instance that
    /// still answers its liveness check is kept).
    pub fn is_zombie(&self, max_timeouts: u32, max_failures: u32, alive: bool) -> bool {
        (self.timeouts > max_timeouts && !alive) || self.failures > max_failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instance() -> ServiceInstance {
        ServiceInstance::new("10.0.0.2:50051".into(), "token".into())
    }

    #[test]
    fn debug_output_does_not_hold_the_token() {
        let text = format!("{:?}", instance());
        assert!(!text.contains("\"token\""), "{text}");
        assert!(text.contains(&token_tag("token")), "{text}");
    }

    #[test]
    fn a_tag_is_short_stable_and_not_the_token() {
        assert_eq!(token_tag("abc"), token_tag("abc"));
        assert_ne!(token_tag("abc"), token_tag("abd"));
        assert_eq!(token_tag("abc").len(), 8);
    }

    #[test]
    fn a_fresh_instance_is_not_a_zombie() {
        assert!(!instance().is_zombie(5, 3, false));
    }

    #[test]
    fn many_failures_make_a_zombie_even_if_it_is_alive() {
        let mut i = instance();
        for _ in 0..4 {
            i.record_failure();
        }
        assert!(i.is_zombie(5, 3, true));
    }

    #[test]
    fn timeouts_make_a_zombie_only_if_the_check_fails() {
        let mut i = instance();
        for _ in 0..6 {
            i.record_timeout();
        }
        assert!(!i.is_zombie(5, 3, true));
        assert!(i.is_zombie(5, 3, false));
    }

    #[test]
    fn a_good_call_clears_the_counts() {
        let mut i = instance();
        i.record_failure();
        i.record_timeout();
        i.record_success();
        assert_eq!((i.failures(), i.timeouts()), (0, 0));
    }
}

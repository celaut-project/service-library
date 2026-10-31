//! Errors of the library.

/// Every error of the library.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The configuration file of the node is missing or cannot be read.
    #[error("the configuration of the node cannot be used: {0}")]
    Config(String),
    /// A call to the gateway failed on every attempt.
    ///
    /// `unreachable` tells "the node refused" from "the node could not be reached": a
    /// consumer that turns what it sees into accusations (a reputation system) must not
    /// mix them.
    #[error("{operation} failed after {attempts} attempt(s): {message}")]
    Gateway {
        /// What the call did, for a person.
        operation: String,
        /// How many times it was tried.
        attempts: u32,
        /// True if the gateway did not answer (no connection, or a time limit).
        unreachable: bool,
        /// The last error.
        message: String,
    },
    /// The gateway answered something that is not usable.
    #[error("unexpected answer: {0}")]
    Protocol(String),
}

impl Error {
    /// True if a call failed because the gateway could not be reached.
    pub fn is_unreachable(&self) -> bool {
        matches!(
            self,
            Error::Gateway {
                unreachable: true,
                ..
            }
        )
    }
}

/// The `unreachable` flag of a bee-rpc error: a transport failure or a time limit, and not
/// an answer of the node.
pub(crate) fn is_unreachable(error: &bee_rpc::Error) -> bool {
    let text = error.to_string().to_ascii_lowercase();
    [
        "unavailable",
        "deadline",
        "timed out",
        "connection",
        "transport error",
    ]
    .iter()
    .any(|t| text.contains(t))
}

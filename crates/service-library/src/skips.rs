//! The blocks that a node says it holds, for a call that gives it a service.
//!
//! A node that receives a block that it already has can ask the sender to stop it: it
//! sends a Buffer with `skip` and an empty chunk, in the answer direction (bee-rpc,
//! `bee_rpc::skip`). nodo does it on the input of `StartService` and
//! `GetServiceEstimatedCost` (`nodo:src/gateway/iterables/abstract_input_service_iterable.py`,
//! nodo#584). A node that does not, never sends one, and the service goes whole.
//!
//! [`Skips`] is shared by the two sides of a call. [`Skips::wrap`] sits on the stream of the
//! service and drops the body of the blocks that the node asked to skip, but not their end
//! markers. [`crate::relay::call_with_service`] reads the skip requests of the answer into it.
//! Wrap a [`crate::SendCounter`] around the result, not under it, to count what goes.

use std::pin::Pin;
use std::task::{Context, Poll};

use bee_rpc::buffer::Buffer;
use bee_rpc::hash::HashAlgorithm;
use bee_rpc::skip::{SkipFilter, StreamControl};
use tokio_stream::Stream;

use crate::relay::Buffers;
use crate::Error as WireError;

/// The skip requests of one call. Clone it to share it.
#[derive(Clone, Debug)]
pub struct Skips {
    control: StreamControl,
}

impl Default for Skips {
    fn default() -> Self {
        Skips::new()
    }
}

impl Skips {
    /// A new set, for block ids in the hash of nodo (SHA3-256).
    pub fn new() -> Self {
        Skips {
            control: StreamControl::new(HashAlgorithm::Sha3_256),
        }
    }

    /// `input` without the body of the blocks that the node asked to skip. A block is
    /// checked for each Buffer, so a request that comes in the middle of a block stops
    /// the rest of it. The end marker of a block is always kept.
    pub fn wrap(&self, input: Buffers) -> Buffers {
        Box::pin(Skipping {
            input,
            filter: SkipFilter::new(self.control.clone()),
        })
    }

    /// Record the skip request of `buffer` of the answer, if it has one.
    pub(crate) fn observe(&self, buffer: &Buffer) -> bool {
        self.control.observe(buffer)
    }

    /// How many different blocks the node asked to skip.
    pub fn blocks(&self) -> usize {
        self.control.peer_skips().len()
    }
}

struct Skipping {
    input: Buffers,
    filter: SkipFilter,
}

impl Stream for Skipping {
    type Item = Result<Buffer, WireError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match std::task::ready!(self.input.as_mut().poll_next(cx)) {
                Some(Ok(buffer)) => {
                    if let Some(kept) = self.filter.push(buffer) {
                        return Poll::Ready(Some(Ok(kept)));
                    }
                }
                other => return Poll::Ready(other),
            }
        }
    }
}

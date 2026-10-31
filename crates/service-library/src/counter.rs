//! Counts what a call takes from a stream of Buffers, for the progress of a transfer.

use std::pin::Pin;

use bee_rpc::buffer::Buffer;
use tokio_stream::Stream;

use crate::relay::Buffers;
use crate::Error as WireError;

/// Counts what a call takes from a stream of Buffers: the Buffers, their bytes (the
/// encoded length of each Buffer, as it goes in a gRPC message), and if the stream came
/// to its end. Clone it to share it.
///
/// A call takes a Buffer of the service when it sends it, so the count is what the node
/// got, or one Buffer more if the call stopped while that Buffer was on its way.
#[derive(Clone, Debug, Default)]
pub struct SendCounter {
    state: std::sync::Arc<CounterState>,
}

#[derive(Debug, Default)]
struct CounterState {
    buffers: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    ended: std::sync::atomic::AtomicBool,
}

impl SendCounter {
    pub fn new() -> Self {
        SendCounter::default()
    }

    /// `input`, counted.
    pub fn wrap(&self, input: Buffers) -> Buffers {
        Box::pin(Counted {
            input,
            counter: self.clone(),
        })
    }

    /// The Buffers taken so far.
    pub fn buffers(&self) -> u64 {
        self.state
            .buffers
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// The bytes of the Buffers taken so far.
    pub fn bytes(&self) -> u64 {
        self.state.bytes.load(std::sync::atomic::Ordering::Acquire)
    }

    /// True if the call took the stream to its end.
    pub fn ended(&self) -> bool {
        self.state.ended.load(std::sync::atomic::Ordering::Acquire)
    }
}

struct Counted {
    input: Buffers,
    counter: SendCounter,
}

impl Stream for Counted {
    type Item = Result<Buffer, WireError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::sync::atomic::Ordering;
        let item = std::task::ready!(self.input.as_mut().poll_next(cx));
        let state = &self.counter.state;
        match &item {
            Some(Ok(buffer)) => {
                state.buffers.fetch_add(1, Ordering::AcqRel);
                state
                    .bytes
                    .fetch_add(prost::Message::encoded_len(buffer) as u64, Ordering::AcqRel);
            }
            Some(Err(_)) => {}
            None => state.ended.store(true, Ordering::Release),
        }
        std::task::Poll::Ready(item)
    }
}

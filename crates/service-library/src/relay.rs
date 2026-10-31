//! A call that gives a service to a node, and stops when the node answers before the end
//! of the service (`docs/celaut/package.md`, "A node that already has the service").
//!
//! nodo reads the request of `StartService` and `GetServiceEstimatedCost` one part at a
//! time (`nodo:src/gateway/iterables/abstract_input_service_iterable.py`). When a Hash
//! (index 4) or the Metadata names a service that is already in its registry, it answers
//! at once: a signal (stop sending), the answer, a signal (send again). Then it reads the
//! rest of the request to its end and drops it. nodo's own client takes the first answer
//! and drops the call (`BeeClient.call_one`).
//!
//! A node that has some blocks of a service that it lacks can say so: it sends a skip
//! request for each block (a Buffer with `skip`) in its answer direction, while it reads
//! the service (nodo#584). The call reads these requests into [`Skips`]; the stream of the
//! service, wrapped with [`Skips::wrap`], stops the body of those blocks. A node that does
//! not send them (an older nodo has no `StreamControl` on this input) gets the whole
//! service. The request does not wait for skip requests after its end: nothing is left
//! to skip, so an older node that reads the request to its end does not make the call wait.
//!
//! This call does the same:
//!
//! - It sends the head (Client, guard, Configuration, Hash), then waits a short time
//!   ([`EARLY_ANSWER_WAIT`]) for the first Buffer of the node. A node that has the
//!   service answers in that time; a node that does not have it answers nothing until
//!   the end of the request, so the wait only delays the start of the service.
//! - It sends the service while it reads the answer. The sender waits while the signal
//!   of the node is closed, as in bee-rpc.
//! - When an answer part comes before the end of the service, it stops: it sends no
//!   more Buffers, and resets the request. A reset is not a clean end, so the node
//!   cannot take the cut service for a complete one. The answer comes early only for a
//!   service that the node has; the node already had it before this call.
//! - When the service fails (a cancel, a file error), the node gets the cancel markers
//!   of bee-rpc and then a clean end, as in `bee_rpc::grpc::call_buffers`, and the call
//!   gives the error of the service.
//! - In all other cases the request ends cleanly only after the whole service.

use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use bee_rpc::buffer::Buffer;
use bee_rpc::frame::{serialize_part, ParseOptions, Parser, Part};
use bee_rpc::grpc::cancel_markers;
use bee_rpc::signal::Signal;
use prost::Message;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamExt};
use tonic::codec::{Codec, EncodeBuf, Encoder};
use tonic::codegen::http::uri::PathAndQuery;
use tonic::Status;

use crate::skips::Skips;
use crate::Error as WireError;

/// Buffers that are already framed: the input of a call that relays a service.
pub type Buffers = Pin<Box<dyn Stream<Item = Result<Buffer, WireError>> + Send>>;

/// A failed call, as an error of the library.
fn call_error(message: String) -> WireError {
    WireError::Gateway {
        operation: "sending a service to the node".to_string(),
        attempts: 1,
        unreachable: message.contains("Unavailable") || message.contains("DeadlineExceeded"),
        message,
    }
}

/// How long the call waits after the head for the first Buffer of the node, before it
/// sends the service. A node that has the service answers in about one round trip.
pub const EARLY_ANSWER_WAIT: Duration = Duration::from_secs(2);

/// How long a failed call waits for its cancel markers to leave (as bee-rpc).
const MARKER_WAIT: Duration = Duration::from_secs(5);

/// What goes on the request: a Buffer, or the order to reset it.
enum Outgoing {
    Buffer(Buffer),
    Reset,
}

/// The codec of the call: Buffers both ways. [`Outgoing::Reset`] fails the encoder, and
/// hyper then resets the HTTP/2 stream (RST_STREAM) in place of a clean end.
#[derive(Default)]
struct RelayCodec;

struct RelayEncoder;

impl Encoder for RelayEncoder {
    type Item = Outgoing;
    type Error = Status;

    fn encode(&mut self, item: Outgoing, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        match item {
            Outgoing::Buffer(buffer) => buffer
                .encode(dst)
                .map_err(|e| Status::internal(e.to_string())),
            Outgoing::Reset => Err(Status::cancelled("the client stops this request")),
        }
    }
}

impl Codec for RelayCodec {
    type Encode = Outgoing;
    type Decode = Buffer;
    type Encoder = RelayEncoder;
    type Decoder = <tonic_prost::ProstCodec<Buffer, Buffer> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        RelayEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        tonic_prost::ProstCodec::<Buffer, Buffer>::default().decoder()
    }
}

/// The request: the Buffers of the send task. When the task ends, the request ends
/// cleanly only if the task said so ([`Shared::clean`]); else it is reset.
struct Request {
    rx: mpsc::Receiver<Buffer>,
    shared: Arc<Shared>,
    ended: bool,
}

impl Stream for Request {
    type Item = Outgoing;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Outgoing>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(buffer)) => Poll::Ready(Some(Outgoing::Buffer(buffer))),
            Poll::Ready(None) => {
                self.ended = true;
                if self.shared.clean.load(Ordering::Acquire) {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Outgoing::Reset))
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// What the send task and the answer side share.
#[derive(Default)]
struct Shared {
    /// The request may end cleanly: the whole service went, or the cancel markers.
    clean: AtomicBool,
    /// The node sent its first Buffer, and the parser read it.
    spoke: AtomicBool,
    spoke_notify: Notify,
    /// The error of the service.
    failed: Mutex<Option<WireError>>,
    failed_notify: Notify,
    /// The cancel markers left (or could not leave).
    markers_sent: AtomicBool,
    markers_notify: Notify,
}

impl Shared {
    fn set_spoke(&self) {
        self.spoke.store(true, Ordering::Release);
        self.spoke_notify.notify_waiters();
    }

    async fn wait_spoke(&self) {
        loop {
            let notified = self.spoke_notify.notified();
            if self.spoke.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    fn lock_failed(&self) -> std::sync::MutexGuard<'_, Option<WireError>> {
        self.failed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn has_failed(&self) -> bool {
        self.lock_failed().is_some()
    }

    async fn wait_failed(&self) {
        loop {
            let notified = self.failed_notify.notified();
            if self.has_failed() {
                return;
            }
            notified.await;
        }
    }

    /// The error of the service, once its markers left (at most [`MARKER_WAIT`]).
    async fn failure(&self) -> WireError {
        let markers = async {
            loop {
                let notified = self.markers_notify.notified();
                if self.markers_sent.load(Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(MARKER_WAIT, markers).await;
        self.lock_failed()
            .take()
            .unwrap_or_else(|| WireError::Protocol("the service failed".into()))
    }
}

/// Stops the send task when the call ends.
struct SendTask(JoinHandle<()>);

impl Drop for SendTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The error of a gRPC status, with the same text as a bee-rpc call.
fn status_error(status: impl std::fmt::Display) -> WireError {
    call_error(bee_rpc::Error::Transport(status.to_string()).to_string())
}

/// Call `path` on `channel`: send `head`, then `service`, and give the parts of the
/// answer with the indices `accepted`. See the module docs. `wait` is the time to wait
/// for the first Buffer of the node after the head ([`EARLY_ANSWER_WAIT`]; zero sends the
/// service at once). `timeout` limits the whole call. `skips` takes the skip requests of the
/// node: wrap `service` with [`Skips::wrap`] to stop the blocks that it holds.
#[allow(clippy::too_many_arguments)]
pub async fn call_with_service(
    channel: tonic::transport::Channel,
    path: &str,
    head: Vec<Part>,
    service: Buffers,
    accepted: &[i32],
    timeout: Duration,
    wait: Duration,
    skips: &Skips,
) -> Result<Vec<Part>, WireError> {
    let work = exchange(channel, path, head, service, accepted, timeout, wait, skips);
    tokio::time::timeout(timeout, work)
        .await
        .map_err(|_| WireError::Gateway {
            operation: "sending a service to the node".to_string(),
            attempts: 1,
            unreachable: true,
            message: "the node did not answer in time".to_string(),
        })?
}

#[allow(clippy::too_many_arguments)]
async fn exchange(
    channel: tonic::transport::Channel,
    path: &str,
    head: Vec<Part>,
    service: Buffers,
    accepted: &[i32],
    timeout: Duration,
    wait: Duration,
    skips: &Skips,
) -> Result<Vec<Part>, WireError> {
    let path = PathAndQuery::from_str(path).map_err(status_error)?;
    let signal = Signal::new();
    let mut parser =
        Parser::new(ParseOptions::accepting(accepted.iter().copied())).with_signal(signal.clone());
    let shared = Arc::new(Shared::default());
    let head: Vec<Buffer> = head.iter().flat_map(serialize_part).collect();
    let (tx, rx) = mpsc::channel::<Buffer>(1);
    let _task = SendTask(tokio::spawn(send_side(
        head,
        service,
        wait,
        signal,
        tx,
        shared.clone(),
    )));

    let mut request = tonic::Request::new(Request {
        rx,
        shared: shared.clone(),
        ended: false,
    });
    request.set_timeout(timeout);
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.map_err(status_error)?;
    // nodo sends the header of its answer with its first Buffer. A node that refuses
    // the cancel markers before its first answer fails here: the error of the service
    // comes first then too.
    let response = client.streaming(request, path, RelayCodec).await;
    let mut response = match response {
        Ok(response) => response.into_inner(),
        Err(status) => {
            if shared.has_failed() {
                return Err(shared.failure().await);
            }
            return Err(status_error(status));
        }
    };

    let mut out = Vec::new();
    loop {
        let next = tokio::select! {
            next = response.message() => next,
            _ = shared.wait_failed() => Ok(None),
        };
        if shared.has_failed() {
            return Err(shared.failure().await);
        }
        let Some(buffer) = next.map_err(status_error)? else {
            break;
        };
        // A skip request is read before the parser, which drops it: the send task asks
        // `skips` for each Buffer of the service.
        skips.observe(&buffer);
        let parsed = parser.push(buffer).map_err(|e| call_error(e.to_string()))?;
        // After the push, so that a first signal of the node already holds the sender.
        shared.set_spoke();
        let answered = parsed
            .iter()
            .any(|p| p.index != 0 && accepted.contains(&p.index));
        out.extend(parsed);
        if answered && !shared.clean.load(Ordering::Acquire) {
            // The node answered before the end of the service, so it has the service.
            // The signal that lets the sender go on is not read: no more Buffer
            // leaves, and the end of the send task resets the request.
            return Ok(out);
        }
    }
    out.extend(parser.finish().map_err(|e| call_error(e.to_string()))?);
    Ok(out)
}

/// The request direction: the head, the wait for the node, then the service. Each Buffer
/// waits for the signal of the node to be open.
async fn send_side(
    head: Vec<Buffer>,
    mut service: Buffers,
    wait: Duration,
    signal: Signal,
    tx: mpsc::Sender<Buffer>,
    shared: Arc<Shared>,
) {
    for buffer in head {
        signal.wait_open().await;
        if tx.send(buffer).await.is_err() {
            return;
        }
    }
    if !wait.is_zero() {
        let _ = tokio::time::timeout(wait, shared.wait_spoke()).await;
    }
    loop {
        signal.wait_open().await;
        match service.next().await {
            None => {
                // Before the sender is dropped: the request then ends cleanly.
                shared.clean.store(true, Ordering::Release);
                return;
            }
            Some(Ok(buffer)) => {
                if tx.send(buffer).await.is_err() {
                    return;
                }
            }
            Some(Err(error)) => {
                // The error first, so that an answer that ends now still gives it.
                *shared.lock_failed() = Some(error);
                shared.failed_notify.notify_waiters();
                for marker in cancel_markers() {
                    if tx.send(marker).await.is_err() {
                        break;
                    }
                }
                shared.clean.store(true, Ordering::Release);
                drop(tx);
                shared.markers_sent.store(true, Ordering::Release);
                shared.markers_notify.notify_waiters();
                return;
            }
        }
    }
}

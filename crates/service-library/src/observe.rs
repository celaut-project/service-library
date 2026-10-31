//! `Observe`: the live events of an instance that this service started (metrics, traffic,
//! notices), as a stream.

use bee_rpc::frame::{ParseOptions, Parser, Part};
use celaut_proto::celaut::{ObserveEvent, ObserveRequest};
use prost::Message;
use tokio::sync::mpsc;

use crate::gateway::Gateway;
use crate::Error;

/// What to ask the node to send, besides the session, the metrics and the notices.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObserveOptions {
    /// One event for each packet. It can be a lot of events.
    pub packets: bool,
    /// The checkpoints of the execution receipt, as events of kind `receipt`.
    pub receipts: bool,
}

/// An observation that is open. Drop it to end it.
#[derive(Debug)]
pub struct Observation {
    /// The events, in order. An error is the last item. The channel closes when the
    /// instance stops.
    pub events: mpsc::Receiver<Result<ObserveEvent, Error>>,
}

/// The request of `Observe`.
pub fn observe_parts(token: &str, options: ObserveOptions) -> Vec<Part> {
    let request = ObserveRequest {
        instance_id: token.to_string(),
        include_packets: options.packets,
        include_receipts: options.receipts,
    };
    vec![Part::new(1, request.encode_to_vec())]
}

impl Gateway {
    /// Observe the instance `token`, a local instance of the node. If the observation does
    /// not start, the first event is one notice that tells why. The stream stops when the
    /// instance stops.
    pub async fn observe(
        &self,
        token: &str,
        options: ObserveOptions,
    ) -> Result<Observation, Error> {
        let call_options = bee_rpc::grpc::CallOptions {
            parse: ParseOptions::accepting([1]),
            block_skip: false,
            // The deadline of a call is also the life of the observation: it lasts as long as
            // the instance does.
            timeout: None,
        };
        let mut response = bee_rpc::grpc::call_raw(
            self.channel(),
            "/celaut.Gateway/Observe",
            observe_parts(token, options),
            call_options,
        )
        .await
        .map_err(|e| Self::failed("observing an instance", 1, e))?;
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            let mut parser = Parser::new(ParseOptions::accepting([1]));
            loop {
                let buffer = match response.next().await {
                    Ok(Some(buffer)) => buffer,
                    Ok(None) => return,
                    Err(e) => {
                        let _ = tx
                            .send(Err(Self::failed("observing an instance", 1, e)))
                            .await;
                        return;
                    }
                };
                let parts = match parser.push(buffer) {
                    Ok(parts) => parts,
                    Err(e) => {
                        let _ = tx
                            .send(Err(Error::Protocol(format!(
                                "the events do not parse: {e}"
                            ))))
                            .await;
                        return;
                    }
                };
                for part in parts.into_iter().filter(|p| p.index == 1) {
                    let event = ObserveEvent::decode(part.data.as_slice())
                        .map_err(|e| Error::Protocol(format!("the event does not decode: {e}")));
                    let failed = event.is_err();
                    // A reader that went away ends the task.
                    if tx.send(event).await.is_err() || failed {
                        return;
                    }
                }
            }
        });
        Ok(Observation { events: rx })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_request_names_the_instance_and_what_to_send() {
        let parts = observe_parts(
            "tok",
            ObserveOptions {
                packets: true,
                receipts: false,
            },
        );
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].index, 1);
        let request = ObserveRequest::decode(parts[0].data.as_slice()).unwrap();
        assert_eq!(request.instance_id, "tok");
        assert!(request.include_packets);
        assert!(!request.include_receipts);
    }
}

//! The notice: a line that the node writes to a service before it stops or hibernates it
//! (nodo#645).
//!
//! A service that declares `notice_declaration` in its `service.json` has a console at
//! `/dev/hvc0`. Before the node stops or hibernates its instance for its father, or for the
//! close of the activity window, it writes one line there:
//!
//! ```text
//! STOP 1794399680
//! HIBERNATE 1794399680
//! ```
//!
//! The number is the unix time, in seconds, at which the node stops or hibernates the VM
//! whatever happens. The VM can end earlier: if the service exits (PID 1) or powers the guest
//! off, the node sees it within about a second. A service reads the line, flushes its state,
//! and exits, or does nothing. A node that does not know a verb is read as a service that
//! ignores it: a later node can add verbs, and [`Notice::parse`] returns `None` for them.
//!
//! **Keep the device open.** [`watch_device`] opens `/dev/hvc0` once and reads lines from it
//! until the service ends. Do not read it with an open for each line (`head -n1`, `read x <
//! /dev/hvc0`, `cat` in a pipe): a line can be lost between two opens, and a short script that
//! reads with `head` can reach its end, and then PID 1 exits and the VM ends.
//!
//! The notice is for *this* service. The instances that the service started are its
//! dependencies, and they live on after it: when the service stops, nobody asks them to stop.
//! [`shutdown_on_notice`] stops the idle instances of the pool within the time of the notice
//! (see [`crate::manager::DependencyManager::shutdown`]). An instance that a lease holds is
//! not stopped there: its holder must give it back, or call `Lease::stop`.
//!
//! Do not mix it with the notices of [`crate::observe`]: those are events about an instance
//! that this service started. This one is about this service.

use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, BufReader, Interest, ReadBuf};
use tokio::sync::mpsc;

use crate::gateway::Node;
use crate::manager::{DependencyManager, MaintenanceReport};
use crate::Error;

/// The device of the notice in the container: the one that the packer declares by default.
pub const DEVICE: &str = "/dev/hvc0";

/// A line is short: the node writes 128 bytes at most. A longer one is not a notice, and the
/// reader drops it without keeping it in memory.
pub const MAX_LINE_BYTES: usize = 4096;

/// `O_NOCTTY` on Linux for amd64 and arm64, the two architectures of a node. The device is a
/// tty: without this flag, opening it can make it the controlling terminal of the service.
const O_NOCTTY: i32 = 0o400;

/// `O_NONBLOCK`, with the same values. The device is read through the poller of the runtime,
/// and not by a thread that waits in `read`: such a thread has no end while the device is
/// silent, and a process that has one cannot end its runtime.
const O_NONBLOCK: i32 = 0o4000;

/// What the node will do at the time of the notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    /// The instance is stopped: its VM ends and its disk is deleted.
    Stop,
    /// The instance is hibernated: its VM ends and its disk is kept. At the wake the
    /// entrypoint runs again on the same disk, and the memory and the processes are lost.
    Hibernate,
}

impl Verb {
    /// The word that the node writes.
    pub fn as_word(&self) -> &'static str {
        match self {
            Verb::Stop => "STOP",
            Verb::Hibernate => "HIBERNATE",
        }
    }
}

/// One notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Notice {
    /// What the node will do.
    pub verb: Verb,
    /// The unix time, in seconds, at which it does it whatever happens.
    pub deadline: u64,
}

impl Notice {
    /// The notice of a line, or `None` if the line is not one: a verb that this version does
    /// not know, a time that is not a whole number, or other words in the line. The node may
    /// write the end of line as `\n` or `\r\n`, and the tty of the guest may echo it, so
    /// spaces around the line are not a fault.
    pub fn parse(line: &str) -> Option<Notice> {
        let mut words = line.split_whitespace();
        let verb = match words.next()? {
            "STOP" => Verb::Stop,
            "HIBERNATE" => Verb::Hibernate,
            _ => return None,
        };
        let deadline = words.next()?.parse::<u64>().ok()?;
        if words.next().is_some() {
            return None;
        }
        Some(Notice { verb, deadline })
    }

    /// How long is left at the unix time `now` (seconds). Zero if the time has passed.
    pub fn remaining_at(&self, now: u64) -> Duration {
        Duration::from_secs(self.deadline.saturating_sub(now))
    }

    /// How long is left now. Zero if the time has passed.
    pub fn remaining(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.remaining_at(now)
    }
}

/// The notices that a device gives, in order. Drop it to stop reading.
#[derive(Debug)]
pub struct Notices {
    /// The notices. The channel closes when the device ends (the VM is ending) or fails.
    pub notices: mpsc::Receiver<Notice>,
}

impl Notices {
    /// The next notice, or `None` when the device has ended.
    pub async fn next(&mut self) -> Option<Notice> {
        self.notices.recv().await
    }
}

/// Read the notices from the device at `path` (normally [`DEVICE`]). The device is opened
/// once and kept open until the reader ends or the result is dropped.
///
/// A tty is read through the poller of the runtime, so nothing waits in a thread and the
/// process can end with the device open and silent. A regular file (a test, a captured log) is
/// read to its end.
///
/// Fails if the device cannot be opened: the service did not declare a notice, or it runs on
/// a node that did not give it the console.
pub async fn watch_device(path: impl AsRef<Path>) -> Result<Notices, Error> {
    let path = path.as_ref();
    let fail = |e: io::Error| {
        Error::Notice(format!(
            "{} cannot be opened: {e}. The service has to declare `notice_declaration` \
             in its service.json, and the node has to carry it.",
            path.display()
        ))
    };
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(O_NOCTTY | O_NONBLOCK)
        .open(path)
        .map_err(fail)?;
    if file.metadata().map_err(fail)?.is_file() {
        return Ok(watch_reader(tokio::fs::File::from_std(file)));
    }
    // SAFETY: the descriptor is the one of `file`, which the `AsyncFd` owns from here: it is
    // open until the `AsyncFd` is dropped, nothing else holds it, and it never changes.
    let device = unsafe { AsyncFd::register_with_interest(file, Interest::READABLE) }
        .map_err(|e| fail(e.into()))?;
    Ok(watch_reader(Pollable(device)))
}

/// A device that the poller of the runtime reads: `read` returns when a line is there, and
/// no thread waits for it.
struct Pollable(AsyncFd<std::fs::File>);

impl AsyncRead for Pollable {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            let mut ready = match this.0.poll_read_ready(cx) {
                Poll::Ready(ready) => ready?,
                Poll::Pending => return Poll::Pending,
            };
            let unfilled = buf.initialize_unfilled();
            match ready.try_io(|inner| {
                let mut device: &std::fs::File = inner.get_ref();
                device.read(unfilled)
            }) {
                Ok(Ok(n)) => {
                    buf.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(Err(e)) => return Poll::Ready(Err(e)),
                // Not ready after all: the poller has been told, and it wakes us again.
                Err(_would_block) => continue,
            }
        }
    }
}

/// Read the notices from any reader. It is what [`watch_device`] runs on the device, and
/// what a test runs on a pipe. The task ends at the end of the reader, at a read error, or
/// when the result is dropped.
pub fn watch_reader<R>(reader: R) -> Notices
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        let mut reader = BufReader::new(reader);
        let mut line = Vec::new();
        loop {
            match next_line(&mut reader, &mut line).await {
                Ok(true) => {
                    let Ok(text) = std::str::from_utf8(&line) else {
                        continue;
                    };
                    if let Some(notice) = Notice::parse(text) {
                        // A reader that went away ends the task.
                        if tx.send(notice).await.is_err() {
                            return;
                        }
                    }
                }
                // The end of the device, or a failure of it: nothing more can come.
                Ok(false) | Err(_) => return,
            }
        }
    });
    Notices { notices: rx }
}

/// Fill `line` with the next line (without its end of line). `Ok(false)` at the end of the
/// reader. A line longer than [`MAX_LINE_BYTES`] comes back empty: it is dropped as it is
/// read, so a guest process that writes to the device without end cannot fill the memory.
async fn next_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> io::Result<bool> {
    line.clear();
    let mut too_long = false;
    let mut read_any = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            // The end: a last line with no end of line is still a line.
            return Ok(read_any && !too_long);
        }
        read_any = true;
        let (used, done) = match available.iter().position(|b| *b == b'\n') {
            Some(i) => (i + 1, true),
            None => (available.len(), false),
        };
        if !too_long {
            if line.len() + used > MAX_LINE_BYTES {
                too_long = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..used]);
            }
        }
        reader.consume(used);
        if done {
            if too_long {
                line.clear();
            }
            return Ok(true);
        }
    }
}

/// What [`shutdown_on_notice`] did.
#[derive(Debug, PartialEq, Eq)]
pub struct Handled {
    /// The notice that it answered.
    pub notice: Notice,
    /// What the shutdown of the pool did, or `None` if the time of the notice ran out first:
    /// then some instances may still run and bill.
    pub report: Option<MaintenanceReport>,
}

/// Wait for the next notice, and when it comes (`STOP` or `HIBERNATE`) stop the idle instances
/// of the pool within the time that the notice gives. `None` if the device ends with no notice.
///
/// Both verbs stop the dependencies: a hibernation ends the VM too, the memory with the tokens
/// of the pool is lost, and the entrypoint runs again at the wake with an empty pool. An
/// instance that nobody stops keeps running and billing.
pub async fn shutdown_on_notice<N: Node>(
    manager: &DependencyManager<N>,
    notices: &mut Notices,
) -> Option<Handled> {
    let notice = notices.next().await?;
    let report = tokio::time::timeout(notice.remaining(), manager.shutdown())
        .await
        .ok();
    Some(Handled { notice, report })
}

/// [`shutdown_on_notice`] in a task. It ends after the first notice. The service stays alive
/// until the node stops the VM, so the task's result tells what the shutdown did.
pub fn spawn_shutdown_on_notice<N>(
    manager: &Arc<DependencyManager<N>>,
    mut notices: Notices,
) -> tokio::task::JoinHandle<Option<Handled>>
where
    N: Node,
{
    let manager = manager.clone();
    tokio::spawn(async move { shutdown_on_notice(&manager, &mut notices).await })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn the_two_verbs_with_a_time_are_notices() {
        assert_eq!(
            Notice::parse("STOP 1794399680"),
            Some(Notice {
                verb: Verb::Stop,
                deadline: 1794399680
            })
        );
        assert_eq!(
            Notice::parse("HIBERNATE 5"),
            Some(Notice {
                verb: Verb::Hibernate,
                deadline: 5
            })
        );
    }

    #[test]
    fn the_end_of_line_and_the_spaces_are_not_a_fault() {
        let stop = Some(Notice {
            verb: Verb::Stop,
            deadline: 7,
        });
        assert_eq!(Notice::parse("STOP 7\n"), stop);
        assert_eq!(Notice::parse("STOP 7\r\n"), stop);
        assert_eq!(Notice::parse("  STOP   7  "), stop);
    }

    #[test]
    fn a_verb_that_this_version_does_not_know_is_ignored() {
        assert_eq!(Notice::parse("SNAPSHOT 5"), None);
        assert_eq!(Notice::parse("stop 5"), None);
    }

    #[test]
    fn a_line_that_is_not_a_notice_is_not_one() {
        for line in [
            "",
            "STOP",
            "STOP five",
            "STOP -1",
            "STOP 5 6",
            "5 STOP",
            "STOP 1.5",
        ] {
            assert_eq!(Notice::parse(line), None, "{line:?}");
        }
    }

    #[test]
    fn the_time_left_never_goes_below_zero() {
        let notice = Notice {
            verb: Verb::Stop,
            deadline: 100,
        };
        assert_eq!(notice.remaining_at(90), Duration::from_secs(10));
        assert_eq!(notice.remaining_at(100), Duration::ZERO);
        assert_eq!(notice.remaining_at(500), Duration::ZERO);
    }

    #[test]
    fn the_words_are_the_ones_of_the_node() {
        assert_eq!(Verb::Stop.as_word(), "STOP");
        assert_eq!(Verb::Hibernate.as_word(), "HIBERNATE");
    }

    #[tokio::test]
    async fn the_notices_of_a_reader_come_in_order_and_what_is_not_one_is_skipped() {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let mut notices = watch_reader(reader);
        writer
            .write_all(b"garbage\nSTOP 10\r\nSNAPSHOT 3\n\nHIBERNATE 20\n")
            .await
            .unwrap();
        assert_eq!(notices.next().await.unwrap().deadline, 10);
        assert_eq!(notices.next().await.unwrap().verb, Verb::Hibernate);
        drop(writer);
        assert_eq!(notices.next().await, None); // the device ended
    }

    #[tokio::test]
    async fn a_line_that_comes_in_pieces_is_one_line() {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let mut notices = watch_reader(reader);
        writer.write_all(b"ST").await.unwrap();
        tokio::task::yield_now().await;
        writer.write_all(b"OP 4").await.unwrap();
        tokio::task::yield_now().await;
        writer.write_all(b"2\n").await.unwrap();
        assert_eq!(notices.next().await.unwrap().deadline, 42);
    }

    #[tokio::test]
    async fn a_last_line_with_no_end_of_line_is_read() {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let mut notices = watch_reader(reader);
        writer.write_all(b"STOP 9").await.unwrap();
        drop(writer);
        assert_eq!(notices.next().await.unwrap().deadline, 9);
        assert_eq!(notices.next().await, None);
    }

    #[tokio::test]
    async fn a_guest_that_writes_without_end_of_line_does_not_hold_the_memory_or_hide_the_next_notice(
    ) {
        let (mut writer, reader) = tokio::io::duplex(8192);
        let mut notices = watch_reader(reader);
        let junk = vec![b'x'; MAX_LINE_BYTES * 5];
        writer.write_all(&junk).await.unwrap();
        writer.write_all(b"\nSTOP 11\n").await.unwrap();
        assert_eq!(notices.next().await.unwrap().deadline, 11);
    }

    #[tokio::test]
    async fn the_device_is_opened_once_and_read_as_a_file() {
        let dir = std::env::temp_dir().join(format!("celaut-notice-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("hvc0");
        std::fs::write(&path, "HIBERNATE 12\nSTOP 13\n").unwrap();
        let mut notices = watch_device(&path).await.unwrap();
        assert_eq!(notices.next().await.unwrap().deadline, 12);
        assert_eq!(notices.next().await.unwrap().deadline, 13);
        assert_eq!(notices.next().await, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_missing_device_says_what_the_service_has_to_declare() {
        let error = watch_device("/nonexistent/hvc0").await.unwrap_err();
        assert!(matches!(error, Error::Notice(_)));
        assert!(error.to_string().contains("notice_declaration"));
    }

    use crate::manager::Limits;
    use celaut_proto::celaut::instance::{Uri, UriSlot};
    use celaut_proto::celaut::service::{api::Slot, Api};
    use celaut_proto::celaut::{Configuration, Instance, ServiceInstance};
    use std::sync::Mutex;

    /// A node that starts instances with an address, and stops them after `stop_delay`.
    #[derive(Default)]
    struct FakeNode {
        stopped: Mutex<Vec<String>>,
        stop_delay: Duration,
    }

    impl Node for Arc<FakeNode> {
        async fn start(
            &self,
            _hash: [u8; 32],
            _c: Configuration,
        ) -> Result<ServiceInstance, Error> {
            Ok(ServiceInstance {
                token: "t0".to_string(),
                instance: Some(Instance {
                    api: Some(Api {
                        slot: vec![Slot {
                            port: 50051,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    uri_slot: vec![UriSlot {
                        internal_port: 50051,
                        uri: vec![Uri {
                            ip: "10.0.0.2".into(),
                            port: 40000,
                        }],
                    }],
                }),
                receipt: None,
            })
        }

        async fn start_package(
            &self,
            _path: &std::path::Path,
            info: &crate::package::PackageInfo,
            c: Configuration,
        ) -> Result<ServiceInstance, Error> {
            self.start(info.service_id, c).await
        }

        async fn stop(&self, token: &str) -> Result<u128, Error> {
            tokio::time::sleep(self.stop_delay).await;
            self.stopped.lock().unwrap().push(token.to_string());
            Ok(0)
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// A pool with one idle instance, and a device that gives `lines`.
    async fn idle_pool(
        node: &Arc<FakeNode>,
        lines: &str,
    ) -> (Arc<DependencyManager<Arc<FakeNode>>>, Notices) {
        let manager = DependencyManager::new(node.clone(), Limits::default());
        let dependency = manager.add_service([1; 32], Configuration::default());
        drop(dependency.acquire().await.unwrap()); // back to the pool: idle
        let (mut writer, reader) = tokio::io::duplex(1024);
        writer.write_all(lines.as_bytes()).await.unwrap();
        (manager, watch_reader(DuplexOwner::new(reader, writer)))
    }

    /// Keeps the writer open, so that the reader does not see the end before a test wants it.
    struct DuplexOwner {
        reader: tokio::io::DuplexStream,
        _writer: tokio::io::DuplexStream,
    }

    impl DuplexOwner {
        fn new(reader: tokio::io::DuplexStream, writer: tokio::io::DuplexStream) -> Self {
            DuplexOwner {
                reader,
                _writer: writer,
            }
        }
    }

    impl AsyncRead for DuplexOwner {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.reader).poll_read(cx, buf)
        }
    }

    #[tokio::test]
    async fn a_stop_notice_stops_the_idle_instances_of_the_pool() {
        let node = Arc::new(FakeNode::default());
        let (manager, mut notices) = idle_pool(&node, &format!("STOP {}\n", now() + 60)).await;
        let handled = shutdown_on_notice(&manager, &mut notices).await.unwrap();
        assert_eq!(handled.notice.verb, Verb::Stop);
        assert_eq!(handled.report.unwrap().stopped, 1);
        assert_eq!(*node.stopped.lock().unwrap(), vec!["t0".to_string()]);
    }

    #[tokio::test]
    async fn a_hibernate_notice_stops_them_too() {
        let node = Arc::new(FakeNode::default());
        let (manager, mut notices) = idle_pool(&node, &format!("HIBERNATE {}\n", now() + 60)).await;
        let handled = shutdown_on_notice(&manager, &mut notices).await.unwrap();
        assert_eq!(handled.notice.verb, Verb::Hibernate);
        assert_eq!(handled.report.unwrap().stopped, 1);
    }

    #[tokio::test]
    async fn a_line_that_is_not_a_notice_does_not_stop_anything() {
        let node = Arc::new(FakeNode::default());
        let (manager, mut notices) =
            idle_pool(&node, &format!("SNAPSHOT {}\nhello\n", now() + 60)).await;
        let waited = tokio::time::timeout(
            Duration::from_millis(200),
            shutdown_on_notice(&manager, &mut notices),
        )
        .await;
        assert!(waited.is_err(), "no notice came, so it keeps waiting");
        assert!(node.stopped.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_shutdown_has_the_time_of_the_notice_and_no_more() {
        let node = Arc::new(FakeNode {
            stop_delay: Duration::from_secs(30),
            ..Default::default()
        });
        // The time of the notice has passed: the shutdown gets none, and says so.
        let (manager, mut notices) = idle_pool(&node, &format!("STOP {}\n", now() - 5)).await;
        let started = std::time::Instant::now();
        let handled = shutdown_on_notice(&manager, &mut notices).await.unwrap();
        assert!(handled.report.is_none());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn a_device_that_ends_with_no_notice_gives_none() {
        let node = Arc::new(FakeNode::default());
        let manager = DependencyManager::new(node.clone(), Limits::default());
        let (writer, reader) = tokio::io::duplex(64);
        drop(writer);
        let mut notices = watch_reader(reader);
        assert!(shutdown_on_notice(&manager, &mut notices).await.is_none());
    }

    #[tokio::test]
    async fn the_task_gives_what_the_shutdown_did() {
        let node = Arc::new(FakeNode::default());
        let (manager, notices) = idle_pool(&node, &format!("STOP {}\n", now() + 60)).await;
        let handled = spawn_shutdown_on_notice(&manager, notices)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handled.report.unwrap().stopped, 1);
    }
}

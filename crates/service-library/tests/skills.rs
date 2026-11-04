//! The skills registry reader against an in-process Explorer that answers a box search as the
//! real one does.

use serde_json::{json, Value};
use service_library::skills::{
    InfoMode, Skills, BENCHMARK_TYPE_ID, COVERAGE_TYPE_ID, RESULT_TYPE_ID, SKILL_TYPE_ID,
};
use service_library::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const SKILL: &str = "aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11";
const BENCH: &str = "bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22";
const SVC_A: &str = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a";
const SVC_B: &str = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b";

/// A box of the Explorer: R9 is JSON, rendered as hex.
fn explorer_box(box_id: &str, r9: Value) -> Value {
    json!({
        "boxId": box_id,
        "assets": [{"tokenId": "ee".repeat(32)}],
        "additionalRegisters": {"R9": {"renderedValue": hex::encode(r9.to_string())}},
    })
}

/// The boxes that the fake Explorer holds, by (R4, R5).
fn registry(r4: &str, r5: Option<&str>) -> Vec<Value> {
    match (r4, r5) {
        (t, None) if t == SKILL_TYPE_ID => vec![explorer_box(
            SKILL,
            json!({"name": "Sat-sorter", "prose": "sort sats", "tags": ["utxo"],
                   "extended_skill_boxes": [], "profile_id": "pp"}),
        )],
        (t, Some(SKILL)) if t == COVERAGE_TYPE_ID => {
            vec![explorer_box("c1", json!({"service_id": SVC_A}))]
        }
        (t, Some(SKILL)) if t == BENCHMARK_TYPE_ID => vec![explorer_box(
            BENCH,
            json!({"name": "12 months",
                   "case_descriptors": [{"name": "case0", "description": "d"}],
                   "performance_metrics": [{"name": "ms", "description": "time", "higher_is_better": false}]}),
        )],
        (t, Some(BENCH)) if t == RESULT_TYPE_ID => vec![
            explorer_box(
                "r1",
                json!({"service_id": SVC_A, "notes": "n", "timestamp": 5,
                       "data": [{"case_meta": [1], "metrics_values": ["2.5"]}]}),
            ),
            explorer_box("r2", json!({"service_id": SVC_B, "data": []})),
        ],
        _ => vec![],
    }
}

/// Serve one connection at a time. `chunked`: answer with `Transfer-Encoding: chunked`.
/// `fail`: answer that status instead. Sends back what each request asked for in `seen`.
async fn serve(
    chunked: bool,
    fail: Option<u16>,
) -> (
    String,
    tokio::sync::mpsc::UnboundedReceiver<(String, Value)>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/explorer", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            let (head, body) = loop {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                raw.extend_from_slice(&buf[..n]);
                if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&raw[..end]).to_string();
                    let length: usize = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if raw.len() >= end + 4 + length {
                        break (head, raw[end + 4..end + 4 + length].to_vec());
                    }
                }
            };
            let path = head.lines().next().unwrap().to_string();
            let request: Value = serde_json::from_slice(&body).unwrap();
            tx.send((path.clone(), request.clone())).unwrap();

            if let Some(status) = fail {
                let msg = "nope";
                let _ = socket
                    .write_all(
                        format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{msg}", msg.len())
                            .as_bytes(),
                    )
                    .await;
                continue;
            }
            let r4 = request["registers"]["R4"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let r5 = request["registers"]["R5"].as_str().map(str::to_string);
            let items = registry(&r4, r5.as_deref());
            let payload = json!({"items": items, "total": items.len()}).to_string();
            let answer = if chunked {
                let (a, b) = payload.split_at(payload.len() / 2);
                format!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{a}\r\n{:x}\r\n{b}\r\n0\r\n\r\n",
                    a.len(),
                    b.len()
                )
            } else {
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                )
            };
            let _ = socket.write_all(answer.as_bytes()).await;
        }
    });
    (uri, rx)
}

#[tokio::test]
async fn the_skill_tree_is_what_the_registry_holds() {
    let (uri, mut seen) = serve(false, None).await;
    let skills = Skills::new(&uri).unwrap();

    let list = skills.load_skills().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].box_id, SKILL);
    assert_eq!(list[0].name, "Sat-sorter");
    assert_eq!(list[0].profile_id, "pp");
    assert_eq!(list[0].tags, ["utxo"]);

    // The search: the prefix of the Explorer, the contract, the registers as hex.
    let (path, request) = seen.recv().await.unwrap();
    assert_eq!(
        path,
        "POST /explorer/api/v1/boxes/unspent/search?offset=0&limit=100 HTTP/1.1"
    );
    assert_eq!(request["registers"]["R4"], SKILL_TYPE_ID);
    assert!(request["registers"].get("R5").is_none());
    assert_eq!(request["ergoTreeTemplateHash"].as_str().unwrap().len(), 64);

    let tree = skills.load_skill_tree(SKILL).await.unwrap();
    assert_eq!(tree.skill.unwrap().name, "Sat-sorter");
    // SVC_A is covered directly and by a result: once. SVC_B only by a result.
    let services: Vec<_> = tree
        .coverages
        .iter()
        .map(|c| c.service_id.clone().unwrap())
        .collect();
    assert_eq!(services, [SVC_A, SVC_B]);
    assert_eq!(tree.coverages[0].box_id, "c1");
    assert_eq!(tree.coverages[1].box_id, "r2");

    let bench = &tree.benchmarks[0];
    assert_eq!(bench.benchmark.name, "12 months");
    assert_eq!(
        bench.benchmark.performance_metrics[0].higher_is_better,
        Some(false)
    );
    assert_eq!(bench.results.len(), 2);
    assert_eq!(bench.results[0].data[0].metrics_values, [2.5]);
    assert_eq!(bench.results[0].timestamp, 5);
}

#[tokio::test]
async fn a_chunked_answer_is_read() {
    let (uri, _seen) = serve(true, None).await;
    let list = Skills::new(&uri).unwrap().load_skills().await.unwrap();
    assert_eq!(list[0].name, "Sat-sorter");
}

#[tokio::test]
async fn ids_that_are_not_hex_find_nothing_and_ask_nothing() {
    let (uri, mut seen) = serve(false, None).await;
    let skills = Skills::new(&uri).unwrap();
    // The types of service data and metadata are placeholders in the registry.
    assert!(skills.service_data(SVC_A).await.unwrap().is_empty());
    assert!(skills.service_metadata(SVC_A).await.unwrap().is_empty());
    assert!(skills.load_benchmarks("not hex").await.unwrap().is_empty());
    assert!(seen.try_recv().is_err());
}

#[tokio::test]
async fn an_explorer_that_refuses_is_an_error_with_its_status() {
    let (uri, _seen) = serve(false, Some(400)).await;
    let error = Skills::new(&uri).unwrap().load_skills().await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Registry {
                status: Some(400),
                unreachable: false,
                ..
            }
        ),
        "{error}"
    );
    assert!(!error.is_unreachable());
}

#[tokio::test]
async fn an_explorer_that_does_not_answer_is_unreachable() {
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = closed.local_addr().unwrap().to_string();
    drop(closed);
    let error = Skills::new(&uri).unwrap().load_skills().await.unwrap_err();
    assert!(error.is_unreachable(), "{error}");
}

#[test]
fn the_address_of_the_explorer_is_checked() {
    assert!(Skills::new("").is_err());
    assert!(Skills::new("http://").is_err());
    assert!(Skills::new("127.0.0.1:notaport").is_err());
    assert!(Skills::new("127.0.0.1:9053").is_ok());
    assert!(Skills::new("http://[::1]:9053/prefix/").is_ok());
    #[cfg(not(feature = "tls"))]
    assert!(Skills::new("https://api.ergoplatform.com").is_err());
    #[cfg(feature = "tls")]
    assert!(Skills::new("https://api.ergoplatform.com").is_ok());
    let _ = InfoMode::Source;
}

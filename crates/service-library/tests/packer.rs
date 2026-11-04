//! The packer client against an in-process HTTP server that answers as the packer service does.

use std::fs::File;
use std::path::PathBuf;

use bee_rpc::bee_file::write_bee;
use bee_rpc::frame::{serialize_part, Part};
use celaut_proto::celaut::metadata::hash_tag::Hash;
use celaut_proto::celaut::metadata::HashTag;
use celaut_proto::celaut::Metadata;
use prost::Message;
use service_library::gateway::SHA3_256_TYPE;
use service_library::packer::Packer;
use service_library::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const ID: [u8; 32] = [7; 32];

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("packer-test-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn bee_bytes(dir: &std::path::Path) -> Vec<u8> {
    let path = dir.join("fixture.bee");
    let metadata = Metadata {
        hashtag: Some(HashTag {
            hash: vec![Hash {
                r#type: SHA3_256_TYPE.to_vec(),
                value: ID.to_vec(),
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let mut buffers = serialize_part(&Part::new(1, metadata)).collect::<Vec<_>>();
    buffers.extend(serialize_part(&Part::new(2, vec![1, 2, 3])));
    write_bee(&mut File::create(&path).unwrap(), buffers).unwrap();
    std::fs::read(path).unwrap()
}

/// What the fake packer does with the next `/pack`.
#[derive(Clone, Copy)]
enum Mode {
    Good,
    Chunked,
    WrongId,
    Status(u16, &'static str),
}

/// Serve one connection at a time; reply to `/health`, `/pack` and `/registry/<id>`.
async fn serve(mode: Mode, bee: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let bee = bee.clone();
            tokio::spawn(async move {
                // The head, then the body by Content-Length.
                let mut data = Vec::new();
                let mut byte = [0u8; 1];
                while !data.ends_with(b"\r\n\r\n") {
                    if socket.read(&mut byte).await.unwrap() == 0 {
                        return;
                    }
                    data.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&data).to_string();
                let request = head.lines().next().unwrap().to_string();
                let length = head
                    .lines()
                    .find_map(|l| l.strip_prefix("Content-Length: "))
                    .map(|v| v.parse::<usize>().unwrap())
                    .unwrap_or(0);
                let mut body = vec![0u8; length];
                if request.starts_with("POST /pack") && matches!(mode, Mode::Status(413, _)) {
                    // Refuses before it reads the file.
                } else {
                    socket.read_exact(&mut body).await.unwrap();
                }
                let reply = |status: &str, headers: &str, body: &[u8]| {
                    let mut out = format!(
                        "HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .into_bytes();
                    out.extend_from_slice(body);
                    out
                };
                let answer = if request.starts_with("GET /health") {
                    reply("200 OK", "", b"ok")
                } else if request.starts_with("GET /registry/") {
                    if request.contains(&"07".repeat(32)) {
                        reply("200 OK", "", br#"{"present": true}"#)
                    } else {
                        reply("404 Not Found", "", b"{}")
                    }
                } else if request.starts_with("POST /registry/") {
                    reply("200 OK", "", b"{}")
                } else if request.starts_with("POST /pack") {
                    assert!(head.contains("Content-Type: application/zip"));
                    if !matches!(mode, Mode::Status(413, _)) {
                        assert_eq!(body, b"PK-zip");
                    }
                    let id = hex::encode(ID);
                    match mode {
                        Mode::Good => reply("200 OK", &format!("X-Service-Id: {id}\r\n"), &bee),
                        Mode::WrongId => {
                            reply("200 OK", &format!("X-Service-Id: {}\r\n", "08".repeat(32)), &bee)
                        }
                        Mode::Chunked => {
                            let mut out = format!(
                                "HTTP/1.1 200 OK\r\nX-Service-Id: {id}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                            )
                            .into_bytes();
                            for part in bee.chunks(5) {
                                out.extend_from_slice(format!("{:x}\r\n", part.len()).as_bytes());
                                out.extend_from_slice(part);
                                out.extend_from_slice(b"\r\n");
                            }
                            out.extend_from_slice(b"0\r\n\r\n");
                            out
                        }
                        Mode::Status(code, text) => reply(&format!("{code} X"), "", text.as_bytes()),
                    }
                } else {
                    reply("404 Not Found", "", b"")
                };
                let _ = socket.write_all(&answer).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    uri
}

#[tokio::test]
async fn health_and_a_good_pack() {
    let d = dir("good");
    let bee = bee_bytes(&d);
    let packer = Packer::new(&format!("http://{}/", serve(Mode::Good, bee.clone()).await));
    packer.health().await.unwrap();
    std::fs::write(d.join("p.zip"), b"PK-zip").unwrap();
    let packed = packer.pack(&d.join("p.zip"), &d.join("out.celaut.bee")).await.unwrap();
    assert_eq!(packed.service_id, hex::encode(ID));
    assert_eq!(packed.size, bee.len() as u64);
    assert_eq!(std::fs::read(&packed.path).unwrap(), bee);
    assert!(!d.join("out.celaut.bee.part").exists());
}

#[tokio::test]
async fn a_chunked_answer_is_the_same_file() {
    let d = dir("chunked");
    let bee = bee_bytes(&d);
    let packer = Packer::new(&serve(Mode::Chunked, bee.clone()).await);
    std::fs::write(d.join("p.zip"), b"PK-zip").unwrap();
    let packed = packer.pack(&d.join("p.zip"), &d.join("out.bee")).await.unwrap();
    assert_eq!(std::fs::read(&packed.path).unwrap(), bee);
}

#[tokio::test]
async fn an_id_that_the_file_does_not_name_leaves_no_file() {
    let d = dir("wrong");
    let bee = bee_bytes(&d);
    let packer = Packer::new(&serve(Mode::WrongId, bee).await);
    std::fs::write(d.join("p.zip"), b"PK-zip").unwrap();
    let error = packer.pack(&d.join("p.zip"), &d.join("out.bee")).await.unwrap_err();
    assert!(error.to_string().contains("the packer said"), "{error}");
    assert!(!d.join("out.bee").exists());
    assert!(!d.join("out.bee.part").exists());
}

#[tokio::test]
async fn the_errors_of_the_packer_keep_their_status_and_text() {
    let d = dir("errors");
    let bee = bee_bytes(&d);
    std::fs::write(d.join("p.zip"), b"PK-zip").unwrap();
    for (code, text) in [(400, "1. no Dockerfile"), (415, "not a zip"), (413, "too large")] {
        let packer = Packer::new(&serve(Mode::Status(code, text), bee.clone()).await);
        match packer.pack(&d.join("p.zip"), &d.join("out.bee")).await {
            Err(Error::Packer { status, message, unreachable, .. }) => {
                assert_eq!(status, Some(code));
                assert_eq!(message, text);
                assert!(!unreachable);
            }
            other => panic!("{code}: {other:?}"),
        }
        assert!(!d.join("out.bee").exists());
    }
}

#[tokio::test]
async fn an_instance_that_does_not_answer_is_unreachable() {
    // A port that nobody listens on.
    let free = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = free.local_addr().unwrap().to_string();
    drop(free);
    let error = Packer::new(&uri).health().await.unwrap_err();
    assert!(error.is_unreachable(), "{error}");
}

#[tokio::test]
async fn the_registry_of_the_packer() {
    let d = dir("registry");
    let packer = Packer::new(&serve(Mode::Good, bee_bytes(&d)).await);
    assert!(packer.has_dependency(&hex::encode(ID)).await.unwrap());
    assert!(!packer.has_dependency(&"09".repeat(32)).await.unwrap());
    assert!(packer.has_dependency("../etc").await.is_err());
    std::fs::write(d.join("dep.tgz"), b"gz").unwrap();
    packer.upload_dependency(&hex::encode(ID), &d.join("dep.tgz")).await.unwrap();
}

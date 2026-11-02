//! Runs inside a node as a service. It starts the mock service, which it carries as a
//! dependency (`__services__`, `__metadata__`, `__block__`), calls it, and stops it.

use std::time::Duration;

use celaut_proto::celaut::Configuration;
use service_library::config::NodeConfig;
use service_library::gateway::Gateway;
use service_library::manager::{DependencyManager, Limits, ReceiptCheck};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ROOT: &str = "/";

type Res<T> = Result<T, Box<dyn std::error::Error>>;

async fn http_get(uri: &str) -> Res<String> {
    let mut s = tokio::net::TcpStream::connect(uri).await?;
    s.write_all(b"GET / HTTP/1.0\r\nHost: mock\r\n\r\n").await?;
    let mut out = Vec::new();
    s.read_to_end(&mut out).await?;
    Ok(String::from_utf8_lossy(&out).into_owned())
}

async fn run() -> Res<()> {
    let config = NodeConfig::load()?;
    println!("gateway {} mem_limit {}", config.gateway, config.mem_limit);
    let gateway = Gateway::new(&config.gateway)?;

    // `.dependencies` is written by the packer (`dependencies_env`): NAME=<hex id>.
    let deps = std::fs::read_to_string(format!("{ROOT}.dependencies"))?;
    let id_hex = deps
        .lines()
        .find_map(|l| l.strip_prefix("MOCK="))
        .ok_or("no MOCK in .dependencies")?
        .trim();
    let mut id = [0u8; 32];
    hex::decode_to_slice(id_hex, &mut id)?;
    println!("mock id {id_hex}");

    let manager = DependencyManager::with_receipts(
        gateway,
        Limits::default(),
        ReceiptCheck::Check {
            peer_id: None,
            require: false,
        },
    );
    let handle = manager
        .add_directory_package(
            ROOT,
            format!("{ROOT}__block__"),
            id,
            Configuration::default(),
        )
        .await?;

    println!("acquiring (the node may build and boot the mock first)");
    let mut lease = handle.acquire().await?;
    println!("instance at {}", lease.uri());

    // The mock listens on 8080 and may need a moment after the start.
    let mut last = String::new();
    for _ in 0..30 {
        match lease
            .call(Duration::from_secs(10), |uri| async move {
                http_get(&uri).await.map_err(|e| e.to_string())
            })
            .await
        {
            Ok(body) if body.contains("pong") => {
                println!("MOCK ANSWERED: {}", body.lines().last().unwrap_or(""));
                let refund = lease.stop().await?;
                println!("stopped, refund {refund}");
                let report = manager.shutdown().await;
                println!("shutdown {report:?}");
                println!("E2E OK");
                return Ok(());
            }
            Ok(body) => last = body,
            Err(e) => last = format!("{e:?}"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!("the mock never answered pong; last: {last}").into())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        println!("E2E FAILED: {e}");
    }
    // Keep the service up so the log can be read.
    tokio::time::sleep(Duration::from_secs(3600)).await;
}

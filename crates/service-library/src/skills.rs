//! A reader of the Celaut skills registry, to find a service by the problem it solves.
//!
//! [`celaut-project/skills`](https://github.com/celaut-project/skills) is a registry on the
//! Ergo blockchain. A *skill* names a problem, a *coverage* says that a service addresses it,
//! a *benchmark* says how to measure it and a *result* is the measure of one service. A
//! service that needs a capability asks the registry for the skill, takes the service ids
//! that cover it, and starts the best one as a dependency
//! ([`crate::manager::DependencyManager::add_service`]).
//!
//! This is the read side only, the same as the MCP server of that repository (`MCP.md`), and
//! it makes the same queries, so both read the same data. The registry has no server: every
//! read is a search of the boxes of the reputation contract in an
//! [Ergo Explorer](https://github.com/ergoplatform/explorer-backend) (`Skills::new`; the
//! public one is [`DEFAULT_EXPLORER`]). Publishing is not here: it needs the wallet of a
//! person to sign a transaction, and the registry has no command for it either.
//!
//! The public Explorer is `https`, so the feature `tls` has to be on for it (rustls with the
//! roots of Mozilla). Without it only `http://` works, for an Explorer of your own or a
//! proxy, and the static `celaut` binary stays smaller. The HTTP is the little that a
//! search needs: one JSON request, `Connection: close`, no HTTP crate.
//!
//! A search that finds nothing is an empty list, and a registry that does not answer is an
//! [`Error::Registry`] (with [`Error::is_unreachable`] true). The ids of the types of
//! service data and metadata are not minted yet (the registry has placeholders), and then
//! [`Skills::service_data`] and [`Skills::service_metadata`] give an empty list, as the MCP
//! server does.

use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Error;

/// The Type NFT of a skill (`celaut:skill:v1`).
pub const SKILL_TYPE_ID: &str = "ffce59c01b9c0c245005f9c2daf817607e912a3ececd5f61aaba48d30230f60c";
/// The Type NFT of a benchmark (`celaut:benchmark:v1`).
pub const BENCHMARK_TYPE_ID: &str =
    "f6480184daf3b7750a58e58319e12adc5266bd986eec0f57ac451c995a30f54d";
/// The Type NFT of a result (`celaut:result:v1`).
pub const RESULT_TYPE_ID: &str = "49b26dc06b1680769477264d2e8e9bf561005236cde3097630bffcff631b7aef";
/// The Type NFT of a coverage (`celaut:coverage:v1`).
pub const COVERAGE_TYPE_ID: &str =
    "1da6799e935cbb0fb14d359f06f23854c3d1bd509508948cc01b7b018dbbbdf5";
/// The Type NFT of the functional part of a service (`celaut:service-data:v1`). A
/// placeholder in the registry until it is minted: it is not hex, and a search gives nothing.
pub const SERVICE_DATA_TYPE_ID: &str = "celaut:service-data:v1";
/// The Type NFT of the description of a service (`celaut:service-metadata:v1`). A placeholder
/// too.
pub const SERVICE_METADATA_TYPE_ID: &str = "celaut:service-metadata:v1";

/// The public Ergo Explorer, which is what the registry uses unless `CELAUT_EXPLORER_API` says
/// another. It needs the feature `tls`.
pub const DEFAULT_EXPLORER: &str = "https://api.ergoplatform.com";

/// The hash of the template of the reputation contract. The Explorer needs it in the search
/// (without it, it answers 400).
const REPUTATION_TREE_HASH: &str =
    "e84b95d84a30df33aa258fe2b9d24c3e75e27a67c6453983c19703029112d147";

/// How many boxes a search asks for, at most (the same as the MCP server).
const SEARCH_LIMIT: usize = 100;
/// Boxes per request of the Explorer.
const PAGE: usize = 100;
/// Time to open the connection.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Time for a whole request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// The most that is read of an answer.
const MAX_BODY: u64 = 16 * 1024 * 1024;
/// The longest head of an answer.
const MAX_HEAD: usize = 64 * 1024;

/// A problem that services can solve.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Skill {
    /// The id of its box: what the other calls ask for.
    pub box_id: String,
    /// Who published it: the token of its reputation proof.
    pub profile_id: String,
    pub name: String,
    /// The problem in words.
    pub prose: String,
    /// The problem in a formal language, if it has one.
    pub formal: String,
    pub tags: Vec<String>,
    pub domain: String,
    /// The skills that this one extends.
    pub extended_skill_box_ids: Vec<String>,
    pub source_hash: Option<String>,
}

/// The assertion that a service addresses a skill.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Coverage {
    pub box_id: String,
    pub profile_id: String,
    /// The id of the service, in hex, which [`crate::manager::DependencyManager::add_service`]
    /// takes. A coverage from a result has it always.
    pub service_id: Option<String>,
    /// The benchmark that the coverage points at, if it is about one benchmark of the skill
    /// and not about the whole skill.
    pub benchmark_id: Option<String>,
}

/// One case of a benchmark.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CaseDescriptor {
    pub name: String,
    pub description: String,
}

/// One number that a benchmark measures.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Metric {
    pub name: String,
    pub description: String,
    /// `None` if the publisher did not say.
    pub higher_is_better: Option<bool>,
}

/// How to measure a skill.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Benchmark {
    pub id: String,
    pub profile_id: String,
    pub skill_box_id: String,
    pub name: String,
    pub description: String,
    pub case_descriptors: Vec<CaseDescriptor>,
    pub performance_metrics: Vec<Metric>,
    pub source_hash: Option<String>,
}

/// The measures of one case: the numbers go by position, as the case descriptors and the
/// metrics of the benchmark do.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaseData {
    pub case_meta: Vec<f64>,
    pub metrics_values: Vec<f64>,
}

/// The measure of one service in a benchmark. (Named `Measure` so it does not clash with
/// `std::result::Result`.)
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Measure {
    pub id: String,
    pub profile_id: String,
    pub benchmark_id: String,
    pub service_id: String,
    pub data: Vec<CaseData>,
    pub notes: String,
    pub timestamp: u64,
    pub source_hash: Option<String>,
}

/// A benchmark with its measures.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BenchmarkTree {
    #[serde(flatten)]
    pub benchmark: Benchmark,
    pub results: Vec<Measure>,
}

/// A skill with the services that cover it and how they were measured.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkillTree {
    /// `None` if there is no skill with that box id.
    pub skill: Option<Skill>,
    pub coverages: Vec<Coverage>,
    pub benchmarks: Vec<BenchmarkTree>,
}

/// Where the content of a service box is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InfoMode {
    /// In the box, as JSON.
    Inline,
    /// Off the chain: the box has only the blake2b256 of it (`source_hash`), and it is looked
    /// up in the source registry.
    Source,
}

/// A part of the specification of a service, from the chain, so it is known without
/// downloading the service.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceData {
    pub box_id: String,
    pub profile_id: String,
    pub service_id: String,
    pub mode: InfoMode,
    pub source_hash: Option<String>,
    /// What the box has (`container`, `api`, `network`), as it is. Empty in `Source` mode.
    pub content: Value,
    pub architecture: Option<String>,
}

/// What the publishers say a service is.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceMetadata {
    pub box_id: String,
    pub profile_id: String,
    pub service_id: String,
    pub mode: InfoMode,
    pub source_hash: Option<String>,
    /// What the box has, as it is. Empty in `Source` mode.
    pub content: Value,
    pub name: Option<String>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
}

/// The registry, as read from an Explorer.
#[derive(Debug, Clone)]
pub struct Skills {
    explorer: Endpoint,
}

#[derive(Debug, Clone)]
struct Endpoint {
    tls: bool,
    host: String,
    port: u16,
    /// What goes before `/api/v1/...`, with no final slash.
    prefix: String,
}

impl Skills {
    /// The registry as the Explorer at `explorer` shows it: `https://host[:port][/prefix]`
    /// (needs the feature `tls`), `http://host[:port][/prefix]`, or `host:port` for plain
    /// HTTP. [`DEFAULT_EXPLORER`] is the public one.
    pub fn new(explorer: &str) -> Result<Self, Error> {
        Ok(Skills {
            explorer: Endpoint::parse(explorer)?,
        })
    }

    /// The registry as the public Explorer shows it, or the one of `CELAUT_EXPLORER_API`.
    pub fn from_env() -> Result<Self, Error> {
        match std::env::var("CELAUT_EXPLORER_API") {
            Ok(url) if !url.trim().is_empty() => Skills::new(url.trim()),
            _ => Skills::new(DEFAULT_EXPLORER),
        }
    }

    /// Every skill of the registry (up to 100).
    pub async fn load_skills(&self) -> Result<Vec<Skill>, Error> {
        let boxes = self.boxes("load_skills", SKILL_TYPE_ID, None).await?;
        Ok(boxes
            .iter()
            .map(|b| {
                let r9 = register9(b);
                Skill {
                    box_id: text(b, "boxId"),
                    profile_id: profile_id(b, &r9),
                    name: text_or(&r9, "name", "Unnamed Skill"),
                    prose: text(&r9, "prose"),
                    formal: text(&r9, "formal"),
                    tags: strings(&r9, "tags"),
                    domain: text(&r9, "domain"),
                    extended_skill_box_ids: strings(&r9, "extended_skill_boxes"),
                    source_hash: optional_text(&r9, "source_hash"),
                }
            })
            .collect())
    }

    /// The benchmarks of a skill.
    pub async fn load_benchmarks(&self, skill_box_id: &str) -> Result<Vec<Benchmark>, Error> {
        let boxes = self
            .boxes("load_benchmarks", BENCHMARK_TYPE_ID, Some(skill_box_id))
            .await?;
        Ok(boxes
            .iter()
            .map(|b| {
                let r9 = register9(b);
                Benchmark {
                    id: text(b, "boxId"),
                    profile_id: profile_id(b, &r9),
                    skill_box_id: skill_box_id.to_string(),
                    name: text_or(&r9, "name", "Unnamed Benchmark"),
                    description: text(&r9, "description"),
                    case_descriptors: array(&r9, "case_descriptors")
                        .iter()
                        .map(|c| CaseDescriptor {
                            name: text(c, "name"),
                            description: text(c, "description"),
                        })
                        .collect(),
                    performance_metrics: array(&r9, "performance_metrics")
                        .iter()
                        .map(|m| Metric {
                            name: text(m, "name"),
                            description: text(m, "description"),
                            higher_is_better: m
                                .get("higher_is_better")
                                .or_else(|| m.get("higherIsBetter"))
                                .and_then(Value::as_bool),
                        })
                        .collect(),
                    source_hash: optional_text(&r9, "source_hash"),
                }
            })
            .collect())
    }

    /// The measures of a benchmark.
    pub async fn load_results(&self, benchmark_id: &str) -> Result<Vec<Measure>, Error> {
        let boxes = self
            .boxes("load_results", RESULT_TYPE_ID, Some(benchmark_id))
            .await?;
        Ok(boxes
            .iter()
            .map(|b| {
                let r9 = register9(b);
                Measure {
                    id: text(b, "boxId"),
                    profile_id: profile_id(b, &r9),
                    benchmark_id: benchmark_id.to_string(),
                    service_id: text(&r9, "service_id"),
                    data: array(&r9, "data")
                        .iter()
                        .map(|c| CaseData {
                            case_meta: numbers(c, "case_meta"),
                            metrics_values: numbers(c, "metrics_values"),
                        })
                        .collect(),
                    notes: text(&r9, "notes"),
                    timestamp: r9.get("timestamp").and_then(Value::as_u64).unwrap_or(0),
                    source_hash: optional_text(&r9, "source_hash"),
                }
            })
            .collect())
    }

    /// The services that cover a skill: the coverages that point at it or at one of its
    /// benchmarks, and the services that have a result in one of its benchmarks. A service is once, and a direct coverage wins.
    /// This is the call to start from when a service needs a capability.
    pub async fn load_coverages(&self, skill_box_id: &str) -> Result<Vec<Coverage>, Error> {
        let boxes = self
            .boxes("load_coverages", COVERAGE_TYPE_ID, Some(skill_box_id))
            .await?;
        let coverage = |b: &Value| {
            let r9 = register9(b);
            Coverage {
                box_id: text(b, "boxId"),
                profile_id: profile_id(b, &r9),
                service_id: optional_text(&r9, "service_id"),
                benchmark_id: optional_text(&r9, "benchmark_id"),
            }
        };
        let mut direct: Vec<Coverage> = boxes.iter().map(coverage).collect();
        let mut results = Vec::new();
        for benchmark in self.load_benchmarks(skill_box_id).await? {
            // A coverage may point at a benchmark of the skill instead of the skill.
            let about_benchmark = self
                .boxes("load_coverages", COVERAGE_TYPE_ID, Some(&benchmark.id))
                .await?;
            direct.extend(about_benchmark.iter().map(coverage));
            results.extend(self.load_results(&benchmark.id).await?);
        }

        let mut seen = std::collections::HashSet::new();
        let mut merged = Vec::new();
        for coverage in direct {
            if let Some(id) = &coverage.service_id {
                if !seen.insert(id.clone()) {
                    continue;
                }
            }
            merged.push(coverage);
        }
        for result in results {
            if result.service_id.is_empty() || !seen.insert(result.service_id.clone()) {
                continue;
            }
            merged.push(Coverage {
                box_id: result.id,
                profile_id: result.profile_id,
                service_id: Some(result.service_id),
                benchmark_id: Some(result.benchmark_id),
            });
        }
        Ok(merged)
    }

    /// A skill with its coverages, its benchmarks and the measures of each.
    pub async fn load_skill_tree(&self, skill_box_id: &str) -> Result<SkillTree, Error> {
        let skill = self
            .load_skills()
            .await?
            .into_iter()
            .find(|s| s.box_id == skill_box_id);
        let coverages = self.load_coverages(skill_box_id).await?;
        let mut benchmarks = Vec::new();
        for benchmark in self.load_benchmarks(skill_box_id).await? {
            let results = self.load_results(&benchmark.id).await?;
            benchmarks.push(BenchmarkTree { benchmark, results });
        }
        Ok(SkillTree {
            skill,
            coverages,
            benchmarks,
        })
    }

    /// What the chain says of the functional part of a service (`container`, `api`,
    /// `network`), one entry for each publisher.
    pub async fn service_data(&self, service_id: &str) -> Result<Vec<ServiceData>, Error> {
        let boxes = self
            .boxes("service_data", SERVICE_DATA_TYPE_ID, Some(service_id))
            .await?;
        Ok(boxes
            .iter()
            .map(|b| {
                let (mode, source_hash, content) = service_info(b);
                let architecture = content
                    .get("container")
                    .and_then(|c| c.get("architecture"))
                    .or_else(|| content.get("architecture"))
                    .and_then(Value::as_str)
                    .map(str::to_string);
                ServiceData {
                    box_id: text(b, "boxId"),
                    profile_id: profile_id(b, &content),
                    service_id: service_id.to_string(),
                    mode,
                    source_hash,
                    content,
                    architecture,
                }
            })
            .collect())
    }

    /// What the chain says a service is (name, description, tags), one entry for each
    /// publisher.
    pub async fn service_metadata(&self, service_id: &str) -> Result<Vec<ServiceMetadata>, Error> {
        let boxes = self
            .boxes(
                "service_metadata",
                SERVICE_METADATA_TYPE_ID,
                Some(service_id),
            )
            .await?;
        Ok(boxes
            .iter()
            .map(|b| {
                let (mode, source_hash, content) = service_info(b);
                ServiceMetadata {
                    box_id: text(b, "boxId"),
                    profile_id: profile_id(b, &content),
                    service_id: service_id.to_string(),
                    mode,
                    source_hash,
                    name: optional_text(&content, "name"),
                    description: optional_text(&content, "description"),
                    tags: content
                        .get("tags")
                        .and_then(Value::as_array)
                        .map(|_| strings(&content, "tags")),
                    content,
                }
            })
            .collect())
    }

    /// The unspent boxes of a type (R4) that point at `pointer` (R5), up to 100. A type or a
    /// pointer that is not hex finds nothing, as in the MCP server.
    async fn boxes(
        &self,
        operation: &str,
        type_id: &str,
        pointer: Option<&str>,
    ) -> Result<Vec<Value>, Error> {
        if !is_hex_id(type_id) || pointer.is_some_and(|p| !is_hex_id(p)) {
            return Ok(Vec::new());
        }
        let mut registers = serde_json::Map::new();
        // The Explorer matches a register by the hex of its bytes.
        registers.insert("R4".into(), json!(type_id.to_ascii_lowercase()));
        if let Some(p) = pointer {
            registers.insert("R5".into(), json!(p.to_ascii_lowercase()));
        }
        let body = json!({
            "ergoTreeTemplateHash": REPUTATION_TREE_HASH,
            "registers": registers,
            "assets": [],
        })
        .to_string();

        let mut all = Vec::new();
        let mut offset = 0;
        while all.len() < SEARCH_LIMIT {
            let limit = PAGE.min(SEARCH_LIMIT - all.len());
            let path = format!("/api/v1/boxes/unspent/search?offset={offset}&limit={limit}");
            let answer = self.post(operation, &path, &body).await?;
            let items = answer
                .get("items")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let got = items.len();
            all.extend(items);
            offset += got;
            if got < limit {
                break;
            }
        }
        Ok(all)
    }

    async fn post(&self, operation: &str, path: &str, body: &str) -> Result<Value, Error> {
        let work = self.explorer.post_json(operation, path, body);
        let answer = tokio::time::timeout(REQUEST_TIMEOUT, work)
            .await
            .map_err(|_| registry_down(operation, "timed out"))??;
        serde_json::from_slice(&answer)
            .map_err(|e| registry_failed(operation, None, format!("not JSON: {e}")))
    }
}

impl Endpoint {
    fn parse(url: &str) -> Result<Self, Error> {
        let bad = |why: &str| registry_failed("explorer address", None, format!("{url:?}: {why}"));
        let (tls, rest) = if let Some(r) = url.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = url.strip_prefix("http://") {
            (false, r)
        } else {
            (false, url)
        };
        if tls && !cfg!(feature = "tls") {
            return Err(bad("https needs the feature `tls` of service-library"));
        }
        let (authority, prefix) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].trim_end_matches('/')),
            None => (rest, ""),
        };
        if authority.is_empty() {
            return Err(bad("no host"));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((h, p)) if !authority.ends_with(']') => (
                h.trim_matches(|c| c == '[' || c == ']').to_string(),
                p.parse::<u16>().map_err(|_| bad("bad port"))?,
            ),
            _ => (
                authority.trim_matches(|c| c == '[' || c == ']').to_string(),
                if tls { 443 } else { 80 },
            ),
        };
        Ok(Endpoint {
            tls,
            host,
            port,
            prefix: prefix.to_string(),
        })
    }

    /// POST a JSON body and give the body of a 200 answer.
    async fn post_json(&self, operation: &str, path: &str, body: &str) -> Result<Vec<u8>, Error> {
        let tcp = tokio::time::timeout(
            CONNECT_TIMEOUT,
            TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .map_err(|_| registry_down(operation, "timed out connecting"))?
        .map_err(|e| registry_down(operation, format!("{}: {e}", self.host)))?;

        if self.tls {
            #[cfg(feature = "tls")]
            {
                let stream = tls_connect(&self.host, tcp)
                    .await
                    .map_err(|e| registry_down(operation, format!("tls: {e}")))?;
                return self.exchange(operation, stream, path, body).await;
            }
            #[cfg(not(feature = "tls"))]
            unreachable!("Endpoint::parse refuses https without the feature `tls`");
        }
        self.exchange(operation, tcp, path, body).await
    }

    async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        operation: &str,
        mut stream: S,
        path: &str,
        body: &str,
    ) -> Result<Vec<u8>, Error> {
        let request = format!(
            "POST {}{path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: celaut-service-library\r\n\
             Accept: application/json\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.prefix,
            self.host,
            body.len()
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| registry_down(operation, e.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|e| registry_down(operation, e.to_string()))?;

        let mut raw = Vec::new();
        // A server may close a TLS connection without the closing alert: the answer is
        // whole if it parses, so a read error ends the read and the parse decides.
        let read = (&mut stream)
            .take(MAX_BODY + MAX_HEAD as u64)
            .read_to_end(&mut raw)
            .await;
        let answer = parse_answer(&raw);
        match (answer, read) {
            (Ok(answer), _) => {
                if answer.status != 200 {
                    return Err(registry_failed(
                        operation,
                        Some(answer.status),
                        String::from_utf8_lossy(&answer.body)
                            .chars()
                            .take(300)
                            .collect::<String>(),
                    ));
                }
                Ok(answer.body)
            }
            (Err(_), Err(e)) => Err(registry_down(operation, e.to_string())),
            (Err(why), Ok(_)) => Err(registry_failed(operation, None, why)),
        }
    }
}

#[cfg(feature = "tls")]
async fn tls_connect(
    host: &str,
    tcp: TcpStream,
) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    use std::sync::{Arc, OnceLock};

    static CONFIG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    let config = CONFIG
        .get_or_init(|| {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            let config = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
            Arc::new(config)
        })
        .clone();
    let name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
    tokio_rustls::TlsConnector::from(config)
        .connect(name, tcp)
        .await
}

struct HttpAnswer {
    status: u16,
    body: Vec<u8>,
}

/// The status and the body of a whole HTTP/1.1 answer (`Content-Length` or chunked, or up to
/// the close of the connection).
fn parse_answer(raw: &[u8]) -> Result<HttpAnswer, String> {
    let end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("the connection closed in the head")?;
    let head = String::from_utf8_lossy(&raw[..end]);
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .strip_prefix("HTTP/1.")
        .and_then(|r| r.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("not an HTTP status line: {status_line:?}"))?;
    let mut chunked = false;
    let mut length = None;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            match k.trim().to_ascii_lowercase().as_str() {
                "transfer-encoding" => chunked = v.to_ascii_lowercase().contains("chunked"),
                "content-length" => length = v.trim().parse::<usize>().ok(),
                _ => {}
            }
        }
    }
    let rest = &raw[end + 4..];
    let body = if chunked {
        dechunk(rest)?
    } else if let Some(n) = length {
        if rest.len() < n {
            return Err("the connection closed in the body".into());
        }
        rest[..n].to_vec()
    } else {
        rest.to_vec()
    };
    Ok(HttpAnswer { status, body })
}

fn dechunk(mut rest: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    loop {
        let line_end = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or("the connection closed in a chunk")?;
        let size_text = String::from_utf8_lossy(&rest[..line_end]);
        let size_text = size_text.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| format!("bad chunk size {size_text:?}"))?;
        rest = &rest[line_end + 2..];
        if size == 0 {
            return Ok(out);
        }
        if rest.len() < size + 2 {
            return Err("the connection closed in a chunk".into());
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

fn registry_failed(operation: &str, status: Option<u16>, message: impl Into<String>) -> Error {
    Error::Registry {
        operation: operation.to_string(),
        status,
        unreachable: false,
        message: message.into(),
    }
}

fn registry_down(operation: &str, message: impl Into<String>) -> Error {
    Error::Registry {
        operation: operation.to_string(),
        status: None,
        unreachable: true,
        message: message.into(),
    }
}

/// A hex id the registry can search for: whole bytes, at least two.
fn is_hex_id(v: &str) -> bool {
    v.len() >= 4 && v.len().is_multiple_of(2) && v.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The JSON of the register R9 of a box (it is a `Coll[Byte]`, which the Explorer renders as
/// hex), or an empty object.
fn register9(b: &Value) -> Value {
    let rendered = b
        .pointer("/additionalRegisters/R9/renderedValue")
        .and_then(Value::as_str)
        .unwrap_or_default();
    hex::decode(rendered)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .unwrap_or_else(|| json!({}))
}

/// A box of service data or metadata: R9 is a JSON or, in source mode, a blake2b256 hash.
fn service_info(b: &Value) -> (InfoMode, Option<String>, Value) {
    let rendered = b
        .pointer("/additionalRegisters/R9/renderedValue")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let decoded = hex::decode(rendered)
        .map(|bytes| String::from_utf8_lossy(&bytes).trim().to_string())
        .unwrap_or_default();
    let is_hash = |s: &str| s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit());
    if is_hash(&decoded) {
        return (InfoMode::Source, Some(decoded), json!({}));
    }
    match serde_json::from_str::<Value>(&decoded) {
        // A JSON string that is only a hash is source mode too.
        Ok(Value::String(s)) if is_hash(s.trim()) => {
            (InfoMode::Source, Some(s.trim().to_string()), json!({}))
        }
        Ok(v @ Value::Object(_)) => (InfoMode::Inline, None, v),
        _ => (InfoMode::Inline, None, json!({})),
    }
}

/// The author of a box: `profile_id` of its JSON, else the first token of the box, else the
/// box.
fn profile_id(b: &Value, r9: &Value) -> String {
    ["profile_id", "profileId"]
        .iter()
        .find_map(|k| r9.get(k).and_then(Value::as_str).filter(|s| !s.is_empty()))
        .or_else(|| b.pointer("/assets/0/tokenId").and_then(Value::as_str))
        .or_else(|| b.get("boxId").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string()
}

fn text(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn text_or(v: &Value, key: &str, default: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(default)
        .to_string()
}

fn optional_text(v: &Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn array<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn strings(v: &Value, key: &str) -> Vec<String> {
    array(v, key)
        .iter()
        .filter_map(|s| s.as_str().map(str::to_string))
        .collect()
}

/// The numbers of a list; a string that holds a number counts, as `Number()` does in the
/// registry.
fn numbers(v: &Value, key: &str) -> Vec<f64> {
    array(v, key)
        .iter()
        .map(|n| {
            n.as_f64()
                .or_else(|| n.as_str().and_then(|s| s.trim().parse().ok()))
                .unwrap_or(f64::NAN)
        })
        .collect()
}

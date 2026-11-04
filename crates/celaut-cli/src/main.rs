//! `celaut`: what `service-library` does, as a command, for a service in any language.
//!
//! Each command does one thing, prints JSON on stdout and ends; an error is JSON on stderr
//! with a code that is not 0. The instances that the service starts are kept in a file
//! ([`pool`]), so a later command reuses them, and their tokens never reach the service.

mod pool;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use celaut_proto::celaut::{Amount, BytesKeyValue, Configuration, ServiceInstance as Started};
use prost::Message;
use serde_json::{json, Value};
use service_library::config::{grpc_uri, NodeConfig, DEFAULT_CONFIG_PATH};
use service_library::gateway::{amount, Gateway, Node};
use service_library::manager::tcp_is_alive;
use service_library::network;
use service_library::observe::ObserveOptions;
use service_library::package::{self, PackageInfo};
use service_library::packer::Packer;
use service_library::resources::Resizer;
use service_library::skills::Skills;
use service_library::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use pool::Instance;

const USAGE: &str = "\
celaut: talk to the node of this Celaut service. Every answer is JSON.

Usage: celaut [GLOBAL] <command> [ARGS]

Commands:
  config                       the gateway, the memory limit and the networks
  inspect <file.bee>           the service id of a package
  cost      DEP                the estimated cost of a start
  acquire   DEP [--new]        an idle instance of DEP, or a new one: {lease, uri}
  release   <lease> [--outcome success|timeout|failure]
                               give the instance back to the pool
  stop      <lease>            stop the instance now: {refund_mu}
  list                         the instances of the pool (no tokens)
  maintain  [--idle-timeout SECS] [--max-timeouts N] [--max-failures N]
                               stop the idle instances that are old or broken
  shutdown                     stop every instance of the pool
  metrics   <lease>            the balance of the instance
  pack      <lease> <project.zip> --out FILE [--wait SECS (60)] [--uri IP:PORT]
                               pack a project with the packer service (the instance of
                               <lease>, or --uri): {service, path, size}
  skills    [--explorer URL]   the skills of the on-chain registry (celaut-project/skills)
  skill     <box-id>           a skill, the services that cover it, its benchmarks and results
  covering  <box-id>           only the services that cover a skill: [{service_id, ...}]
  service-info <service-id>    what the chain says of a service: {data, metadata}
  deposit   <lease> <MU>       move MU to the instance (negative: back to you)
  tunnel    <lease> --slot N   send stdin to a port of the instance; the answer to stdout
  observe   <lease> [--packets] [--receipts]
                               the events of the instance, one JSON per line
  resize    --mem BYTES        ask the node for a memory limit for this service
  resolve   --tag T... [--formal K=V]... [--protocol P]... [--prose TEXT]
                               the members of a network: {tags, peers}. The node also
                               opens their addresses in your firewall (a grant)
  chat      --client-id ID [--conversation C] <text>
                               a message to the operator of the node

DEP, one of:
  --name NAME                  an entry of <root>/.dependencies, carried under <root>
  --service HEX                carried under <root> if <root>/__services__/HEX exists,
                               otherwise a service that the node has
  --package FILE               a .bee file
and: [--root DIR (/)] [--block-dir DIR (<root>/__block__)] [--env K=V]...
     [--initial-mu MU] [--attempts N (5)] [--no-verify]

The registry is read from an Ergo Explorer (--explorer URL, $CELAUT_EXPLORER_API, or the
public one); it needs no node. The service id of a coverage is what `--service` takes.

GLOBAL:
  --config PATH                the configuration of the node (/__config__)
  --state PATH                 the pool file ($CELAUT_STATE or /tmp/celaut/pool.json)
  --peer-id ID                 the identity of the node, that the receipts must name
  --require-receipt            refuse an instance with no start receipt
  --no-receipt-check           do not check the start receipt

Exit codes: 0 done, 1 refused or failed, 2 bad arguments, 3 the node is unreachable.
";

/// The flags that take no value.
const SWITCHES: &[&str] = &[
    "new",
    "no-verify",
    "packets",
    "receipts",
    "require-receipt",
    "no-receipt-check",
    "help",
];

/// What went wrong, as the command tells it.
#[derive(Debug)]
enum Fail {
    Usage(String),
    Library(Error),
    Other(String),
}

impl From<Error> for Fail {
    fn from(e: Error) -> Self {
        Fail::Library(e)
    }
}

impl From<std::io::Error> for Fail {
    fn from(e: std::io::Error) -> Self {
        Fail::Other(e.to_string())
    }
}

type Res<T> = Result<T, Fail>;

fn usage(message: impl Into<String>) -> Fail {
    Fail::Usage(message.into())
}

/// The words of the command line: the positional ones, and the flags with their values.
struct Args {
    words: Vec<String>,
    flags: Vec<(String, Option<String>)>,
}

impl Args {
    fn parse(raw: impl Iterator<Item = String>) -> Res<Self> {
        let mut raw = raw.peekable();
        let mut args = Args {
            words: Vec::new(),
            flags: Vec::new(),
        };
        while let Some(word) = raw.next() {
            let Some(flag) = word.strip_prefix("--") else {
                args.words.push(word);
                continue;
            };
            if let Some((name, value)) = flag.split_once('=') {
                args.flags.push((name.to_string(), Some(value.to_string())));
            } else if SWITCHES.contains(&flag) {
                args.flags.push((flag.to_string(), None));
            } else {
                let value = raw
                    .next()
                    .ok_or_else(|| usage(format!("--{flag} needs a value")))?;
                args.flags.push((flag.to_string(), Some(value)));
            }
        }
        Ok(args)
    }

    fn has(&self, name: &str) -> bool {
        self.flags.iter().any(|(n, _)| n == name)
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.flags
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .and_then(|(_, v)| v.as_deref())
    }

    fn values<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a str> {
        self.flags
            .iter()
            .filter(move |(n, _)| n == name)
            .filter_map(|(_, v)| v.as_deref())
    }

    fn number<T: std::str::FromStr>(&self, name: &str) -> Res<Option<T>> {
        self.value(name)
            .map(|v| {
                v.parse()
                    .map_err(|_| usage(format!("--{name} {v:?} is not a number")))
            })
            .transpose()
    }

    fn word(&self, at: usize, what: &str) -> Res<&str> {
        self.words
            .get(at)
            .map(String::as_str)
            .ok_or_else(|| usage(format!("missing {what}")))
    }

    fn lease(&self) -> Res<u64> {
        let word = self.word(1, "the lease")?;
        word.parse()
            .map_err(|_| usage(format!("{word:?} is not a lease")))
    }
}

/// Where a dependency comes from.
enum Source {
    /// The node has it.
    Hash,
    /// A file, or directories under a root, that the first start sends.
    Package(PathBuf, Box<PackageInfo>),
}

struct Dep {
    id: [u8; 32],
    source: Source,
    configuration: Configuration,
    attempts: u32,
}

impl Dep {
    fn hex(&self) -> String {
        hex::encode(self.id)
    }

    fn configuration_key(&self) -> String {
        hex::encode(self.configuration.encode_to_vec())
    }
}

fn service_id(text: &str) -> Res<[u8; 32]> {
    let mut id = [0u8; 32];
    hex::decode_to_slice(text.trim(), &mut id)
        .map_err(|_| usage(format!("{text:?} is not a service id (64 hex digits)")))?;
    Ok(id)
}

/// The id of `name` in `<root>/.dependencies`, which the packer writes (`NAME=<hex id>`).
fn named(root: &Path, name: &str) -> Res<[u8; 32]> {
    let path = root.join(".dependencies");
    let text = std::fs::read_to_string(&path)
        .map_err(|e| Fail::Other(format!("{}: {e}", path.display())))?;
    let id = text
        .lines()
        .filter_map(|l| l.split_once('='))
        .find(|(k, _)| k.trim() == name)
        .map(|(_, v)| v.trim())
        .ok_or_else(|| Fail::Other(format!("no {name} in {}", path.display())))?;
    service_id(id)
}

fn configuration(args: &Args) -> Res<Configuration> {
    let mut env = Vec::new();
    for pair in args.values("env") {
        let (key, value) = pair
            .split_once('=')
            .ok_or_else(|| usage(format!("--env {pair:?} is not K=V")))?;
        env.push(BytesKeyValue {
            key: key.to_string(),
            value: Some(value.as_bytes().to_vec()),
        });
    }
    let initial_mu = args
        .number::<u128>("initial-mu")?
        .map(|n| Amount { n: n.to_string() });
    Ok(Configuration {
        environment_variables: env,
        spec_slot: Vec::new(),
        initial_mu,
    })
}

/// The dependency that the flags name. `sent` are the services that the node has already:
/// those start by hash, and their files are not read again.
fn dependency(args: &Args, sent: &[String]) -> Res<Dep> {
    let root = PathBuf::from(args.value("root").unwrap_or("/"));
    let blocks = args
        .value("block-dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("__block__"));
    let configuration = configuration(args)?;
    let attempts = args.number::<u32>("attempts")?.unwrap_or(5).max(1);
    let was_sent = |id: &[u8; 32]| sent.contains(&hex::encode(id));

    let directory = |id: [u8; 32]| -> Res<Source> {
        if was_sent(&id) {
            return Ok(Source::Hash);
        }
        let info = package::inspect_directory(&root, &blocks, id)?;
        Ok(Source::Package(root.clone(), Box::new(info)))
    };
    let (id, source) = match (
        args.value("name"),
        args.value("service"),
        args.value("package"),
    ) {
        (Some(name), None, None) => {
            let id = named(&root, name)?;
            (id, directory(id)?)
        }
        (None, Some(hex), None) => {
            let id = service_id(hex)?;
            if root.join("__services__").join(hex::encode(id)).exists() {
                (id, directory(id)?)
            } else {
                (id, Source::Hash)
            }
        }
        (None, None, Some(file)) => {
            let info = package::inspect(Path::new(file))?;
            let id = info.service_id;
            if was_sent(&id) {
                (id, Source::Hash)
            } else {
                if !args.has("no-verify") {
                    package::verify_blocks(Path::new(file))?;
                }
                (id, Source::Package(PathBuf::from(file), Box::new(info)))
            }
        }
        _ => return Err(usage("give one of --name, --service or --package")),
    };
    Ok(Dep {
        id,
        source,
        configuration,
        attempts,
    })
}

struct Context {
    config_path: PathBuf,
    state: PathBuf,
    peer_id: Option<String>,
    check_receipt: bool,
    require_receipt: bool,
}

impl Context {
    fn config(&self) -> Res<NodeConfig> {
        Ok(NodeConfig::load_from(&self.config_path)?)
    }

    fn gateway(&self) -> Res<Gateway> {
        let gateway = Gateway::new(&self.config()?.gateway)?;
        Ok(match &self.peer_id {
            Some(peer) => gateway.with_peer_id(peer.clone()),
            None => gateway,
        })
    }

    fn pool<R>(&self, f: impl FnOnce(&mut pool::Pool) -> R) -> Res<R> {
        Ok(pool::with(&self.state, f)?)
    }

    /// The token of a lease that the pool holds.
    fn token(&self, lease: u64) -> Res<String> {
        self.pool(|p| p.get(lease).filter(|i| i.leased).map(|i| i.token.clone()))?
            .ok_or_else(|| Fail::Other(format!("no lease {lease}")))
    }

    /// The faults of the start receipt, if it is checked.
    fn receipt_faults(&self, started: &Started, dep: &Dep) -> Option<String> {
        if !self.check_receipt {
            return None;
        }
        let Some(receipt) = &started.receipt else {
            return self
                .require_receipt
                .then(|| "the node gave no receipt".to_string());
        };
        let initial = dep.configuration.initial_mu.as_ref().map(|a| a.n.as_str());
        let faults = service_library::receipt::check_start(
            receipt,
            self.peer_id.as_deref(),
            &started.token,
            &dep.id,
            &dep.configuration.environment_variables,
            initial,
        );
        (!faults.is_empty()).then(|| {
            faults
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        })
    }

    /// Start an instance of `dep`: the attempts that a gateway failure allows, the check of
    /// the receipt and of the address. An instance that is not usable is stopped.
    async fn launch(&self, gateway: &Gateway, dep: &Dep) -> Res<(String, String)> {
        let mut attempt = 1;
        let started = loop {
            let result = match &dep.source {
                Source::Hash => gateway.start(dep.id, dep.configuration.clone()).await,
                Source::Package(path, info) => {
                    gateway
                        .start_package(path, info, dep.configuration.clone())
                        .await
                }
            };
            match result {
                Ok(started) => break started,
                Err(Error::Gateway { .. }) if attempt < dep.attempts => {
                    tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt))).await;
                    attempt += 1;
                }
                Err(Error::Gateway {
                    operation,
                    unreachable,
                    message,
                    ..
                }) => {
                    return Err(Fail::Library(Error::Gateway {
                        operation,
                        attempts: attempt,
                        unreachable,
                        message,
                    }))
                }
                Err(other) => return Err(other.into()),
            }
        };
        if matches!(dep.source, Source::Package(..)) {
            let service = dep.hex();
            self.pool(|p| {
                if !p.sent.contains(&service) {
                    p.sent.push(service);
                }
            })?;
        }
        let unusable = match self.receipt_faults(&started, dep) {
            Some(faults) => Some(Error::Receipt(faults)),
            None => started
                .instance
                .as_ref()
                .ok_or_else(|| Error::Protocol("the node gave no addresses".to_string()))
                .and_then(grpc_uri)
                .err(),
        };
        if let Some(e) = unusable {
            // An instance that the service cannot use or trust must not stay on the node.
            let _ = gateway.stop_with_retries(&started.token).await;
            return Err(e.into());
        }
        let uri = grpc_uri(started.instance.as_ref().expect("checked above"))?;
        Ok((uri, started.token))
    }

    /// Stop the instances `taken` out of the pool. The ones that do not stop go back to it,
    /// so that a later `shutdown` tries them again.
    async fn stop_all(&self, gateway: &Gateway, taken: Vec<Instance>) -> Res<Value> {
        let mut stopped = 0;
        let mut leaked = Vec::new();
        for instance in taken {
            match gateway.stop_with_retries(&instance.token).await {
                Ok(_) => stopped += 1,
                Err(_) => leaked.push(Instance {
                    leased: false,
                    ..instance
                }),
            }
        }
        let leaked_count = leaked.len();
        if !leaked.is_empty() {
            self.pool(|p| p.instances.extend(leaked))?;
        }
        Ok(json!({ "stopped": stopped, "leaked": leaked_count }))
    }
}

/// The members of a network with all their ports, so the caller picks the port of the
/// protocol it needs.
fn members(instances: &[celaut_proto::celaut::Instance]) -> Vec<Value> {
    instances
        .iter()
        .map(|i| {
            let slots: Vec<Value> = network::slots(i)
                .into_iter()
                .map(|s| {
                    json!({
                        "internal_port": s.internal_port,
                        "transport": s.transport,
                        "protocol_stack": s.protocol_stack,
                        "uris": s.uris,
                    })
                })
                .collect();
            json!({ "slots": slots })
        })
        .collect()
}

fn mu(a: Option<&Amount>) -> Res<String> {
    Ok(amount(a)?.to_string())
}

async fn run(args: Args) -> Res<Value> {
    let ctx = Context {
        config_path: PathBuf::from(args.value("config").unwrap_or(DEFAULT_CONFIG_PATH)),
        state: args
            .value("state")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("CELAUT_STATE").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(pool::DEFAULT_STATE)),
        peer_id: args.value("peer-id").map(str::to_string),
        check_receipt: !args.has("no-receipt-check"),
        require_receipt: args.has("require-receipt"),
    };
    let command = args.word(0, "a command (see --help)")?;
    match command {
        "config" => {
            let config = ctx.config()?;
            let networks: Vec<Value> = config
                .file
                .network_resolution
                .iter()
                .map(|n| {
                    let peers: Vec<String> = n
                        .peer_instances
                        .iter()
                        .filter_map(|i| grpc_uri(i).ok())
                        .collect();
                    json!({
                        "tags": n.tags,
                        "peers": peers,
                        "members": members(&n.peer_instances),
                    })
                })
                .collect();
            Ok(json!({
                "gateway": config.gateway,
                "mem_limit": config.mem_limit,
                "networks": networks,
            }))
        }
        "pack" => {
            // `pack <lease> <zip>`, or `pack --uri IP:PORT <zip>`.
            let zip = args
                .word(if args.has("uri") { 1 } else { 2 }, "the project zip")?
                .to_string();
            let out = args
                .value("out")
                .ok_or_else(|| usage("pack needs --out FILE"))?
                .to_string();
            let wait = args.number::<u64>("wait")?.unwrap_or(60);
            let uri = match args.value("uri") {
                Some(uri) => uri.to_string(),
                None => {
                    let lease = args.lease()?;
                    ctx.pool(|p| {
                        p.instances
                            .iter()
                            .find(|i| i.id == lease)
                            .map(|i| i.uri.clone())
                    })?
                    .ok_or_else(|| Fail::Other(format!("no lease {lease}")))?
                }
            };
            let packer = Packer::new(&uri);
            // A packer that was just started may not answer yet.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait);
            loop {
                match packer.health().await {
                    Ok(()) => break,
                    Err(e) if e.is_unreachable() && std::time::Instant::now() < deadline => {
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            let packed = packer.pack(Path::new(&zip), Path::new(&out)).await?;
            Ok(json!({
                "service": packed.service_id,
                "path": packed.path,
                "size": packed.size,
            }))
        }
        "skills" | "skill" | "covering" | "service-info" => {
            let skills = match args.value("explorer") {
                Some(url) => Skills::new(url)?,
                None => Skills::from_env()?,
            };
            match command {
                "skills" => Ok(serde_json::to_value(skills.load_skills().await?)
                    .map_err(|e| Fail::Other(e.to_string()))?),
                "skill" => Ok(serde_json::to_value(
                    skills
                        .load_skill_tree(args.word(1, "the box id of the skill")?)
                        .await?,
                )
                .map_err(|e| Fail::Other(e.to_string()))?),
                "covering" => Ok(serde_json::to_value(
                    skills
                        .load_coverages(args.word(1, "the box id of the skill")?)
                        .await?,
                )
                .map_err(|e| Fail::Other(e.to_string()))?),
                _ => {
                    let id = args.word(1, "the service id")?;
                    Ok(json!({
                        "data": skills.service_data(id).await?,
                        "metadata": skills.service_metadata(id).await?,
                    }))
                }
            }
        }
        "inspect" => {
            let info = package::inspect(Path::new(args.word(1, "the package")?))?;
            Ok(json!({ "service": hex::encode(info.service_id) }))
        }
        "cost" => {
            let dep = dependency(&args, &[])?;
            let cost = ctx
                .gateway()?
                .estimated_cost(dep.id, dep.configuration.clone())
                .await?;
            Ok(json!({
                "service": dep.hex(),
                "cost_mu": mu(cost.cost.as_ref())?,
                "init_maintenance_cost_mu": mu(cost.init_maintenance_cost.as_ref())?,
                "max_maintenance_cost_mu": mu(cost.max_maintenance_cost.as_ref())?,
            }))
        }
        "acquire" => {
            let sent = ctx.pool(|p| p.sent.clone())?;
            let dep = dependency(&args, &sent)?;
            let (service, key) = (dep.hex(), dep.configuration_key());
            if !args.has("new") {
                let idle = ctx.pool(|p| {
                    p.take_idle(&service, &key).map(|i| {
                        i.leased = true;
                        (i.id, i.uri.clone())
                    })
                })?;
                if let Some((lease, uri)) = idle {
                    return Ok(json!({ "lease": lease, "uri": uri, "new": false }));
                }
            }
            let gateway = ctx.gateway()?;
            let (uri, token) = ctx.launch(&gateway, &dep).await?;
            let lease = ctx.pool(|p| {
                let id = p.next_id();
                p.instances.push(Instance {
                    id,
                    service,
                    configuration: key,
                    uri: uri.clone(),
                    token,
                    leased: true,
                    last_used: pool::now(),
                    timeouts: 0,
                    failures: 0,
                });
                id
            })?;
            Ok(json!({ "lease": lease, "uri": uri, "new": true }))
        }
        "release" => {
            let lease = args.lease()?;
            let outcome = args.value("outcome");
            if !matches!(outcome, None | Some("success" | "timeout" | "failure")) {
                return Err(usage("--outcome is success, timeout or failure"));
            }
            let found = ctx.pool(|p| {
                let Some(i) = p.get(lease).filter(|i| i.leased) else {
                    return false;
                };
                match outcome {
                    Some("success") => {
                        i.timeouts = 0;
                        i.failures = 0;
                    }
                    Some("timeout") => i.timeouts += 1,
                    Some("failure") => i.failures += 1,
                    _ => {}
                }
                i.leased = false;
                i.last_used = pool::now();
                true
            })?;
            if !found {
                return Err(Fail::Other(format!("no lease {lease}")));
            }
            Ok(json!({}))
        }
        "stop" => {
            let lease = args.lease()?;
            let gateway = ctx.gateway()?;
            let instance = ctx
                .pool(|p| p.remove(lease))?
                .ok_or_else(|| Fail::Other(format!("no lease {lease}")))?;
            match gateway.stop_with_retries(&instance.token).await {
                Ok(refund) => Ok(json!({ "refund_mu": refund.to_string() })),
                Err(e) => {
                    // It may still run: keep it, so that `shutdown` tries again.
                    ctx.pool(|p| p.instances.push(instance))?;
                    Err(e.into())
                }
            }
        }
        "list" => {
            let now = pool::now();
            let instances = ctx.pool(|p| {
                p.instances
                    .iter()
                    .map(|i| {
                        json!({
                            "lease": i.id,
                            "service": i.service,
                            "uri": i.uri,
                            "leased": i.leased,
                            "idle_secs": if i.leased { 0 } else { now.saturating_sub(i.last_used) },
                            "timeouts": i.timeouts,
                            "failures": i.failures,
                        })
                    })
                    .collect::<Vec<_>>()
            })?;
            Ok(json!({ "instances": instances }))
        }
        "maintain" => {
            let idle_timeout = args.number::<u64>("idle-timeout")?.unwrap_or(60 * 60);
            let max_timeouts = args.number::<u32>("max-timeouts")?.unwrap_or(5);
            let max_failures = args.number::<u32>("max-failures")?.unwrap_or(20);
            let gateway = ctx.gateway()?;
            let now = pool::now();
            // The candidates leave the pool for the pass, so no `acquire` takes them.
            let candidates = ctx.pool(|p| {
                let (out, keep) = p.instances.drain(..).partition(|i: &Instance| {
                    !i.leased
                        && (now.saturating_sub(i.last_used) > idle_timeout
                            || i.timeouts > max_timeouts
                            || i.failures > max_failures)
                });
                p.instances = keep;
                out
            })?;
            let mut to_stop = Vec::new();
            let mut kept = Vec::new();
            for i in candidates {
                let idle_too_long = now.saturating_sub(i.last_used) > idle_timeout;
                // A slow instance that still answers is kept.
                let dead = i.failures > max_failures
                    || (i.timeouts > max_timeouts && !tcp_is_alive(i.uri.clone()).await);
                if idle_too_long || dead {
                    to_stop.push(i);
                } else {
                    kept.push(i);
                }
            }
            if !kept.is_empty() {
                ctx.pool(|p| p.instances.extend(kept))?;
            }
            ctx.stop_all(&gateway, to_stop).await
        }
        "shutdown" => {
            let gateway = ctx.gateway()?;
            let all = ctx.pool(|p| std::mem::take(&mut p.instances))?;
            ctx.stop_all(&gateway, all).await
        }
        "metrics" => {
            let token = ctx.token(args.lease()?)?;
            let m = ctx.gateway()?.metrics(&token).await?;
            Ok(json!({ "balance_mu": mu(m.balance.as_ref())?, "suspended": m.suspended }))
        }
        "deposit" => {
            let token = ctx.token(args.lease()?)?;
            let text = args.word(2, "the MU to move")?;
            let difference: i128 = text
                .parse()
                .map_err(|_| usage(format!("{text:?} is not a number of MU")))?;
            let out = ctx.gateway()?.modify_deposit(&token, difference).await?;
            Ok(json!({ "success": out.success, "message": out.message }))
        }
        "tunnel" => {
            let token = ctx.token(args.lease()?)?;
            let slot = args
                .number::<u16>("slot")?
                .ok_or_else(|| usage("tunnel needs --slot"))?;
            let mut payload = Vec::new();
            tokio::io::stdin().read_to_end(&mut payload).await?;
            let answer = ctx.gateway()?.tunnel(&token, slot, &payload).await?;
            let mut out = tokio::io::stdout();
            out.write_all(&answer).await?;
            out.flush().await?;
            Ok(Value::Null)
        }
        "observe" => {
            let token = ctx.token(args.lease()?)?;
            let options = ObserveOptions {
                packets: args.has("packets"),
                receipts: args.has("receipts"),
            };
            let mut observation = ctx.gateway()?.observe(&token, options).await?;
            let mut out = tokio::io::stdout();
            while let Some(event) = observation.events.recv().await {
                let event = event?;
                let line = json!({
                    "time": event.time,
                    "kind": event.kind,
                    "payload": event.payload.map(|p| format!("{p:?}")),
                });
                out.write_all(format!("{line}\n").as_bytes()).await?;
                out.flush().await?;
            }
            Ok(Value::Null)
        }
        "resize" => {
            let mem = args
                .number::<u64>("mem")?
                .ok_or_else(|| usage("resize needs --mem BYTES"))?;
            let resized = ctx.gateway()?.resize(mem).await?;
            Ok(json!({
                "mem_limit": resized.mem_limit,
                "balance_mu": resized.balance_mu.to_string(),
            }))
        }
        "resolve" => {
            let tags: Vec<&str> = args.values("tag").collect();
            if tags.is_empty() {
                return Err(usage("resolve needs --tag"));
            }
            let mut pairs = Vec::new();
            for pair in args.values("formal") {
                pairs.push(
                    pair.split_once('=')
                        .ok_or_else(|| usage(format!("--formal {pair:?} is not K=V")))?,
                );
            }
            let protocols: Vec<&str> = args.values("protocol").collect();
            let request = network::request(
                &tags,
                args.value("prose").unwrap_or_default(),
                &pairs,
                &protocols,
            )?;
            let resolved = ctx.gateway()?.resolve_network(request).await?;
            Ok(json!({
                "tags": resolved.tags,
                "peers": members(&resolved.peer_instances),
            }))
        }
        "chat" => {
            let client_id = args
                .value("client-id")
                .ok_or_else(|| usage("chat needs --client-id"))?;
            let body = args.words[1..].join(" ");
            let ack = ctx
                .gateway()?
                .chat(client_id, &body, args.value("conversation"))
                .await?;
            Ok(json!({ "stored": ack.stored, "reason": ack.reason }))
        }
        other => Err(usage(format!("{other:?} is not a command (see --help)"))),
    }
}

fn main() -> ExitCode {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(e) => return report(e),
    };
    if args.has("help") || args.words.first().map(String::as_str) == Some("help") {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    // One thread: a command waits on the node, and does not need more.
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => return report(Fail::Other(e.to_string())),
    };
    match runtime.block_on(run(args)) {
        Ok(Value::Null) => ExitCode::SUCCESS,
        Ok(answer) => {
            println!("{answer}");
            ExitCode::SUCCESS
        }
        Err(e) => report(e),
    }
}

fn report(fail: Fail) -> ExitCode {
    let (code, kind, unreachable, message) = match fail {
        Fail::Usage(m) => (2, "usage", false, m),
        Fail::Other(m) => (1, "failed", false, m),
        Fail::Library(e) => {
            let unreachable = e.is_unreachable();
            let kind = match &e {
                Error::Config(_) => "config",
                Error::Gateway { .. } => "gateway",
                Error::Receipt(_) => "receipt",
                Error::Resources(_) => "resources",
                Error::Protocol(_) => "protocol",
                Error::Packer { .. } => "packer",
                Error::Registry { .. } => "registry",
            };
            (
                if unreachable { 3 } else { 1 },
                kind,
                unreachable,
                e.to_string(),
            )
        }
    };
    eprintln!(
        "{}",
        json!({ "error": message, "kind": kind, "unreachable": unreachable })
    );
    ExitCode::from(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Args {
        Args::parse(line.split_whitespace().map(str::to_string)).unwrap()
    }

    #[test]
    fn flags_take_values_and_switches_do_not() {
        let a = args("acquire --new --env A=1 --env=B=2 --service ab --attempts 3");
        assert_eq!(a.words, ["acquire"]);
        assert!(a.has("new"));
        assert_eq!(a.values("env").collect::<Vec<_>>(), ["A=1", "B=2"]);
        assert_eq!(a.value("service"), Some("ab"));
        assert_eq!(a.number::<u32>("attempts").unwrap(), Some(3));
    }

    #[test]
    fn a_flag_without_its_value_is_a_usage_error() {
        assert!(matches!(
            Args::parse(["--service".to_string()].into_iter()),
            Err(Fail::Usage(_))
        ));
    }

    #[test]
    fn the_configuration_has_the_env_and_the_budget() {
        let c = configuration(&args("x --env K=v=w --initial-mu 10")).unwrap();
        assert_eq!(c.environment_variables[0].key, "K");
        assert_eq!(
            c.environment_variables[0].value.as_deref(),
            Some(&b"v=w"[..])
        );
        assert_eq!(c.initial_mu.unwrap().n, "10");
        assert!(matches!(
            configuration(&args("x --initial-mu lots")),
            Err(Fail::Usage(_))
        ));
    }

    #[test]
    fn a_name_comes_from_the_dependencies_file() {
        let dir = std::env::temp_dir().join(format!("celaut-named-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let id = "ab".repeat(32);
        std::fs::write(dir.join(".dependencies"), format!("OTHER=00\nMOCK={id}\n")).unwrap();
        assert_eq!(named(&dir, "MOCK").unwrap(), [0xab; 32]);
        assert!(named(&dir, "NONE").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_service_that_was_sent_starts_by_hash() {
        let id = "cd".repeat(32);
        let dep = dependency(
            &args(&format!("acquire --service {id} --root /nonexistent")),
            &[],
        )
        .unwrap();
        assert!(matches!(dep.source, Source::Hash));
        assert_eq!(dep.hex(), id);
    }
}

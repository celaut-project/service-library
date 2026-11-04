---
name: celaut-service
description: How a Celaut service, and an AI agent that runs as one, talks to its node. Use it when you write or run a service that starts dependencies, pays for them, checks the node, or packages another service. It leaves out what a service does not decide (balancing between nodes).
---

# A Celaut service and its node

A service runs on a node. The node writes `/__config__` (a `ConfigurationFile`) and the
service reads from it where its gateway is. Use the `service-library` crate for all of this
(`NodeConfig::load`, `Gateway::new`, `DependencyManager`). The wire details come from
`celaut.proto` in `crates/celaut-proto/protos/` (synced from nodo; see `SOURCE`).

A service that is not written in Rust uses the `celaut` command instead (the release binary
`celaut-linux-<arch>`, see the README): the same pool, with the instances kept in a file
between commands and the tokens out of reach of the service. `celaut acquire`, `release`,
`maintain` and `shutdown` are the pool; everything below applies to it too.

## What matters to a service

**Identity and credentials.** A service is an *instance* of its node. It sends no `Client`:
the node knows the caller. Each instance that the service starts has the service as its
father, and the **instance token** of the answer of `StartService` is the credential for that
instance. Keep it private and never log it: whoever has it can stop the instance and read
its balance. The library follows this: errors and `Debug` output name an instance by
`instance::token_tag`, a short hash of the token. Keep the token in memory or in the state of your
own service, and stop the instance when you stop needing it.

**Dependencies.** A dependency is a service (its SHA3-256) plus a `Configuration`.
`DependencyManager::add_service` adds one that the node has; `add_package` adds one from a
`.bee` file (it checks that every block holds what its id names, unless
`ServiceOptions::verify_package` is off; the first instance sends the file, the next ones start
by hash; a node that has the service answers after the hash and the file stops there, and a
node that holds some blocks makes the library skip them). `add_directory_package(root, block_dir,
service_id, configuration)` adds one that is stored as directories of the block store, the way
services carry their dependencies: `root/__services__/<hex id>` is the Service and
`root/__metadata__/<hex id>` the Metadata (optional), with the blocks in `block_dir`. It uses
`bee_rpc::store` to send them (a block that is a single file is checked against its id as it
leaves). Adding the same pair again gives
the same pool. `ServiceOptions` gives each dependency its own number of start attempts (5 by
default; only gateway failures are tried again), idle time, timeouts and failures.

Use an instance: `ServiceHandle::acquire` gives a `Lease` (an idle instance, or a new one:
a start can take minutes, because the node can build and boot the service first), and
dropping the lease gives the instance back. The instance given back last goes out first, so
the surplus ones go unused and the maintenance can stop them. Run your calls to it through
`Lease::call(timeout, |uri| ...)`: it records a good answer, a timeout or a failure, and the
pool stops an instance that fails or times out too often (a slow instance that still passes
the liveness check stays). Stop one that you know is broken with `Lease::stop`.

Keep the pool clean: run `spawn_maintenance(interval, tcp_is_alive)` (or `maintain` yourself
with your own liveness check) so idle and broken instances are stopped, and call `shutdown`
when the service ends. An instance that nobody stops keeps running and billing, and a stop
that fails is in the report as a leak, not as success.

**Costs.** Everything is paid in MU, the unit of the node. Ask the price before you start
(`Gateway::estimated_cost`). `Configuration.initial_mu` is the most that a start can cost you;
without it the node funds the instance for some hours of the resources that it asks for.
Read the balance of an instance with `Gateway::metrics`, and move balance with
`Gateway::modify_deposit` (positive to the instance, negative back to you). `StopService`
gives the unused balance back to you. A stop that fails on every attempt is an error that
says the instance may still run: treat it as a leak, not as success.

**Telling "refused" from "unreachable".** `Error::is_unreachable` is true when the gateway did
not answer. Do not count it as a fault of the node when you keep a reputation or accuse
anyone.

**Checking the node.** `StartService` answers with a start receipt that the node signed
(`ServiceInstance.receipt`). Build the pool with `DependencyManager::with_receipts` and
`ReceiptCheck::Check`: it checks the signature, the token, the service, the digest of the
variables you sent and the start amount, and stops an instance whose receipt fails. Give the
`peer_id` of the node if you know it: without it the check proves only that some key signed.
`require: true` also refuses an instance with no receipt (a node with no identity key gives
none). `receipt::verify` and `receipt::problems` check any receipt. A client that holds a
`client_id` on another node must send the `Client` (`gateway::client_part`) on the calls that
need it, and keeps that id as a master secret.

**Resources and networks.** `ConfigurationFile.initial_sysresources` gives the memory limit at
the start (`NodeConfig::mem_limit`). To change it, `Gateway::modify_resources`: only a local
instance may call it, the node takes a fee from the balance of the instance, and it answers
with the resources and the balance now. `network_resolution` gives the networks that the node
maps for the service. Reach a dependency at the address of its instance (`Lease::uri`); when
it has none that you can reach, `Gateway::tunnel` sends bytes to one of its ports through the
node in one request and one answer (held in memory). For a stream, `tunnel::open` (or
`Gateway::open_tunnel`) gives channels to and from the service and checks the signed
checkpoints of the node against the bytes that you sent and got (`tunnel::ReceiptSink`); give
the gateway the identity of the node with `Gateway::with_peer_id` so they must name it. The
node charges the traffic to the instance.

**Watching and talking.** `Gateway::observe` streams the live events of an instance that you
started (the session, metrics such as CPU, memory and balance, notices, and with options the
packets and the receipt checkpoints); the stream ends when the instance stops, and an
observation that cannot start is one notice that says why. `Gateway::chat` sends a text
message to the operator of a node, with the `client_id` of this service on that node (the node
must have it bound to a peer).

**Networks.** `network_resolution` has only what the node resolved at the start. A network that
was deferred (a `${VAR}` was not set), or a later look at the members of a domain, is
`Gateway::resolve_network` (CLI: `celaut resolve --tag T --formal K=V ... [--protocol P]`;
`config` also gives `networks[].members`). Build the request with `network::request`: the
`formal` body is the `key=value` lines sorted by key, joined with `\n`, and the node compares it
byte for byte with your declaration (fixed keys unchanged, `${VAR}` filled, keys may be added).
A value that is still `${...}` is refused before the call. **The call is a grant**: if the
request fits, the node opens the returned addresses in your firewall. The answer keeps every
slot with its `internal_port`, its `transport` and its `protocol_stack` as the member declares
them (`network::slots`). Pick the slot by protocol, never by port number: nobody agrees on
ports. A port that the member does not declare has no protocol, and it is not guessed. A
refusal is exit code 1, a node that does not answer is 3.

**Not your concern.** Choosing between nodes, and balancing the load, belong to the node of the
service and to clients. A service talks to one gateway.

## Finding a service by the problem it solves

A service that needs a capability (sort sats, summarize Markdown, solve SAT) does not need to
know which service has it. The registry of
[`celaut-project/skills`](https://github.com/celaut-project/skills) lists *skills* (problems) on
the Ergo blockchain, and for each one the services that cover it (*coverages*), how it is
measured (*benchmarks*) and how each service did (*results*). The library reads it
(`service_library::skills`, `celaut skills`); it only reads. Publishing needs the wallet of a
person to sign a transaction, and the registry has no command for it (its MCP server is read
only too).

1. `celaut skills` (`Skills::load_skills`): every skill, with `boxId`, `name`, `prose`, `tags`.
   Match the name, tags and prose with what you need. There is no search call: the list is
   short.
2. `celaut skill <boxId>` (`load_skill_tree`): the skill, its `coverages` (`serviceId`), and its
   `benchmarks` with their `results`. A result has `data[].metricsValues`, which go by position
   with the `performanceMetrics` of its benchmark (`name`, `higherIsBetter`). `celaut covering
   <boxId>` gives only the services.
3. Pick a service. A coverage is a claim of whoever published it, and anyone can publish one:
   prefer a service that has results in a benchmark that measures what you need (the registry
   ranks by reputation: `profileId` is the publisher). `celaut service-info <serviceId>`
   (`service_data`, `service_metadata`) says what the chain knows of a service without
   downloading it (architecture, api, name); those two types are not minted yet in the
   registry and the answer is empty until they are.
4. Start it: the `serviceId` is the id that `celaut acquire --service HEX` and
   `DependencyManager::add_service` take. The node must have it, or get it from its peers; if
   you have the package, `--package FILE`.

```sh
celaut skills | jq -r '.[] | [.boxId, .name] | @tsv'
celaut covering "$SKILL_BOX_ID" | jq -r '.[].serviceId'
celaut acquire --service "$SERVICE_ID"
```

The registry is read from an Ergo Explorer: `--explorer URL`, else `$CELAUT_EXPLORER_API`, else
the public one (`https://api.ergoplatform.com`). It needs no node and no wallet, so it works
before any instance is started. A failure to reach the Explorer is exit code 3
(`kind: "registry"`), and a search that finds nothing is `[]`. The Explorer is `https`: in
Rust, turn on the feature `tls` of `service-library` (the `celaut` binary has it by default;
`--no-default-features` builds a binary that is about 1 MB smaller and speaks `http://` only).
The library returns at most 100 boxes of each kind.

## Developing and packaging services

A service can be the *mother* of another: it takes the source of a service (or its own),
changes it and packs it again, then starts the result as a dependency. This is an evolving
pattern. A service **cannot** ask its node to pack: the gateway has no `Pack` call, and
`nodo pack` is a command of the operator. The way is the **packer service**,
[`bee-rpc-protocol/celaut-packer-service`](https://github.com/bee-rpc-protocol/celaut-packer-service),
which does what `nodo pack` does over HTTP on port 8080. It is a service like any other: you
start it as a dependency, and the library has a client for it (`service_library::packer`,
`celaut pack`).

1. Start the packer as a dependency. Its id is the `packer` of the `core_services` of the
   node. With the library: `DependencyManager::add_service` and `ServiceHandle::acquire`; with
   the command: `celaut acquire --service <packer id>`, which gives `{"lease", "uri"}`. If the
   node does not have the packer, give it to it first (`add_package`, or `--package FILE`); a
   start can take minutes while the node boots it.
2. Zip the project: `Dockerfile`, `service.json` and optionally `pack_config.json` (the
   layout is the one of nodo's `docs/PACKING.md`).
3. Pack. `Packer::new(&lease.uri())` and `packer.pack(&zip, &out).await`; or
   `celaut pack <lease> project.zip --out out.celaut.bee`. It waits for `GET /health`
   (the command, up to `--wait` seconds), sends `POST /pack` with no time limit (a build takes
   minutes; `with_pack_timeout` sets one), writes the answer through a partial file, and checks
   that the id that the file names is the `X-Service-Id` of the packer: on a mismatch there is no
   file. The result is `{service, path, size}`. The packer answers 415 (not a zip), 400 (the
   configuration or the packing is wrong, with a numbered list of what to fix) and 413 (too
   large); they are `Error::Packer { status, message, .. }` with the text of the packer, and
   `is_unreachable` is true when the instance does not answer.
4. `add_package(out)` (or `celaut acquire --package out.celaut.bee`): the first instance
   sends the file to your node, and the next ones start by hash.
5. Give the packer back (`release`, then `stop` or `shutdown` when you are done): an instance
   that nobody stops keeps billing.

```sh
# a service in any language, with the celaut command
celaut acquire --service "$PACKER_ID"                  # {"lease":1,"uri":"10.0.0.9:8080","new":true}
zip -qr /tmp/project.zip Dockerfile service.json
celaut pack 1 /tmp/project.zip --out /tmp/out.celaut.bee    # {"service":"<id>","path":...,"size":...}
celaut release 1 --outcome success
celaut acquire --package /tmp/out.celaut.bee --initial-mu 1000
```

`uri` is the first address that the packer declares in its interface; if you reach the packer
by another address, `celaut pack --uri IP:PORT <zip> --out FILE` takes it.

Keep the budget of the new instance small (`Configuration.initial_mu`). Packing needs the
packer to reach container registries, so give it the network that its `service.json` asks
for. A dependency of the project that names a service by its id (not a git URL or a path
inside the zip) is not in the registry of the packer, which starts empty: `Packer::has_dependency`
(`GET /registry/<id>`) says if it has it, and `Packer::upload_dependency(id, bundle)`
(`POST /registry/<id>`) gives it a gzip tar with `service`, `metadata` (if any) and
`blocks/<id>`, the bundle that `nodo pack` builds. The command has no call for this yet.

## Memory of the service

`resources::ResourceManager` is room for heavy jobs on top of the memory that the service needs
by itself (the `ResourceManager` of the Python library). Build it with `base`, the `mem_limit`
that the node gave the service at the start, and a `Resizer` (`Gateway` is one, through
`ModifyServiceSystemResources`):

```rust
let memory = ResourceManager::new(gateway.clone(), initial_mem_limit);
let lock = memory.lock(job_bytes).await?;   // waits; asks the node for base + job if the room is small
// ... allocate and work ...
lock.release().await;                        // or drop it; the limit goes back to base + what is locked
```

`lock_for(len, limit)` gives up after a time and `try_lock(len)` does not wait. The node
charges a fee per request: the manager asks for `base + locked + waiting` when memory is short,
asks for `base + locked` when the last waiter is served and memory is unlocked, and never asks
twice in a row for the same amount. It never asks for less than `base`, and never for 0: nodo
hibernates an instance with no memory. To hibernate on purpose, `hibernate()` asks for 0; it
fails while a lock is held or waits. `stats()` gives the limit now, the room that is locked and
free, what waits, and the balance in MU that the node reported on the last request. Only a local
instance may use this call.

## Not done yet

- The optional `dev_client` of the Python library (a `Client` on the start).
- Checking that the id of the service in a package is the hash of its whole Service: the
  blocks are checked one by one, and the node checks the rest.
- Tests against a real node: everything here is tested against a fake node and the vectors
  of nodo only. The skills registry was read from the public Explorer (7 skills at the time,
  one with 4 coverages and 5 benchmarks), but a service found there was never started.
- Publishing skills, coverages, benchmarks and results. The registry plans it in its MCP server
  (signing with a seed in the environment, or an unsigned transaction signed outside).

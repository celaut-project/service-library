---
name: celaut-service
description: How a Celaut service, and an AI agent that runs as one, talks to its node. Use it when you write or run a service that starts dependencies, pays for them, checks the node, or packages another service. It leaves out what a service does not decide (balancing between nodes).
---

# A Celaut service and its node

A service runs on a node. The node writes `/__config__` (a `ConfigurationFile`) and the
service reads from it where its gateway is. Use the `service-library` crate for all of this
(`NodeConfig::load`, `Gateway::new`, `DependencyManager`). The wire details come from
`celaut.proto` in `crates/celaut-proto/protos/` (synced from nodo; see `SOURCE`).

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

**Not your concern.** Choosing between nodes, and balancing the load, belong to the node of the
service and to clients. A service talks to one gateway.

## Developing and packaging services

A service can be the *mother* of another: it takes the source of a service (or its own),
changes it and packs it again, then starts the result as a dependency. This is an evolving
pattern. The packer is itself a service,
[`bee-rpc-protocol/celaut-packer-service`](https://github.com/bee-rpc-protocol/celaut-packer-service),
which does what `nodo pack` does over HTTP on port 8080:

1. Start the packer as a dependency, and reach it at the address of its instance.
2. `POST /pack` with a zip of the project (`Dockerfile`, `service.json` and optionally
   `pack_config.json`). The answer is a `.celaut.bee` file; the header `X-Service-Id` has the
   id. `GET /health` says when it is ready. Errors: 415 (not a zip), 400 (config or packing
   error, with a numbered list of what to fix), 413 (too large). The packer also serves a
   browser IDE with templates, and its `SERVICE_CONFIG_GUIDE.md` describes `service.json`.
3. Write the answer to a file, `package::inspect` it, and `add_package` it: the first instance
   sends the file to your node.

Check that the id you start is the one that the packer said, and keep the budget of the new
instance small (`Configuration.initial_mu`). Packing needs the packer to reach container
registries, so give it the network that its `service.json` asks for.

## Memory of the service

`resources::ResourceManager` is a pool of memory that grows and shrinks with the jobs of the
service (the `ResourceManager` of the Python library). Build it with the `mem_limit` that the
node gave the service and a `Resizer` (`Gateway` is one, through `ModifyServiceSystemResources`):

```rust
let memory = ResourceManager::new(gateway.clone(), initial_mem_limit);
let lock = memory.lock(job_bytes).await?;   // waits; asks the node for more if the pool is small
// ... allocate and work ...
lock.release().await;                        // or drop it; the pool shrinks to what is locked
```

`lock_for(len, limit)` gives up after a time and `try_lock(len)` does not wait. The node
charges a fee per request: the manager asks for `locked + waiting` when memory is short, asks
for `locked` when the last waiter is served and memory is unlocked, and never asks twice in a
row for the same amount. `stats()` gives the pool, what is locked and waiting, and the balance
in MU that the node reported on the last request. Only a local instance may use this call.

## Not done yet

- The optional `dev_client` of the Python library (a `Client` on the start).
- Checking that the id of the service in a package is the hash of its whole Service: the
  blocks are checked one by one, and the node checks the rest.
- Tests against a real node: everything here is tested against a fake node and the vectors
  of nodo only.

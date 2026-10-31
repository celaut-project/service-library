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
its balance. Keep it in memory or in the state of your own service, and stop the instance
when you stop needing it.

**Dependencies.** Start a dependency by the SHA3-256 of its service when the node has it
(`add_service`), or from a `.bee` file with `add_package`: the first instance sends the file
and the next ones start by hash. A node that already has the service answers after the hash and
the file stops there; a node that has only some blocks asks for them to be skipped, and the
library does not send them (`relay`, `skips`; `counter::SendCounter` counts what went). `package::inspect` tells the layout and the service id of a
file (`nodo export` and `GetService` number their parts differently; both work). Take instances from the pool
(`DependencyManager::add_service`, `ServiceHandle::acquire`) instead of starting one per
call: a start can take minutes, because the node can build and boot the service first.
Report how each call went (`record_success`, `record_failure`, `record_timeout`) so the pool
stops broken instances. Call `shutdown` when the service ends: an instance that nobody stops
keeps running and billing.

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

## Not done yet

- Tests against a real node: everything here is tested against a fake node and the vectors
  of nodo only.
- `Chat` and `Observe`.

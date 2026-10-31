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

**Dependencies.** Start a dependency by the SHA3-256 of its service (the node must have the
service; sending a whole service is not in the library yet). Take instances from the pool
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

**Checking the node.** Receipts: `StartService` can answer with a start receipt that the node
signed (`ServiceInstance.receipt`). The library does not check it yet. A client that holds
a `client_id` on another node must send the `Client` (`gateway::client_part`) on the calls
that need it, and keeps that id as a master secret.

**Resources and networks.** `ConfigurationFile.initial_sysresources` gives the memory limit at
the start (`NodeConfig::mem_limit`). `network_resolution` gives the networks that the
node maps for the service. Both are read as the node wrote them.

**Not your concern.** Choosing between nodes, and balancing the load, belong to the node of the
service and to clients. A service talks to one gateway.

## Developing and packaging services

A service can be the *mother* of another: it takes the source of a service (or its own),
changes it and packs it again, then starts the result as a dependency. This is an evolving
pattern, and an agent that does it should start from a packer service such as
`bee-rpc-protocol/celaut-packer-service`, which carries what an agent needs to build one.
Check the hash of what you start, and keep the budget of the new instance small.

## Not done yet

- Starting a service that the node does not have (sending Metadata and Service as blocks).
- Resource changes (`ModifyServiceSystemResources`) and `ServiceTunnel`.
- Checking receipts and the identity of the node.
- Chat and `Observe`.

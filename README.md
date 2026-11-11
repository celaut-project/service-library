# service-library

The library of a Celaut service, in Rust. It is the rewrite of
[celaut-service-libraries](https://github.com/bee-rpc-protocol/celaut-service-libraries)
(Python), and it talks to the node of the service directly over
[bee-rpc-over-grpc-rs](https://github.com/bee-rpc-protocol/bee-rpc-over-grpc-rs).

## What is here

| Part | What it does |
|---|---|
| `crates/celaut-proto` | The protobuf types of the gateway (`celaut.proto`, `buffer.proto`), copied from nodo. `protos/SOURCE` has the commit. |
| `service-library::config` | Reads the `ConfigurationFile` of `/__config__`: the gateway address and the memory limit. |
| `service-library::gateway` | The calls of a service: `StartService` (by hash), `StopService` (with retries), `ModifyDeposit`, `GetMetrics`, `GetServiceEstimatedCost`. A service sends no `Client`. |
| `service-library::gateway` (more) | `ModifyServiceSystemResources` (`modify_resources`) and `ServiceTunnel` (`tunnel`: one request and one answer). |
| `service-library::network`, `Gateway::resolve_network` | `ResolveNetwork`: the members of a `Service.Network`, with every port and the protocols it declares for each. The call is a grant: the node opens the returned addresses in the firewall of the service. Tested against an in-process node. |
| `service-library::notice` | The notice that the node writes to the service before it stops or hibernates it (nodo#645): `watch_device` reads `/dev/hvc0` once and gives `Notice { verb, deadline }`, `shutdown_on_notice` stops the idle instances of the pool within the time of the notice. A service that does not read the device makes the node wait all its time at each stop. `celaut notice` is the command. Tested on a real pty. |
| `service-library::observe`, `Gateway::chat` | `Observe` as a stream of events, and `Chat` to the operator of a node. Tested against an in-process node. |
| `service-library::tunnel` | A streaming `ServiceTunnel` with the check of the signed checkpoints of the node, ported from chatui and tested against an in-process node that lies. |
| `service-library::instance`, `manager` | The pool of dependencies: reuse, idle stop, zombies, `shutdown`; `add_service` by hash and `add_package` from a `.bee` file (checked block by block); `add_directory_package` from `__services__/<hash>` and `__metadata__/<hash>` directories (read with `bee_rpc::store`); per-dependency options; start retries; `Lease::call` records how a call went; LIFO reuse; `tcp_is_alive`; optional check of the start receipt. |
| `service-library::resources` | `ResourceManager`: a memory pool that asks the node for more (`ModifyServiceSystemResources`) when a lock does not fit and shrinks when it is freed; `lock`, `lock_for`, `try_lock`, `stats`. |
| `service-library::packer` | A client of the packer service ([`celaut-packer-service`](https://github.com/bee-rpc-protocol/celaut-packer-service)): `health`, `pack` (a zip in, a `.bee` out, the id checked), `has_dependency` and `upload_dependency`. Plain HTTP over TCP, no HTTP crate. Tested against an in-process server. `celaut pack` is the command. |
| `service-library::skills` | A reader of the [skills registry](https://github.com/celaut-project/skills) (problems on Ergo, with the services that cover them and their benchmarks): `load_skills`, `load_coverages`, `load_benchmarks`, `load_results`, `load_skill_tree`, `service_data`, `service_metadata`. The same queries as its MCP server, read from an Ergo Explorer; `https` needs the feature `tls`. Read only. Tested against an in-process Explorer and read once from the public one. `celaut skills`, `skill`, `covering` and `service-info` are the commands. |
| `service-library::package` | Reads a `.bee` file (`nodo export` or `GetService` layout), says its service id, and gives its Buffers for `StartService`. |
| `service-library::relay`, `skips`, `counter` | Sends a service to the node with an early answer: a node that has it stops the transfer, and a node that holds some blocks makes the library skip them. Ported from chatui with its tests against an in-process node. |
| `service-library::statement`, `verifier` | The signed balance statements of nodo (nodo#631), ported from chatui with its vectors: `Gateway::balance_statement` reads the account of an instance or of a descendant, within a budget, and `statement::verify` checks the signature, the hash chain, the head and the balance. `Verifier` is the optional verifier: it reads the instances of the service and their children and grandchildren and reports a stop that the service did not ask for, MU kept, a refund that is not in the ledger, a refused or bad statement. A read costs MU, so it has a budget. `DependencyManager::verify` and `spawn_verifier` run it over the pool. `celaut verify` is the command. |
| `service-library::receipt` | The check of execution receipts, ported from chatui with the vectors of nodo (`check_start` for the start receipt). |
| `crates/celaut-cli` | `celaut`: the library as a command, for a service in any language (see below). |
| `SKILL.md` | What an AI agent that runs as a service has to know, and what it can leave out. It includes how to pack a service with the packer service (there is no `Pack` call in the gateway). |

## `celaut`: the command, for a service in any language

A service that is not written in Rust copies `celaut` into its image and runs it. It is a
static binary (about 1.7 MB, and about 1 MB more with `https` for the skills registry; about 1 MB of memory while a command runs, no process that
stays) for each architecture of a node. Each tag `v*` publishes it in the releases:
`celaut-linux-amd64` and `celaut-linux-arm64`, with their `.sha256`.

```Dockerfile
FROM python:3.13-alpine
ADD --chmod=755 https://github.com/celaut-project/service-library/releases/download/v0.4.1/celaut-linux-arm64 /usr/local/bin/celaut
```

Each command prints one JSON on stdout. An error is JSON on stderr
(`{"error", "kind", "unreachable"}`), with the exit code 1 (refused or failed), 2 (bad
arguments) or 3 (the node did not answer: not a fault of the node).

```sh
celaut acquire --name MOCK           # {"lease":1,"uri":"10.0.0.9:8080","new":true}
# ... call the instance at uri ...
celaut release 1 --outcome success   # back to the pool; the next acquire reuses it
celaut maintain                      # stop old and broken idle instances (run it now and then)
celaut shutdown                      # stop every instance, when the service ends
```

The instances live in a pool file (`/tmp/celaut/pool.json`, mode 0600; `--state` or
`CELAUT_STATE` to move it), so commands reuse them, and their tokens never leave it: the
service only sees lease numbers. `celaut --help` lists the rest: `cost`, `stop`, `list`,
`metrics`, `notice` (the notices of the node to this service, one JSON per line; `--shutdown` stops the pool when one comes), `verify` (the signed statements of the instances and their children, within a `--budget` of MU), `pack`, `skills`, `skill`, `covering`, `service-info`, `deposit`, `tunnel` (stdin to a port, the answer to stdout), `observe` (JSON
lines; `data` holds the fields of the event: the session, the packets with their peer, the notices), `resize`, `chat`, `resolve`, `config`, `inspect`. The start receipt is checked unless
`--no-receipt-check`. The `ResourceManager` is not here: it needs a process that stays, and
`celaut resize --mem` asks the node directly.

To build it yourself, static, as the release does:

    cargo build --release -p celaut-cli --target aarch64-unknown-linux-musl   # CC=musl-gcc

Add `--no-default-features` for a binary without `https` (about 1 MB smaller): `celaut skills`
then needs `--explorer http://...`.

## Not done yet

See the end of `SKILL.md`. Nothing was tried against a real node yet.

## Develop

    cargo test

The generated code of `celaut-proto` is committed, so a build needs no `protoc`. To move to a
newer `celaut.proto`, copy it from nodo `dev` and regenerate with the tool that chatui uses
(`rust/tools/gen-protos`).

## License

GPL-3.0-only, because bee-rpc is.

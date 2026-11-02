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
| `service-library::observe`, `Gateway::chat` | `Observe` as a stream of events, and `Chat` to the operator of a node. Tested against an in-process node. |
| `service-library::tunnel` | A streaming `ServiceTunnel` with the check of the signed checkpoints of the node, ported from chatui and tested against an in-process node that lies. |
| `service-library::instance`, `manager` | The pool of dependencies: reuse, idle stop, zombies, `shutdown`; `add_service` by hash and `add_package` from a `.bee` file (checked block by block); `add_directory_package` from `__services__/<hash>` and `__metadata__/<hash>` directories (read with `bee_rpc::store`); per-dependency options; start retries; `Lease::call` records how a call went; LIFO reuse; `tcp_is_alive`; optional check of the start receipt. |
| `service-library::resources` | `ResourceManager`: a memory pool that asks the node for more (`ModifyServiceSystemResources`) when a lock does not fit and shrinks when it is freed; `lock`, `lock_for`, `try_lock`, `stats`. |
| `service-library::package` | Reads a `.bee` file (`nodo export` or `GetService` layout), says its service id, and gives its Buffers for `StartService`. |
| `service-library::relay`, `skips`, `counter` | Sends a service to the node with an early answer: a node that has it stops the transfer, and a node that holds some blocks makes the library skip them. Ported from chatui with its tests against an in-process node. |
| `service-library::receipt` | The check of execution receipts, ported from chatui with the vectors of nodo (`check_start` for the start receipt). |
| `crates/celaut-cli` | `celaut`: the library as a command, for a service in any language (see below). |
| `SKILL.md` | What an AI agent that runs as a service has to know, and what it can leave out. |

## `celaut`: the command, for a service in any language

A service that is not written in Rust copies `celaut` into its image and runs it. It is a
static binary (about 1.7 MB, about 1 MB of memory while a command runs, no process that
stays) for each architecture of a node. Each tag `v*` publishes it in the releases:
`celaut-linux-amd64` and `celaut-linux-arm64`, with their `.sha256`.

```Dockerfile
FROM python:3.13-alpine
ADD --chmod=755 https://github.com/celaut-project/service-library/releases/download/v0.1.0/celaut-linux-arm64 /usr/local/bin/celaut
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
`metrics`, `deposit`, `tunnel` (stdin to a port, the answer to stdout), `observe` (JSON
lines), `resize`, `chat`, `config`, `inspect`. The start receipt is checked unless
`--no-receipt-check`. The `ResourceManager` is not here: it needs a process that stays, and
`celaut resize --mem` asks the node directly.

To build it yourself, static, as the release does:

    cargo build --release -p celaut-cli --target aarch64-unknown-linux-musl   # CC=musl-gcc

## Not done yet

See the end of `SKILL.md`. Nothing was tried against a real node yet.

## Develop

    cargo test

The generated code of `celaut-proto` is committed, so a build needs no `protoc`. To move to a
newer `celaut.proto`, copy it from nodo `dev` and regenerate with the tool that chatui uses
(`rust/tools/gen-protos`).

## License

GPL-3.0-only, because bee-rpc is.

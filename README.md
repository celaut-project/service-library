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
| `service-library::gateway` (more) | `ModifyServiceSystemResources` (`modify_resources`) and `ServiceTunnel` (`tunnel`, one request and one answer). |
| `service-library::instance`, `manager` | The pool of dependencies: reuse, idle stop, zombies, `shutdown`; `add_service` by hash and `add_package` from a `.bee` file; optional check of the start receipt. |
| `service-library::package` | Reads a `.bee` file (`nodo export` or `GetService` layout), says its service id, and gives its Buffers for `StartService`. |
| `service-library::receipt` | The check of execution receipts, ported from chatui with the vectors of nodo (`check_start` for the start receipt). |
| `SKILL.md` | What an AI agent that runs as a service has to know, and what it can leave out. |

## Not done yet

See the end of `SKILL.md`. Nothing was tried against a real node yet.

## Develop

    cargo test

The generated code of `celaut-proto` is committed, so a build needs no `protoc`. To move to a
newer `celaut.proto`, copy it from nodo `dev` and regenerate with the tool that chatui uses
(`rust/tools/gen-protos`).

## License

GPL-3.0-only, because bee-rpc is.

# Trying service-library against a real node

`mock/` is a service that answers `pong` over HTTP on 8080. `crates/e2e-client` is a service
that carries the mock as a dependency, starts it with `DependencyManager::add_directory_package`,
calls it, stops it and prints `E2E OK`.

    docker run --rm -v "$PWD":/src:z -w /src -e CARGO_TARGET_DIR=/src/target-musl rust:alpine \
      sh -c 'apk add --no-cache musl-dev git build-base && cargo build --release -p e2e-client'
    e2e/prepare.sh
    sudo nodo pack e2e/stage/mock
    sudo nodo pack e2e/stage/client
    sudo nodo execute <client id>
    # then read the log of the instance: it ends with E2E OK

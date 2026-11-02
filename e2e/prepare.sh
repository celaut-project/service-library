#!/bin/bash
# Stage the two projects for `nodo pack`: e2e/stage/mock and e2e/stage/client.
# Needs the static binary: see e2e/README.md.
set -euo pipefail
cd "$(dirname "$0")"
BIN=../target-musl/release/e2e-client
[ -x "$BIN" ] || { echo "build $BIN first (e2e/README.md)"; exit 1; }
rm -rf stage && mkdir -p stage
cp -a mock stage/mock
mkdir -p stage/client/.service stage/client/dependencies
cp -a mock stage/client/dependencies/mock
cp "$BIN" stage/client/e2e-client
cp client/.service/* stage/client/.service/

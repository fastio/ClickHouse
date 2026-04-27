#!/usr/bin/env bash
# Thin wrapper around the Go sweep driver.
#
# The original bash harness (lib/build.sh + lib/measure.sh + ...) was
# rewritten in Go because `var=$(clickhouse client ...)` under `set -e`
# intermittently returned rc=1 with empty output, killing the sweep at the
# first build. The Go driver in `cmd/sweep/` talks to ClickHouse over the
# HTTP interface and avoids that interaction entirely.
#
# Build the binary on first use, then forward all arguments. See
# `cmd/sweep/sweep --help` for the flag set.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN="$DIR/cmd/sweep/sweep"

if [ ! -x "$BIN" ] || [ "$DIR/cmd/sweep/main.go" -nt "$BIN" ]; then
    (cd "$DIR/cmd/sweep" && go build -o sweep .)
fi

exec "$BIN" "$@"

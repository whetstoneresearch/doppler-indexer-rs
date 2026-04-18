#!/bin/sh
set -eu

cd /app

if [ -n "${INDEXER_ARGS:-}" ]; then
  # Intentionally split INDEXER_ARGS into argv for flags like --chains base.
  # shellcheck disable=SC2086
  exec /app/doppler-indexer-rs ${INDEXER_ARGS}
fi

exec /app/doppler-indexer-rs

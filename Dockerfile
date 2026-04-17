FROM rust:1.94-bookworm AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY config ./config
COPY docs ./docs
COPY migrations ./migrations

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/doppler-indexer-rs /app/doppler-indexer-rs
COPY --from=builder /app/config /app/config
COPY --from=builder /app/migrations /app/migrations
COPY --from=builder /app/docs /app/docs
COPY docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh \
    && mkdir -p /app/data

ENV RUST_LOG=info

ENTRYPOINT ["/usr/bin/tini", "--", "/app/entrypoint.sh"]
CMD []

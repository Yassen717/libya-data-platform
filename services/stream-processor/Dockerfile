# syntax=docker/dockerfile:1

# --- Build ---
# cmake is required by rdkafka's `cmake-build` feature (bundled librdkafka).
FROM rust:1-bookworm AS builder
WORKDIR /app
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

# --- Runtime ---
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3 zlib1g \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin ly
COPY --from=builder /app/target/release/ly-processor /usr/local/bin/ly-processor
USER ly
ENTRYPOINT ["/usr/local/bin/ly-processor"]

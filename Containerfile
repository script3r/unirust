FROM rust:1.82 as builder

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  cmake \
  protobuf-compiler \
  && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY benches ./benches
COPY configs ./configs

RUN cargo build --release --no-default-features --bin unirust_shard --bin unirust_router --bin unirust_client

FROM debian:bookworm-slim

RUN useradd -m unirust
WORKDIR /app
COPY --from=builder /app/target/release/unirust_shard /usr/local/bin/unirust_shard
COPY --from=builder /app/target/release/unirust_router /usr/local/bin/unirust_router
COPY --from=builder /app/target/release/unirust_client /usr/local/bin/unirust_client
USER unirust

CMD ["unirust_shard", "--listen", "0.0.0.0:50061", "--shard-id", "0", "--ontology", "/app/config/ontology.json"]

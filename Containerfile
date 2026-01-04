# Unirust Container Image
#
# Multi-stage build for efficient, minimal production image
#
# Usage:
#   podman build -t unirust -f Containerfile .
#   podman run --rm -p 50061:50061 unirust shard
#   podman run --rm -p 50060:50060 unirust router --shards shard-0:50061
#
# Or use with compose.yaml for full cluster deployment

FROM rust:1.82 AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy source files
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY benches ./benches

# Build release binaries
RUN cargo build --release --no-default-features \
    --bin unirust_shard \
    --bin unirust_router \
    --bin unirust_loadtest

# Production image
FROM debian:bookworm-slim

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 unirust

# Copy binaries
COPY --from=builder /app/target/release/unirust_shard /usr/local/bin/
COPY --from=builder /app/target/release/unirust_router /usr/local/bin/
COPY --from=builder /app/target/release/unirust_loadtest /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown unirust:unirust /data

WORKDIR /app
USER unirust

# Default environment
ENV UNIRUST_PROFILE=high-throughput

# Expose ports (shard: 50061, router: 50060)
EXPOSE 50060 50061

# Entrypoint script for flexible command selection
COPY --chown=unirust:unirust <<'EOF' /usr/local/bin/entrypoint.sh
#!/bin/sh
set -e

case "$1" in
    shard)
        shift
        exec unirust_shard --listen 0.0.0.0:50061 "$@"
        ;;
    router)
        shift
        exec unirust_router --listen 0.0.0.0:50060 "$@"
        ;;
    loadtest)
        shift
        exec unirust_loadtest "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
EOF

RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["shard", "--shard-id", "0", "--data-dir", "/data"]

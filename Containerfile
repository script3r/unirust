# Unirust Container Image
#
# Multi-stage Rust build with a Debian slim runtime and CLI tools
#
# Usage:
#   podman build -t unirust -f Containerfile .
#   podman run --rm -p 127.0.0.1:50061:50061 -v unirust-data:/data -v unirust-backup:/backup unirust
# The default command includes --data-dir /data and --backup-dir /backup.
# Supplying an explicit command replaces those defaults; pass both paths to shard.
#
# Use compose.yaml or scripts/podman_cluster.sh for a router/shard network.
# Separate named data/backup volumes can still share one host or physical disk.

FROM rust:1.88-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    libclang-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy source files
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY benches ./benches

# Build release binaries
RUN cargo build --release --locked --features test-support \
    --bin unirust_shard \
    --bin unirust_router \
    --bin unirust_healthcheck \
    --bin unirust_backup \
    --bin unirust_rebalance \
    --bin unirust_client \
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
COPY --from=builder /app/target/release/unirust_healthcheck /usr/local/bin/
COPY --from=builder /app/target/release/unirust_backup /usr/local/bin/
COPY --from=builder /app/target/release/unirust_rebalance /usr/local/bin/
COPY --from=builder /app/target/release/unirust_client /usr/local/bin/
COPY --from=builder /app/target/release/unirust_loadtest /usr/local/bin/

# Create persistent data and checkpoint mount points
RUN mkdir -p /data /backup && chown unirust:unirust /data /backup

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
    backup)
        shift
        exec unirust_backup "$@"
        ;;
    rebalance)
        shift
        exec unirust_rebalance "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
EOF

RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["shard", "--shard-id", "0", "--data-dir", "/data", "--backup-dir", "/backup"]

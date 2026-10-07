# New wrapper only: no edits are made to any PMXT source file.
FROM golang:1.23-bookworm AS spool-build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /polyspool ./cmd/spool

FROM rust:1.94-slim-bookworm AS pmxt-build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config ca-certificates && rm -rf /var/lib/apt/lists/*
ENV CARGO_BUILD_JOBS=1 CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=false
WORKDIR /build
COPY .upstream/shared/rust/polymarket-orderbook-rust ./shared/rust/polymarket-orderbook-rust
COPY .upstream/services/polymarket/polymarket-orderbook-rust-pubsub ./services/polymarket/polymarket-orderbook-rust-pubsub
WORKDIR /build/services/polymarket/polymarket-orderbook-rust-pubsub
RUN cargo build --release --locked -j 1 && strip target/release/polymarket-orderbook-rust-pubsub

FROM debian:bookworm-slim AS receiver
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=spool-build /polyspool /usr/local/bin/polyspool
ENTRYPOINT ["polyspool"]
CMD ["receive"]

FROM receiver AS pmxt
COPY --from=pmxt-build /build/services/polymarket/polymarket-orderbook-rust-pubsub/target/release/polymarket-orderbook-rust-pubsub /app/polymarket-orderbook-rust-pubsub
CMD ["tap", "/app/polymarket-orderbook-rust-pubsub"]

FROM python:3.12-slim-bookworm AS worker
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 MALLOC_ARENA_MAX=2 \
    ARROW_NUM_THREADS=1 OMP_NUM_THREADS=1 HF_HUB_DISABLE_XET=1 \
    HF_HUB_DISABLE_TELEMETRY=1 HF_HUB_DISABLE_PROGRESS_BARS=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY polydata ./polydata
CMD ["python", "-m", "polydata.worker", "run"]

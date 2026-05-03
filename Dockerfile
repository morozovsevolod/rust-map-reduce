FROM rust:1.95-bookworm AS builder
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
RUN rustup target add wasm32-unknown-unknown
WORKDIR /src
COPY . .
RUN cargo build --release --bin master --bin worker --bin client
RUN cargo build --release --target wasm32-unknown-unknown -p task

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/master /usr/local/bin/master
COPY --from=builder /src/target/release/worker /usr/local/bin/worker
COPY --from=builder /src/target/release/client /usr/local/bin/client
COPY --from=builder /src/target/wasm32-unknown-unknown/release/task.wasm /app/task.wasm
WORKDIR /app

FROM runtime AS master
EXPOSE 3000 50051
CMD ["master"]

FROM runtime AS worker
EXPOSE 3001
ENTRYPOINT ["worker"]

FROM runtime AS client
CMD ["client"]

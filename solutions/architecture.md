# Architecture

## Metadata-only master

The master holds no data buffers in memory. All files (input chunks, WASM modules, intermediate outputs) live on disk. The master tracks job state, task state, and the worker registry. This keeps the master's memory footprint constant regardless of input size or number of jobs.

Data flows via HTTP file servers. The master serves chunks and WASM modules. Workers serve their intermediate outputs. gRPC carries only metadata - task assignments, file paths, and URLs - never file contents.

## Two-port master

The master listens on two separate ports: HTTP (Axum) on 3000 for the client API and file serving, and gRPC (tonic) on 50051 for worker communication. Separating these concerns avoids mixing two transport protocols and keeps the client-facing REST API independent of the internal worker protocol.

## Worker independence

Each worker runs three concurrent services: a heartbeat loop, a task polling loop, and an HTTP status/data server. The heartbeat loop sends periodic signals to the master. The task loop blocks on `GetTask` until work is available. The HTTP server responds to forced polls from the master and serves intermediate files to other workers during shuffle.

Workers use atomic counters for available slots instead of locks. This avoids contention when tasks complete and slots free up, since the slot count is a simple integer that doesn't need consistency with other state.

## WASM as pluggable tasks

The `.wit` interface defines `map` and `reduce` as exports. Any WASM module implementing these two functions is a valid task. The master accepts raw `.wasm` bytes via multipart upload, hashes them, and serves them by hash. Workers cache locally so the same module is never fetched twice. The hash-based deduplication means resubmitting an identical module reuses the existing copy on both master and worker.

## Monorepo structure

Five crates share a single Cargo workspace: `proto` (generated gRPC types), `task` (WASM module), `master`, `worker`, and `client`. The `proto` crate is a dependency of `master` and `worker`. The `task` crate compiles to `wasm32-unknown-unknown` and has no shared code with the other crates beyond the interface contract.

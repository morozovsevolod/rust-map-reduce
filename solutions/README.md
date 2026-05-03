# Solutions

Design choices and rationale for this MapReduce implementation.

## Index

| Document | Covers |
|----------|--------|
| [Architecture](architecture.md) | Master-worker layout, metadata-only master, disk-backed data flow |
| [Scheduling](scheduling.md) | Task assignment, map-then-reduce ordering, load distribution, retry logic |
| [WASM Runtime](wasm_runtime.md) | Wasmtime integration, memory layout, fuel budgeting, bump allocator |
| [Heartbeat and Eviction](heartbeat_eviction.md) | Worker liveness, forced polls, eviction strikes, task timeout reset |
| [Data Flow and Storage](data_flow.md) | Chunk splitting, partitioning, shuffle, k-way merge |
| [Cleanup](cleanup.md) | Post-download cleanup, delayed deletion, worker coordination |
| [Client API](client_api.md) | Job submission, status polling, one-time result download |
| [gRPC Protocol](grpc_protocol.md) | WorkerService and MasterControl RPCs, message shapes |

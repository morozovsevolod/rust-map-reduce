# rust-map-reduce

A distributed MapReduce framework in Rust. Submit a WASM module with `map` and `reduce` functions plus some input data - the system splits the work across worker nodes and gives you the merged result.

## What it does

1. Master splits the input into line-aligned chunks (16-64 MB each).
2. Workers pull map tasks, run your WASM `map()` on each chunk, and partition output by `hash(key) % num_reduces`.
3. After all maps finish, workers pull reduce tasks, fetch intermediate data from map workers, sort by key, and run `reduce()`.
4. Master merges the reduce outputs via k-way merge into a single sorted result file.

The built-in `task` crate implements word-count → dedup. Any WASM module exporting `map`/`reduce` works as a drop-in replacement.

## Architecture

| Crate | What |
|-------|------|
| `master` | Coordinator. Two ports: HTTP 3000 (REST for clients, file serving) and gRPC 50051 (worker communication). Holds zero data buffers - everything lives on disk. |
| `worker` | Runs tasks. Pulls work from master, executes WASM via Wasmtime, serves intermediate files over HTTP. |
| `client` | CLI for submitting jobs, checking status, downloading results. |
| `task` | Example WASM module (word count). Compiles to `wasm32-unknown-unknown`, no stdlib. |
| `proto` | Shared gRPC types generated from `.proto`. |

### Key design choices

**Metadata-only master.** The master never holds actual data in memory. Files are written to `/tmp/mapreduce_data/`, chunks are served over HTTP, and gRPC messages carry only paths and URLs. This keeps the master light and lets it coordinate many workers without becoming a bottleneck.

**Pull-based scheduling.** Workers call `GetTask` when they have free slots. Master never pushes. This means back-pressure is natural - a worker with 0 slots gets nothing, no busy-loop needed.

**Map-first barrier.** All map tasks must complete before any reduce starts. No explicit barrier primitives, just a check in the scheduler: "are all maps done? yes → assign reduce."

**WASM sandboxing with fuel budget.** User code runs in Wasmtime with a fuel limit - 10M base + 500 per input byte. Guarantees no infinite loops, and ties compute cost directly to data volume.

**gRPC for control, HTTP for data.** Clean split: gRPC carries task assignments and metadata (a few KB), HTTP serves the actual chunk files (megabytes). No streaming gRPC to keep things simple.

**One-time result download.** The result file is deleted after the response is sent. Prevents cleanup races - if you didn't download it, it's still there. If you did, it's gone and a 30-second delayed cleanup wipes the rest.

## Deploy

### Docker (easiest)

```bash
docker-compose build
docker-compose up -d
```

This starts a master on ports 3002 -> 3000 (REST) and 50051 -> 50051 (gRPC), plus three workers with status pages on 3004, 3005, 3006.

### Native

```bash
# Master in one terminal:
cargo run --bin master

cargo run --bin worker http://127.0.0.1:50051 w1 4 3001
cargo run --bin worker http://127.0.0.1:50051 w2 4 3002
```

Worker args: `<grpc_url> <name> <slots> <status_port>`.

## Use

All commands hit the master REST API (default `http://localhost:3000`).

Submit a job (multipart form with WASM + input file):
```bash
curl -F wasm=@path-to-your-task.wasm \
     -F input=@path-to-your-data.txt \
     -F num_reduces=4 \
     http://localhost:3000/api/v1/jobs
```

Or let the master generate random data instead of providing a file:
```bash
curl -F wasm=@path-to-your-task.wasm \
     -F generate_mb=100 \
     -F num_reduces=4 \
     http://localhost:3000/api/v1/jobs
```

The response includes the `job_id` - save it for the next steps.

Check progress:
```bash
curl http://localhost:3000/api/v1/jobs/<job-id>/status
```

Download result (only works once, after status is `done`):
```bash
curl -o path-to-output-file.txt \
     http://localhost:3000/api/v1/jobs/<job-id>/result
```

## Custom WASM

See `task/src/main.rs` for a working reference.

For custom task, change code in the `/task` folder with target `wasm32-unknown-unknown`. For other implementations look at /`task/wit/mapreduce.wit` for interface.

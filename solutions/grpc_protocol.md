# gRPC Protocol

## Two services

The master exposes two gRPC services. `WorkerService` handles worker-to-master communication. `MasterControl` handles master-to-worker communication (used for forced status polls).

Separating these avoids putting master-initiated calls in the worker-facing service. The `MasterControl` service is defined on the worker side, and the master calls it when a forced poll is needed.

## WorkerService RPCs

- **Heartbeat** - worker sends ID, address, available slots, and data URL. Master responds with `alive: bool`. This doubles as registration - the first heartbeat creates the worker entry.
- **GetTask** - worker requests work. Master returns a `TaskAssignment` or `Unavailable`. The assignment contains metadata only: task ID, job ID, type, and a payload discriminated by map/reduce.
- **ReportResult** - worker sends task ID, output file path, and its data URL. No file data. The master records the location for shuffle/merge.
- **ReportFailure** - worker sends task ID and a reason string. The master increments the retry count and either reschedules or marks the job failed.

## Payload variants

`TaskAssignment` uses a `oneof` for the payload. Map tasks carry the WASM hash, chunk file path, and number of reducers. Reduce tasks carry the WASM hash, partition index, and a list of `DataLocation` entries pointing to intermediate files on map workers. This keeps the assignment message compact - each worker only sees the fields it needs.

## No streaming

All RPCs are unary. Streaming would add complexity without benefit. Task assignments are small metadata. Data flows over HTTP. A streaming `GetTask` would require keeping a long-lived connection open per worker, which is harder to manage under reconnects and load balancer timeouts.

## Message size limit

Workers set a 256 MB maximum decoding size on their gRPC clients. This accommodates large reduce task assignments where the intermediate file list can be substantial when many map tasks exist. The default tonic limit (4 MB) is too small for jobs with hundreds of map tasks.

## Empty and status types

`ReportResult` and `ReportFailure` return an `Empty` message. The actual state update happens on the master side. The response is an acknowledgement. `PollWorkerStatus` returns `WorkerStatus` with active/finished task lists and available slots. This is only used during forced polls, not for periodic monitoring.

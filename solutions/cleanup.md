# Cleanup

## One-time result download

The result endpoint marks `result_downloaded = true` on the first call. Subsequent calls return 404. This prevents double-download and ensures cleanup runs exactly once. The flag is set before the response body is constructed, so even a failed response prevents re-download.

## Delayed deletion

After the result response body is fully sent, the master schedules cleanup with a 30-second delay. The response bytes are buffered in memory before the response is constructed, so deleting the disk file does not affect the client's download. The 30-second window accounts for slow networks and ensures TCP segments are flushed before the file disappears.

Cleanup is triggered by an Axum response stream that yields the data on the first poll and spawns the cleanup task on the second poll. The second poll only fires when axum has finished writing all bytes to the socket.

## Coordinated worker cleanup

The master collects all unique worker data URLs associated with a job - from intermediate map locations and reduce outputs. A DELETE request is sent to each worker's `/jobs/{job_id}` endpoint. The worker removes its local job directory. Unreachable workers are logged as warnings but do not block master-side cleanup.

## In-memory state removal

After disk cleanup, the master removes the job from its state map and all tasks whose IDs start with the job ID. This keeps the in-memory registry clean and prevents stale tasks from appearing in status responses.

## Cleanup scope

Only the job that was downloaded is cleaned up. Other jobs on the same workers or the same master are untouched. The per-job directory layout ensures cleanup is isolated to one directory.

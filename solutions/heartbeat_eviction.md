# Heartbeat and Eviction

## Periodic checks from the master

The master runs a single async loop that ticks every 5 seconds. Each tick checks two things: worker heartbeats and task timeouts. Combining both into one loop avoids spawning two competing monitor tasks and keeps the lock contention pattern simple - one writer per tick.

## Forced poll before eviction

When a worker's last heartbeat exceeds 15 seconds, the master increments its poll counter. On the first and second strikes, the master attempts an HTTP GET to the worker's `/status` endpoint. A successful response resets the counter. This handles the case where heartbeats are dropped but the worker is still running. The worker only gets evicted after 3 strikes, meaning 2 failed forced polls plus the original timeout.

A 5-second timeout on the forced poll request prevents the monitor tick from hanging indefinitely.

## Eviction triggers task reschedule

When a worker reaches 3 strikes, it is removed from the registry. All tasks with `Running(that_worker)` are reset to `Pending`. The next `GetTask` call from any healthy worker may pick one of these tasks. Rescheduling happens in the same loop iteration, so there is no gap between eviction and reassignment.

## Worker-side heartbeat loop

Workers send heartbeats every 5 seconds via gRPC. The heartbeat request carries the worker's status server address and HTTP data URL. This keeps the master's registry up to date without a separate registration RPC. The first heartbeat effectively registers the worker.

When the heartbeat loop exits (master returns `alive: false` or the connection dies), the worker sends a shutdown signal on a oneshot channel. The task loop receives this and stops cleanly. The status server continues until the process exits, allowing the master's forced poll to see a dying worker rather than a silent one.

## Task timeout independence

Task timeouts (300 seconds) are checked alongside heartbeats but operate independently. A timed-out task is reset on the master and removed from the worker's active set. The worker is not evicted. This separates the "task is stuck" case from the "worker is dead" case. A worker can have one stuck task and continue processing others.

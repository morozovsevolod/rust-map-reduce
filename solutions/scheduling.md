# Scheduling

## Pull-based assignment

Workers request tasks via `GetTask` rather than the master pushing work. This avoids maintaining a queue per worker and back-pressure handling. When a worker calls `GetTask`, the scheduler picks the next pending task atomically and returns it. If no tasks exist or the worker has no free slots, the master returns `Unavailable`. The worker sleeps 2 seconds before retrying.

Pull-based scheduling means the master only updates state when work is claimed. Push-based would require tracking pending assignments per worker and handling the case where work is pushed but never started.

## Map-first ordering

All map tasks are scheduled before any reduce task. The scheduler checks for pending map tasks first. A reduce task is only eligible when every map task for a job has status `Done`. This guarantees that all intermediate partition files exist before any reducer tries to fetch them. No coordination or barrier primitives are needed - the map-first check acts as an implicit barrier.

## Slot-aware assignment

Each worker declares a slot count at startup. The master tracks active tasks per worker and computes available slots as `total - active`. A worker with zero available slots receives `Unavailable` from `GetTask`. This prevents overloading any single worker. Tasks distribute naturally since workers poll at roughly the same rate and each claim drains one slot.

## Task timeout reset

Tasks running longer than 300 seconds are reset to `Pending`. The worker slot is freed. The task becomes eligible for reassignment on the next `GetTask`. This handles hung tasks without removing the worker. A worker with a single stuck task still receives new work on its remaining slots.

## Failure with limited retries

When a worker reports task failure, the master increments the task's retry count. After one retry, a second failure marks the task as permanently failed and the entire job transitions to `Failed`. The worker stays in the pool. This distinguishes between task-level bugs (bad WASM, corrupt input) and worker-level failures (network drop, crash). The former gets a single retry. The latter is handled by the heartbeat eviction path.

## Rescheduling on eviction

When a worker is evicted, all its running tasks are reset to `Pending`. The tasks become eligible for any healthy worker. No task-level tracking of which worker held it is needed - the reset clears the worker reference and the tasks re-enter the pool.

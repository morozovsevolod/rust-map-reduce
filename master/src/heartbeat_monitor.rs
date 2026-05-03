use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::scheduler::reschedule_worker_tasks;
use crate::state::{SharedState, TaskStatus};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
const TASK_TIMEOUT: Duration = Duration::from_secs(300);
const EVICTION_STRIKES: u32 = 3;

/// Monitor worker heartbeats and task timeouts.
/// Evicts workers after 3 missed heartbeats and resets stuck tasks.
pub async fn heartbeat_monitor(state: Arc<SharedState>) {
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    loop {
        interval.tick().await;
        check_heartbeats(&state).await;
        check_task_timeouts(&state).await;
    }
}

async fn check_heartbeats(state: &SharedState) {
    let now = Instant::now();
    let mut workers_to_poll = Vec::new();
    let mut worker_ids_to_evict = Vec::new();

    {
        let mut workers = state.workers.write().await;
        for (id, worker) in workers.iter_mut() {
            if now - worker.last_heartbeat > HEARTBEAT_TIMEOUT {
                worker.poll_counter += 1;
                if worker.poll_counter >= EVICTION_STRIKES {
                    worker_ids_to_evict.push(id.clone());
                } else {
                    workers_to_poll.push((id.clone(), worker.address.clone()));
                }
            }
        }
    }

    // Try forced poll on workers that haven't reached eviction yet
    let mut poll_succeeded = Vec::new();
    for (wid, addr) in workers_to_poll {
        let url = format!("http://{}/status", addr);
        let resp = tokio::time::timeout(Duration::from_secs(5), reqwest::get(&url)).await;

        match resp {
            Ok(Ok(response)) if response.status().is_success() => {
                tracing::info!(worker_id = %wid, "forced poll succeeded");
                poll_succeeded.push(wid);
            }
            _ => {
                tracing::warn!(worker_id = %wid, url = %url, "forced poll failed");
            }
        }
    }

    // Atomically reset counters for all successful forced polls in a single lock
    if !poll_succeeded.is_empty() {
        let mut workers = state.workers.write().await;
        for wid in &poll_succeeded {
            if let Some(w) = workers.get_mut(wid) {
                w.poll_counter = 0;
            }
        }
    }

    for wid in worker_ids_to_evict {
        reschedule_worker_tasks(state, &wid).await;
        state.workers.write().await.remove(&wid);
        tracing::info!(worker_id = %wid, "evicted worker after {} missed heartbeats", EVICTION_STRIKES);
    }
}

async fn check_task_timeouts(state: &SharedState) {
    let now = Instant::now();

    // Collect timed-out task IDs with read lock
    let timed_out: Vec<(String, String)> = {
        let tasks = state.tasks.read().await;
        tasks
            .iter()
            .filter_map(|(id, task)| {
                if let TaskStatus::Running(ref worker_id) = task.status {
                    if let Some(started) = task.started_at {
                        if now - started > TASK_TIMEOUT {
                            return Some((id.clone(), worker_id.clone()));
                        }
                    }
                }
                None
            })
            .collect()
    };

    // Update only affected tasks
    if !timed_out.is_empty() {
        let timed_out_ids: std::collections::HashSet<&str> =
            timed_out.iter().map(|(id, _)| id.as_str()).collect();
        {
            let mut tasks = state.tasks.write().await;
            for (id, _) in &timed_out {
                if let Some(task) = tasks.get_mut(id) {
                    task.status = TaskStatus::Pending;
                    task.started_at = None;
                }
            }
        }

        // Sync job embedded task copies
        {
            let mut jobs = state.jobs.write().await;
            for job in jobs.values_mut() {
                for t in job.map_tasks.iter_mut().chain(job.reduce_tasks.iter_mut()) {
                    if timed_out_ids.contains(t.id.as_str()) {
                        t.status = TaskStatus::Pending;
                        t.started_at = None;
                    }
                }
            }
        }

        for (_tid, wid) in &timed_out {
            let mut workers = state.workers.write().await;
            for worker in workers.values_mut() {
                if worker.id == *wid {
                    for (tid, _) in &timed_out {
                        worker.active_tasks.remove(tid);
                    }
                    break;
                }
            }
        }

        tracing::info!(count = timed_out.len(), "reset timed-out tasks to pending");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Worker;

    fn make_state() -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!("mr_hb_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        SharedState::new(dir)
    }

    async fn add_worker(state: &SharedState, id: &str, slots: u32) {
        let mut workers = state.workers.write().await;
        workers.insert(
            id.to_string(),
            Worker::new(id.to_string(), "127.0.0.1:0".to_string(), slots),
        );
    }

    #[test]
    fn test_timeout_constants() {
        assert!(HEARTBEAT_TIMEOUT.as_secs() > HEARTBEAT_INTERVAL.as_secs());
        assert!(TASK_TIMEOUT.as_secs() > HEARTBEAT_TIMEOUT.as_secs());
        assert!(EVICTION_STRIKES > 0);
    }

    #[tokio::test]
    async fn test_evict_dead_worker() {
        let state = make_state();
        add_worker(&state, "w1", 2).await;

        {
            let mut workers = state.workers.write().await;
            let w = workers.get_mut("w1").unwrap();
            w.last_heartbeat = Instant::now() - Duration::from_secs(60);
            w.poll_counter = 3;
        }

        check_heartbeats(&state).await;

        assert!(state.workers.read().await.get("w1").is_none());
    }

    #[tokio::test]
    async fn test_task_timeout_reset() {
        let state = make_state();

        let task = crate::state::Task {
            id: "job1-map-0".to_string(),
            task_type: "map".to_string(),
            index: 0,
            status: TaskStatus::Running("w1".to_string()),
            started_at: Some(Instant::now() - Duration::from_secs(310)),
            retry_count: 0,
        };
        state
            .tasks
            .write()
            .await
            .insert("job1-map-0".to_string(), task);

        add_worker(&state, "w1", 1).await;
        {
            let mut workers = state.workers.write().await;
            let w = workers.get_mut("w1").unwrap();
            w.active_tasks.insert("job1-map-0".to_string(), ());
        }

        check_task_timeouts(&state).await;

        let tasks = state.tasks.read().await;
        let task = tasks.get("job1-map-0").unwrap();
        assert!(matches!(task.status, TaskStatus::Pending));
        assert!(task.started_at.is_none());

        let workers = state.workers.read().await;
        let w = workers.get("w1").unwrap();
        assert_eq!(w.available_slots(), 1);
        assert!(!w.active_tasks.contains_key("job1-map-0"));
    }
}

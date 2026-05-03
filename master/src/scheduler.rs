use std::time::Instant;

use proto::mapreduce::task_assignment::Payload;
use proto::mapreduce::{DataLocation, MapTaskPayload, ReduceTaskPayload, TaskAssignment};

use crate::state::{JobStatus, SharedState, TaskStatus};

/// Atomically pick and assign a task for a worker.
/// Returns None if worker has no slots or no tasks available.
pub async fn pick_and_assign_task(state: &SharedState, worker_id: &str) -> Option<TaskAssignment> {
    let mut tasks = state.tasks.write().await;
    let mut jobs = state.jobs.write().await;
    let mut workers = state.workers.write().await;

    let worker = workers.get_mut(worker_id)?;
    if worker.available_slots() == 0 {
        return None;
    }

    let now = Instant::now();
    let (assignment, job_id) =
        if let Some(a) = pick_map_task_inner(&mut tasks, &jobs, &state.data_dir) {
            a
        } else if let Some(a) = pick_reduce_task_inner(&mut tasks, &mut jobs) {
            a
        } else {
            return None;
        };

    let task_id = assignment.task_id.clone();

    if let Some(task) = tasks.get_mut(&task_id) {
        task.status = TaskStatus::Running(worker_id.to_string());
        task.started_at = Some(now);
    }

    if let Some(job) = jobs.get_mut(&job_id) {
        for t in job.map_tasks.iter_mut().chain(job.reduce_tasks.iter_mut()) {
            if t.id == task_id {
                t.status = TaskStatus::Running(worker_id.to_string());
                t.started_at = Some(now);
            }
        }
        if job.status == JobStatus::Pending {
            job.status = JobStatus::Running;
        }
    }

    worker.active_tasks.insert(task_id, ());

    Some(assignment)
}

fn pick_map_task_inner(
    tasks: &mut std::collections::HashMap<String, crate::state::Task>,
    jobs: &std::collections::HashMap<String, crate::state::Job>,
    _data_dir: &std::path::Path,
) -> Option<(TaskAssignment, String)> {
    let (task_id, task) = tasks
        .iter()
        .find(|(_, t)| t.task_type == "map" && matches!(t.status, TaskStatus::Pending))?;

    let job_id = task.id.split(':').next().unwrap_or("").to_string();
    let job = jobs.get(&job_id)?;

    // Chunk file path served via master HTTP
    let chunk_file = format!(
        "/data/{}/chunks/chunk_{}",
        job_id,
        task.index.min(job.chunk_files.len().saturating_sub(1))
    );

    Some((
        TaskAssignment {
            task_id: task_id.clone(),
            job_id: job.id.clone(),
            task_type: "map".to_string(),
            task_index: task.index as i32,
            payload: Some(Payload::MapPayload(MapTaskPayload {
                wasm_hash: job.wasm_hash.clone(),
                chunk_file,
                num_reduces: job.num_reduces as i32,
            })),
        },
        job_id,
    ))
}

fn pick_reduce_task_inner(
    tasks: &mut std::collections::HashMap<String, crate::state::Task>,
    jobs: &mut std::collections::HashMap<String, crate::state::Job>,
) -> Option<(TaskAssignment, String)> {
    let job_id = jobs.iter().find_map(|(id, j)| {
        let all_maps_done = j.map_tasks.iter().all(|mt| {
            tasks
                .get(&mt.id)
                .map(|t| matches!(t.status, TaskStatus::Done))
                .unwrap_or(false)
        });
        if all_maps_done {
            Some(id.clone())
        } else {
            None
        }
    })?;

    let job = jobs.get_mut(&job_id)?;

    let (task_id, task) = tasks.iter().find(|(id, t)| {
        id.starts_with(job_id.as_str())
            && t.task_type == "reduce"
            && matches!(t.status, TaskStatus::Pending)
    })?;

    let partition = task.index as i32;

    // Build list of intermediate files for this partition from each map task
    let mut intermediate_files = Vec::new();
    for map_task in &job.map_tasks {
        if let Some((url, job_dir)) = job.intermediate_locs.get(&map_task.id) {
            let part_file = format!("{job_dir}/map_r{partition}.out");
            intermediate_files.push(DataLocation {
                url: format!("{url}/files"),
                file_path: part_file,
            });
        }
    }

    Some((
        TaskAssignment {
            task_id: task_id.clone(),
            job_id: job.id.clone(),
            task_type: "reduce".to_string(),
            task_index: task.index as i32,
            payload: Some(Payload::ReducePayload(ReduceTaskPayload {
                wasm_hash: job.wasm_hash.clone(),
                partition,
                intermediate_files,
            })),
        },
        job_id,
    ))
}

/// Check if a job is fully complete and assemble the final result.
pub async fn check_job_completion(state: &SharedState, task_id: &str) {
    let job_id = task_id.split(':').next().unwrap_or("").to_string();

    let _reduce_task_indices: Vec<usize> = {
        let jobs = state.jobs.read().await;
        let tasks = state.tasks.read().await;
        let Some(job) = jobs.get(&job_id) else {
            return;
        };

        let map_done = job.map_tasks.iter().all(|mt| {
            tasks
                .get(&mt.id)
                .map(|t| matches!(t.status, TaskStatus::Done))
                .unwrap_or(false)
        });
        let reduce_done = job.reduce_tasks.iter().all(|rt| {
            tasks
                .get(&rt.id)
                .map(|t| matches!(t.status, TaskStatus::Done))
                .unwrap_or(false)
        });

        if !(map_done && reduce_done) {
            return;
        }

        job.reduce_tasks.iter().map(|t| t.index).collect()
    };

    // Collect reduce output file locations
    let reduce_outputs: Vec<(String, String)> = {
        let jobs = state.jobs.read().await;
        let job = jobs.get(&job_id).unwrap();
        job.reduce_outputs.clone()
    };

    // Fetch reduce outputs to master disk, then merge
    let data_dir = &state.data_dir;
    let job_dir = data_dir.join(&job_id);
    let local_outputs_dir = job_dir.join("reduce_local");
    tokio::fs::create_dir_all(&local_outputs_dir).await.ok();

    let mut local_files = Vec::new();

    for (i, (url, file_path)) in reduce_outputs.iter().enumerate() {
        let local = local_outputs_dir.join(format!("reduce_{}.out", i));
        let fetch_url = format!("{url}/files/{file_path}");
        if let Err(e) = fetch_file(&fetch_url, &local).await {
            tracing::error!(url = %fetch_url, error = %e, "failed to fetch reduce output");
        } else if local.exists() {
            local_files.push(local);
        }
    }

    // Sort local files for deterministic merge order
    local_files.sort();

    let result_file = job_dir.join("result.txt");
    if let Err(e) = crate::storage::merge_sorted_files(&local_files, &result_file) {
        tracing::error!(error = %e, "failed to merge reduce outputs");
        return;
    }

    // Clean up fetched reduce files
    tokio::fs::remove_dir_all(&local_outputs_dir).await.ok();

    {
        let mut jobs = state.jobs.write().await;
        if let Some(j) = jobs.get_mut(&job_id) {
            j.result_file = Some(result_file);
            j.status = JobStatus::Done;
        }
    }

    tracing::info!(job_id = %job_id, "job complete, result on disk");
}

/// Fetch a file from a worker's data URL.
async fn fetch_file(url: &str, dest: &std::path::Path) -> std::io::Result<()> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotFound, e))?;

    if !resp.status().is_success() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("HTTP {}", resp.status()),
        ));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))?;

    std::fs::write(dest, bytes)?;
    Ok(())
}

/// Reschedule tasks from a dead worker back to pending.
pub async fn reschedule_worker_tasks(state: &SharedState, worker_id: &str) {
    let task_ids: Vec<String> = {
        let mut tasks = state.tasks.write().await;
        tasks
            .iter_mut()
            .filter_map(|(id, t)| {
                if let TaskStatus::Running(ref wid) = t.status {
                    if wid == worker_id {
                        t.status = TaskStatus::Pending;
                        t.started_at = None;
                        Some(id.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect()
    };

    if !task_ids.is_empty() {
        let task_ids_set: std::collections::HashSet<&str> =
            task_ids.iter().map(|s| s.as_str()).collect();
        let mut jobs = state.jobs.write().await;
        for job in jobs.values_mut() {
            for t in job.map_tasks.iter_mut().chain(job.reduce_tasks.iter_mut()) {
                if task_ids_set.contains(t.id.as_str()) {
                    t.status = TaskStatus::Pending;
                    t.started_at = None;
                }
            }
        }
    }

    tracing::info!(
        worker_id = %worker_id,
        count = task_ids.len(),
        "rescheduled tasks from evicted worker"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Job, Task, Worker};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!("mr_test_state_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        SharedState::new(dir)
    }

    async fn add_worker(state: &SharedState, id: &str, slots: u32) {
        let mut workers = state.workers.write().await;
        workers.insert(
            id.to_string(),
            Worker::new(id.to_string(), "127.0.0.1:0".to_string(), slots),
        );
    }

    async fn add_job_with_maps(
        state: &SharedState,
        job_id: &str,
        num_maps: usize,
        num_reduces: usize,
    ) {
        let (chunks_dir, _) =
            crate::storage::create_job_dirs(job_id, &state.data_dir).expect("create job dirs");

        let chunk_files: Vec<std::path::PathBuf> = (0..num_maps)
            .map(|i| {
                let p = chunks_dir.join(format!("chunk_{i}"));
                std::fs::write(&p, format!("chunk_data_{i}")).ok();
                p
            })
            .collect();

        let mut map_tasks = Vec::new();
        for i in 0..num_maps {
            let task = Task {
                id: format!("{}:map:{}", job_id, i),
                task_type: "map".to_string(),
                index: i,
                status: TaskStatus::Pending,
                started_at: None,
                retry_count: 0,
            };
            map_tasks.push(task.clone());
            state.tasks.write().await.insert(task.id.clone(), task);
        }

        let mut reduce_tasks = Vec::new();
        for i in 0..num_reduces {
            let task = Task {
                id: format!("{}:reduce:{}", job_id, i),
                task_type: "reduce".to_string(),
                index: i,
                status: TaskStatus::Pending,
                started_at: None,
                retry_count: 0,
            };
            reduce_tasks.push(task.clone());
            state.tasks.write().await.insert(task.id.clone(), task);
        }

        let job = Job {
            id: job_id.to_string(),
            wasm_hash: "abc123".to_string(),
            num_reduces,
            status: JobStatus::Pending,
            map_tasks,
            reduce_tasks,
            chunk_files,
            intermediate_locs: HashMap::new(),
            reduce_outputs: Vec::new(),
            result_file: None,
            result_downloaded: false,
        };
        state.jobs.write().await.insert(job_id.to_string(), job);
    }

    #[tokio::test]
    async fn test_pick_and_assign_map_task() {
        let state = make_state();
        add_worker(&state, "w1", 2).await;
        add_job_with_maps(&state, "job1", 3, 2).await;

        let task = pick_and_assign_task(&state, "w1").await;
        assert!(task.is_some());
        let t = task.unwrap();
        assert_eq!(t.task_type, "map");
        assert_eq!(t.job_id, "job1");

        if let Some(Payload::MapPayload(p)) = &t.payload {
            assert!(!p.chunk_file.is_empty());
            assert_eq!(p.wasm_hash, "abc123");
        } else {
            panic!("expected map payload");
        }
    }

    #[tokio::test]
    async fn test_no_slots() {
        let state = make_state();
        add_worker(&state, "w1", 0).await;
        add_job_with_maps(&state, "job1", 3, 2).await;

        let task = pick_and_assign_task(&state, "w1").await;
        assert!(task.is_none());
    }

    #[tokio::test]
    async fn test_reschedule_worker_tasks() {
        let state = make_state();
        add_worker(&state, "w1", 2).await;
        add_job_with_maps(&state, "job1", 2, 0).await;

        let assignment = pick_and_assign_task(&state, "w1").await.unwrap();
        let task_id = assignment.task_id;

        reschedule_worker_tasks(&state, "w1").await;

        let tasks = state.tasks.read().await;
        let t = tasks.get(&task_id).unwrap();
        assert!(matches!(t.status, TaskStatus::Pending));
    }
}

use std::hash::Hasher;
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use proto::mapreduce::task_assignment::Payload;
use proto::mapreduce::worker_service_client::WorkerServiceClient;
use proto::mapreduce::{TaskFailure, TaskRequest, TaskResult};
use tonic::transport::Channel;

use crate::state::WorkerState;

/// Main task loop: poll for tasks, execute via WASM runtime, report result/failure.
pub async fn task_loop(
    state: Arc<WorkerState>,
    mut client: WorkerServiceClient<Channel>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        if state.available_slots.load(Ordering::Relaxed) <= 0 {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            tokio::select! {
                _ = &mut shutdown_rx => {
                    tracing::info!("shutdown signal received, stopping task loop");
                    return;
                }
                else => {}
            };
        }

        let task = tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::info!("shutdown signal received, stopping task loop");
                return;
            }
            result = poll_task(&state, &mut client) => result,
        };

        match task {
            Ok(Some(assignment)) => {
                let task_id = assignment.task_id.clone();

                tracing::info!(
                    task_id = %task_id,
                    job_id = %assignment.job_id,
                    task_type = %assignment.task_type,
                    "received task"
                );

                {
                    let mut active = state.active_tasks.write().await;
                    active.insert(task_id.clone());
                }

                state.available_slots.fetch_sub(1, Ordering::Relaxed);

                match execute_task(&state, &assignment).await {
                    Ok(output_file) => {
                        report_result(&mut client, &task_id, &output_file, &state.data_url).await;
                    }
                    Err(e) => {
                        tracing::error!(task_id = %task_id, error = %e, "task failed");
                        report_failure(&mut client, &task_id, &e).await;
                    }
                }

                state.available_slots.fetch_add(1, Ordering::Relaxed);

                {
                    let mut active = state.active_tasks.write().await;
                    active.remove(&task_id);
                }
                {
                    let mut finished = state.finished_tasks.write().await;
                    finished.insert(task_id);
                }
            }
            Ok(None) => {
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to poll for task");
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    }
}

/// Poll the master for a task.
async fn poll_task(
    state: &WorkerState,
    client: &mut WorkerServiceClient<Channel>,
) -> Result<Option<proto::mapreduce::TaskAssignment>, String> {
    let request = TaskRequest {
        worker_id: state.id.clone(),
    };

    match client.get_task(request).await {
        Ok(resp) => Ok(Some(resp.into_inner())),
        Err(status) => {
            if status.code() == tonic::Code::Unavailable {
                Ok(None)
            } else {
                Err(format!("GetTask RPC error: {}", status))
            }
        }
    }
}

/// Fetch a file from a URL and write to a local path.
async fn fetch_file(url: &str, dest: &std::path::Path) -> Result<(), String> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| format!("HTTP GET failed: {}", e))?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("read response: {}", e))?;
    tokio::fs::write(dest, bytes)
        .await
        .map_err(|e| format!("write to disk: {}", e))?;
    Ok(())
}

/// Resolve WASM module — fetch from master if not cached locally.
async fn resolve_wasm(
    state: &WorkerState,
    master_url: &str,
    wasm_hash: &str,
) -> Result<Vec<u8>, String> {
    let wasm_dir = state.data_dir.join("wasm");
    std::fs::create_dir_all(&wasm_dir).ok();
    let local_path = wasm_dir.join(format!("{wasm_hash}.wasm"));

    if local_path.exists() {
        std::fs::read(&local_path).map_err(|e| format!("read cached wasm: {}", e))
    } else {
        let wasm_url = format!("{}/wasm/{}.wasm", master_url, wasm_hash);
        fetch_file(&wasm_url, &local_path).await?;
        std::fs::read(&local_path).map_err(|e| format!("read downloaded wasm: {}", e))
    }
}

/// Execute a map task: fetch chunk, run WASM, partition output.
async fn execute_map(
    state: &WorkerState,
    master_url: &str,
    assignment: &proto::mapreduce::TaskAssignment,
) -> Result<String, String> {
    let p = if let Some(Payload::MapPayload(pp)) = &assignment.payload {
        pp
    } else {
        return Err("no map payload".to_string());
    };

    let wasm = resolve_wasm(state, master_url, &p.wasm_hash).await?;
    let job_dir = state.data_dir.join(&assignment.job_id);
    std::fs::create_dir_all(&job_dir).ok();

    // Fetch chunk from master HTTP
    let chunk_local = job_dir.join("chunk_input");
    let chunk_path = p.chunk_file.trim_start_matches('/');
    let chunk_url = format!("{}/{}", master_url, chunk_path);
    fetch_file(&chunk_url, &chunk_local).await?;

    // Run WASM map function — produces raw key\tvalue lines
    let map_output = job_dir.join("map_raw.out");
    let output_size = crate::wasm_runtime::execute_map(
        &wasm,
        &chunk_local.display().to_string(),
        &map_output.display().to_string(),
    )?;

    tracing::info!(task_id = %assignment.task_id, bytes = output_size, "map wasm complete");

    // Partition output into R files by hash(key) mod R
    let num_reduces = if p.num_reduces <= 0 {
        1
    } else {
        p.num_reduces as usize
    };
    partition_map_output(&map_output, num_reduces, &job_dir)?;

    // Report data dir path so master can find partition files
    let output_file = assignment.job_id.clone();

    Ok(output_file)
}

/// Read map output (key\tvalue\n...), write each line to partition file hash(key) % R.
fn partition_map_output(
    map_output_path: &std::path::Path,
    num_reduces: usize,
    job_dir: &std::path::Path,
) -> Result<(), String> {
    let content =
        std::fs::read_to_string(map_output_path).map_err(|e| format!("read map output: {}", e))?;

    let mut partition_bufs: Vec<Vec<u8>> = vec![Vec::new(); num_reduces];

    for line in content.lines() {
        let key = match line.find('\t') {
            Some(pos) => &line[..pos],
            None => line,
        };

        let partition = fast_hash(key.as_bytes()) % num_reduces as u64;
        let idx = partition as usize;
        partition_bufs[idx].extend_from_slice(line.as_bytes());
        partition_bufs[idx].push(b'\n');
    }

    for (i, buf) in partition_bufs.iter().enumerate() {
        let part_file = job_dir.join(format!("map_r{}.out", i));
        std::fs::write(&part_file, buf).map_err(|e| format!("write partition {}: {}", i, e))?;
    }

    Ok(())
}

/// Execute a reduce task: fetch partition data from map workers, sort, run WASM.
async fn execute_reduce(
    state: &WorkerState,
    _master_url: &str,
    assignment: &proto::mapreduce::TaskAssignment,
) -> Result<String, String> {
    let p = if let Some(Payload::ReducePayload(pp)) = &assignment.payload {
        pp
    } else {
        return Err("no reduce payload".to_string());
    };

    let wasm = resolve_wasm(state, _master_url, &p.wasm_hash).await?;
    let job_dir = state.data_dir.join(&assignment.job_id);
    std::fs::create_dir_all(&job_dir).ok();

    // Fetch intermediate files for this partition from each map worker
    let mut all_data = Vec::new();
    for loc in &p.intermediate_files {
        // loc.url already includes /files prefix from master
        let url = format!("{}/{}", loc.url, loc.file_path);
        let safe_name = loc.file_path.replace(['/', '.'], "_");
        let tmp_file = job_dir.join(format!(
            "fetch_{}_{}",
            safe_name,
            loc.url.replace(['/', ':', '.'], "_")
        ));

        match fetch_file(&url, &tmp_file).await {
            Ok(()) => {
                let data =
                    std::fs::read(&tmp_file).map_err(|e| format!("read fetched file: {}", e))?;
                all_data.extend_from_slice(&data);
                let _ = std::fs::remove_file(&tmp_file);
            }
            Err(e) => {
                tracing::warn!(url = %url, error = %e, "failed to fetch intermediate file");
            }
        }
    }

    // Sort all data by key for the reduce phase
    let data_str = String::from_utf8_lossy(&all_data);
    let mut lines: Vec<&str> = data_str.lines().filter(|l| !l.is_empty()).collect();
    lines.sort_by(|a, b| {
        let ka = a.split('\t').next().unwrap_or(*a);
        let kb = b.split('\t').next().unwrap_or(*b);
        ka.cmp(kb)
    });

    // Write sorted input to a single file for WASM reduce
    let reduce_input = job_dir.join("reduce_input.out");
    let mut f =
        std::fs::File::create(&reduce_input).map_err(|e| format!("create reduce input: {}", e))?;
    for line in lines {
        writeln!(f, "{}", line).map_err(|e| format!("write reduce input: {}", e))?;
    }

    // Run WASM reduce on sorted data
    let reduce_output = job_dir.join("reduce.out");
    crate::wasm_runtime::execute_reduce(
        &wasm,
        &reduce_input.display().to_string(),
        &reduce_output.display().to_string(),
    )?;

    let output_file = format!("{}/reduce.out", assignment.job_id);

    Ok(output_file)
}

/// Dispatch to map or reduce executor.
async fn execute_task(
    state: &WorkerState,
    assignment: &proto::mapreduce::TaskAssignment,
) -> Result<String, String> {
    // Derive master HTTP data server URL from worker state
    let master_url = state.master_http_url.clone();

    match assignment.task_type.as_str() {
        "map" => execute_map(state, &master_url, assignment).await,
        "reduce" => execute_reduce(state, &master_url, assignment).await,
        _ => Err(format!("unknown task type: {}", assignment.task_type)),
    }
}

/// Report task completion — file path + data URL, no data transfer.
async fn report_result(
    client: &mut WorkerServiceClient<Channel>,
    task_id: &str,
    output_file: &str,
    worker_data_url: &str,
) {
    let result = TaskResult {
        task_id: task_id.to_string(),
        output_file: output_file.to_string(),
        worker_data_url: worker_data_url.to_string(),
    };

    match client.report_result(result).await {
        Ok(_) => tracing::info!(task_id = %task_id, "reported result"),
        Err(e) => tracing::error!(task_id = %task_id, error = %e, "failed to report result"),
    }
}

/// Report task failure to master.
async fn report_failure(client: &mut WorkerServiceClient<Channel>, task_id: &str, reason: &str) {
    let failure = TaskFailure {
        task_id: task_id.to_string(),
        reason: reason.to_string(),
    };

    match client.report_failure(failure).await {
        Ok(_) => tracing::info!(task_id = %task_id, "reported failure"),
        Err(e) => tracing::error!(task_id = %task_id, error = %e, "failed to report failure"),
    }
}

/// Fast hash for partitioning — consistent within a single job run.
fn fast_hash(bytes: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut h = DefaultHasher::new();
    h.write(bytes);
    h.finish()
}

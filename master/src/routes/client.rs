use axum::extract::path::Path;
use axum::extract::Multipart;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::sync::Arc;

use crate::cleanup;
use crate::state::{Job, JobStatus, SharedState, Task, TaskStatus};
use crate::storage;

/// Submit a new MapReduce job.
///
/// Multipart fields:
///   - wasm: the .wasm binary
///   - input: input file bytes (optional if generate_mb is provided)
///   - generate_mb: generate N MB of random data
///   - num_reduces: number of reduce tasks (default 4)
pub async fn submit_job(
    State(state): State<Arc<SharedState>>,
    mut multipart: Multipart,
) -> Response {
    let mut wasm_data: Option<Vec<u8>> = None;
    let mut input_data: Option<Vec<u8>> = None;
    let mut generate_mb: Option<u32> = None;
    let mut num_reduces: usize = 4;

    while let Some(field) = multipart.next_field().await.unwrap_or(None) {
        let name = field.name().map(|s| s.to_string());
        let data = match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(_) => continue,
        };

        match name.as_deref() {
            Some("wasm") => wasm_data = Some(data),
            Some("input") => input_data = Some(data),
            Some("generate_mb") => {
                generate_mb = Some(String::from_utf8_lossy(&data).trim().parse().unwrap_or(0));
            }
            Some("num_reduces") => {
                num_reduces = String::from_utf8_lossy(&data).trim().parse().unwrap_or(4);
            }
            _ => {}
        }
    }

    let Some(wasm) = wasm_data else {
        return StatusCode::BAD_REQUEST.into_response();
    };

    let job_id = uuid::Uuid::new_v4().to_string();
    let data_dir = state.data_dir.clone();

    // Create job directories on disk
    let (_chunks_dir, _wasm_dir) = storage::create_job_dirs(&job_id, &data_dir)
        .map_err(|e| {
            tracing::error!("failed to create job dirs: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })
        .unwrap();

    // Save WASM module by hash (deduplicated)
    let wasm_hash = storage::wasm_hash(&wasm);
    if let Err(e) = storage::save_wasm(&wasm, &wasm_hash, &data_dir) {
        tracing::error!("failed to save wasm: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Write input to disk (either from upload or generated)
    let input_path = match input_data {
        Some(data) => match storage::write_input(&data, &job_id, &data_dir) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("failed to write input: {}", e);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        },
        None => {
            let Some(mb) = generate_mb else {
                return StatusCode::BAD_REQUEST.into_response();
            };
            match storage::generate_and_write(mb, &job_id, &data_dir) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("failed to generate input: {}", e);
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            }
        }
    };

    // Split input file into chunks in the chunks/ directory
    let chunks_dir = data_dir.join(&job_id).join("chunks");
    let chunk_files = match storage::split_into_chunks(&input_path, &chunks_dir) {
        Ok(chunks) => chunks,
        Err(e) => {
            tracing::error!("failed to split input: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    // Remove the raw input file — we only need chunks
    let _ = std::fs::remove_file(&input_path);

    // Build map and reduce task metadata
    let mut map_tasks = Vec::new();
    for (i, _) in chunk_files.iter().enumerate() {
        map_tasks.push(Task {
            id: format!("{}:map:{}", job_id, i),
            task_type: "map".to_string(),
            index: i,
            status: TaskStatus::Pending,
            started_at: None,
            retry_count: 0,
        });
    }

    let mut reduce_tasks = Vec::new();
    for i in 0..num_reduces {
        reduce_tasks.push(Task {
            id: format!("{}:reduce:{}", job_id, i),
            task_type: "reduce".to_string(),
            index: i,
            status: TaskStatus::Pending,
            started_at: None,
            retry_count: 0,
        });
    }

    let job = Job {
        id: job_id.clone(),
        wasm_hash,
        num_reduces,
        status: JobStatus::Pending,
        map_tasks: map_tasks.clone(),
        reduce_tasks: reduce_tasks.clone(),
        chunk_files,
        intermediate_locs: std::collections::HashMap::new(),
        reduce_outputs: Vec::new(),
        result_file: None,
        result_downloaded: false,
    };

    {
        let mut jobs = state.jobs.write().await;
        jobs.insert(job_id.clone(), job);
    }

    {
        let mut tasks = state.tasks.write().await;
        for task in &map_tasks {
            tasks.insert(task.id.clone(), task.clone());
        }
        for task in &reduce_tasks {
            tasks.insert(task.id.clone(), task.clone());
        }
    }

    tracing::info!(
        "job {} created with {} map, {} reduce tasks",
        job_id,
        map_tasks.len(),
        reduce_tasks.len()
    );

    (StatusCode::OK, Json(json!({ "job_id": job_id }))).into_response()
}

/// Poll job status.
pub async fn job_status(
    Path(job_id): Path<String>,
    State(state): State<Arc<SharedState>>,
) -> Response {
    let (map_progress, reduce_progress, status_str) = {
        let jobs = state.jobs.read().await;
        let tasks = state.tasks.read().await;
        let Some(job) = jobs.get(&job_id) else {
            return StatusCode::NOT_FOUND.into_response();
        };

        let map_done = job
            .map_tasks
            .iter()
            .filter(|mt| {
                tasks
                    .get(&mt.id)
                    .map(|t| matches!(t.status, TaskStatus::Done))
                    .unwrap_or(false)
            })
            .count();
        let map_total = job.map_tasks.len();
        let map_progress = if map_total > 0 {
            map_done as f32 / map_total as f32
        } else {
            0.0
        };

        let reduce_done = job
            .reduce_tasks
            .iter()
            .filter(|rt| {
                tasks
                    .get(&rt.id)
                    .map(|t| matches!(t.status, TaskStatus::Done))
                    .unwrap_or(false)
            })
            .count();
        let reduce_total = job.reduce_tasks.len();
        let reduce_progress = if reduce_total > 0 {
            reduce_done as f32 / reduce_total as f32
        } else {
            0.0
        };

        let status_str = match job.status {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Done => "done",
            JobStatus::Failed => "failed",
        };

        (map_progress, reduce_progress, status_str)
    };

    (
        StatusCode::OK,
        Json(json!({
            "status": status_str,
            "map_progress": map_progress,
            "reduce_progress": reduce_progress,
        })),
    )
        .into_response()
}

/// Download job result — streams from disk file.
pub async fn job_result(
    Path(job_id): Path<String>,
    State(state): State<Arc<SharedState>>,
) -> Response {
    let result_path = {
        let mut jobs = state.jobs.write().await;
        let Some(job) = jobs.get_mut(&job_id) else {
            return StatusCode::NOT_FOUND.into_response();
        };

        if job.status != JobStatus::Done {
            return StatusCode::IM_A_TEAPOT.into_response();
        }

        if job.result_downloaded {
            return StatusCode::NOT_FOUND.into_response();
        }

        job.result_downloaded = true;

        let Some(path) = job.result_file.clone() else {
            return StatusCode::NOT_FOUND.into_response();
        };

        path
    };

    // Serve the result file from disk, with cleanup scheduled after response body completes
    match tokio::fs::read(&result_path).await {
        Ok(bytes) => cleanup::result_response(bytes, state, job_id),
        Err(e) => {
            tracing::error!(error = %e, "failed to read result file");
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

// Router is created in mod.rs to allow adding file-serving routes with same state

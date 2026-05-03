use axum::extract::{path::Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::sync::Arc;

use crate::state::WorkerState;

/// Start an HTTP status + data server on the given port.
/// Exposes:
///   - /status — endpoint for forced polls by the master
///   - /files/{path} — serve intermediate output files for shuffle
pub async fn start_status_server(state: Arc<WorkerState>, port: u16) -> std::io::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    let app = axum::Router::new()
        .route("/status", axum::routing::get(get_status))
        .route("/files/*path", axum::routing::get(serve_file))
        .route("/jobs/:job_id", axum::routing::delete(delete_job))
        .with_state(state);

    axum::serve(listener, app).await?;
    Ok(())
}

async fn get_status(
    State(state): State<Arc<WorkerState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let active = state.active_tasks.read().await;
    let finished = state.finished_tasks.read().await;

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "worker_id": state.id,
            "address": state.address,
            "available_slots": state.available_slots.load(std::sync::atomic::Ordering::Relaxed),
            "active_tasks": active.iter().cloned().collect::<Vec<_>>(),
            "finished_tasks": finished.iter().cloned().collect::<Vec<_>>(),
        })),
    )
}

/// Serve an intermediate file from the worker's data directory.
/// Path parameter is relative to the data_dir.
async fn serve_file(
    State(state): State<Arc<WorkerState>>,
    Path(relative_path): Path<String>,
) -> Response {
    // Security: reject path traversal attempts
    if relative_path.contains("..") {
        return StatusCode::FORBIDDEN.into_response();
    }

    let file_path = state.data_dir.join(&relative_path);

    match tokio::fs::read(&file_path).await {
        Ok(bytes) => (StatusCode::OK, bytes).into_response(),
        Err(e) => {
            tracing::debug!(error = %e, "file not found: {:?}", file_path);
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// Delete a job's local data directory, triggered by master's cleanup request.
async fn delete_job(
    State(state): State<Arc<WorkerState>>,
    Path(job_id): Path<String>,
) -> Response {
    if job_id.contains("..") {
        return StatusCode::FORBIDDEN.into_response();
    }

    let job_dir = state.data_dir.join(&job_id);
    match tokio::fs::remove_dir_all(&job_dir).await {
        Ok(()) => {
            tracing::info!(job_id = %job_id, "cleaned up job data on worker");
            StatusCode::OK.into_response()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(job_id = %job_id, "job data already gone on worker");
            StatusCode::OK.into_response()
        }
        Err(e) => {
            tracing::error!(job_id = %job_id, error = %e, "failed to clean up job data on worker");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

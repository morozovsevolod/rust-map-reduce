pub mod client;
pub mod grpc;

use axum::extract::path::Path;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

use crate::state::SharedState;

pub use grpc::{create_grpc_server, create_master_control};

/// Serve a file from disk, returning NOT_FOUND if missing.
async fn serve_file(path: std::path::PathBuf) -> Response {
    match tokio::fs::read(&path).await {
        Ok(bytes) => (StatusCode::OK, bytes).into_response(),
        Err(e) => {
            tracing::debug!(error = %e, "file not found: {:?}", path);
            StatusCode::NOT_FOUND.into_response()
        }
    }
}

/// Serve a chunk file: GET /data/{job_id}/chunks/{chunk_id}
pub async fn serve_chunk(
    State(state): State<Arc<SharedState>>,
    Path((job_id, chunk_id)): Path<(String, String)>,
) -> Response {
    let chunk_path = state.data_dir.join(&job_id).join("chunks").join(&chunk_id);

    serve_file(chunk_path).await
}

/// Serve a WASM module: GET /wasm/{hash}.wasm
/// Note: in axum 0.7 (matchit 0.7), the `.wasm` literal in the route pattern
/// is matched but NOT stripped from the captured parameter, so `hash` already
/// includes the `.wasm` extension. Use it directly as the filename.
pub async fn serve_wasm(
    State(state): State<Arc<SharedState>>,
    Path(filename): Path<String>,
) -> Response {
    let wasm_path = state.data_dir.join("wasm").join(filename);

    serve_file(wasm_path).await
}

/// Create the full Axum router (client API + file serving).
pub fn create_router(state: Arc<SharedState>) -> axum::Router {
    axum::Router::new()
        .route("/api/v1/jobs", axum::routing::post(client::submit_job))
        .route(
            "/api/v1/jobs/:job_id/status",
            axum::routing::get(client::job_status),
        )
        .route(
            "/api/v1/jobs/:job_id/result",
            axum::routing::get(client::job_result),
        )
        .route(
            "/data/:job_id/chunks/:chunk_id",
            axum::routing::get(serve_chunk),
        )
        .route("/wasm/:hash.wasm", axum::routing::get(serve_wasm))
        .with_state(state)
}

mod heartbeat;
mod state;
mod status;
mod task_loop;
mod wasm_runtime;

use proto::mapreduce::worker_service_client::WorkerServiceClient;
use state::WorkerState;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let master_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50051".to_string());

    let address = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1".to_string());

    let num_slots: i32 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    let status_port: u16 = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3001);

    // Derive master HTTP data server URL from gRPC URL
    // master_url = http://master:50051 → master_http_url = http://master:3000
    let master_http_url = std::env::args().nth(5).unwrap_or_else(|| {
        if let Some(host_port) = master_url.strip_prefix("http://") {
            let host = host_port.split(':').next().unwrap_or("127.0.0.1");
            format!("http://{}:3000", host)
        } else {
            "http://127.0.0.1:3000".to_string()
        }
    });

    let id = uuid::Uuid::new_v4().to_string();
    let status_address = format!("{}:{}", address, status_port);
    let data_dir = std::path::PathBuf::from(format!("/tmp/worker_data_{}", id));
    let data_url = format!("http://{}:{}", address, status_port);
    let state = WorkerState::new(
        id.clone(),
        address.clone(),
        status_address,
        data_url,
        master_http_url,
        data_dir,
        num_slots,
    );

    tracing::info!(
        worker_id = %id,
        address = %address,
        slots = num_slots,
        master = %master_url,
        status_port = status_port,
        "starting worker"
    );

    let max_msg: usize = 256 * 1024 * 1024; // 256 MB
    let client = WorkerServiceClient::connect(master_url.clone())
        .await
        .expect("failed to connect to master")
        .max_decoding_message_size(max_msg);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    // Spawn heartbeat loop (sends shutdown signal when it exits)
    let hb_state = state.clone();
    tokio::spawn(async move {
        heartbeat::heartbeat_loop(hb_state, client).await;
        let _ = shutdown_tx.send(());
    });

    // Spawn status server for forced polls
    let status_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = status::start_status_server(status_state, status_port).await {
            tracing::error!("status server error: {}", e);
        }
    });

    // Run task loop (exits when shutdown signal received)
    let client = WorkerServiceClient::connect(master_url)
        .await
        .expect("failed to connect to master for task loop")
        .max_decoding_message_size(max_msg);

    task_loop::task_loop(state, client, shutdown_rx).await;

    tracing::info!("worker stopped");
}

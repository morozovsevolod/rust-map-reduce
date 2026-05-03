use std::sync::Arc;

use proto::mapreduce::worker_service_client::WorkerServiceClient;
use proto::mapreduce::HeartbeatRequest;
use tonic::transport::Channel;

use crate::state::WorkerState;

/// Send heartbeats to master every 5 seconds.
/// The worker_address field carries the status server address for forced polls.
/// The worker_data_url carries the HTTP base URL for fetching intermediate files.
pub async fn heartbeat_loop(state: Arc<WorkerState>, mut client: WorkerServiceClient<Channel>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));

    loop {
        interval.tick().await;

        let req = HeartbeatRequest {
            worker_id: state.id.clone(),
            worker_address: state.status_address.clone(),
            available_slots: state
                .available_slots
                .load(std::sync::atomic::Ordering::Relaxed),
            worker_data_url: state.data_url.clone(),
        };

        match client.heartbeat(req).await {
            Ok(resp) => {
                if !resp.into_inner().alive {
                    tracing::warn!("master told us to shut down");
                    break;
                }
            }
            Err(e) => {
                tracing::error!("heartbeat failed: {}", e);
            }
        }
    }
}

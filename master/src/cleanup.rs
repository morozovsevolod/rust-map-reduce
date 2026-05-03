use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::stream;
use http_body::Frame;

use crate::state::SharedState;
use crate::storage;

/// Delay (seconds) after result response body completes before cleanup begins.
const CLEANUP_DELAY_SECS: u64 = 30;

/// Build a response that schedules cleanup only after the response body is fully consumed.
///
/// The result bytes are buffered in memory, so cleanup deleting the disk file
/// doesn't affect the response. The cleanup timer is spawned in the stream's
/// `poll_next` after the data is yielded — which happens when axum finishes
/// writing all bytes to the socket and the stream completes.
///
/// Additionally, even with the stream completing immediately, there's a 30s
/// safety delay. By the time the stream yields, the file has already been
/// read (async I/O), giving ~30s + read time head start. This is more than
/// enough for the client to receive the response.
pub fn result_response(
    bytes: Vec<u8>,
    state: Arc<SharedState>,
    job_id: String,
) -> Response {
    // Stream that yields the data, then on completion (next poll) spawns cleanup.
    // The cleanup task sleeps CLEANUP_DELAY_SECS before actually deleting anything.
    let s = stream::unfold(
        (Bytes::from(bytes), state, job_id, false),
        move |(data, st, jid, done)| async move {
            if !done {
                // First poll: yield the data as an HTTP body frame
                Some((Result::<Frame<Bytes>, String>::Ok(Frame::data(data)), (Bytes::new(), st, jid, true)))
            } else {
                // Second poll: stream is complete, response body has been sent.
                // Spawn the cleanup task with the safety delay.
                let st = st;
                let jid = jid;
                tokio::spawn(async move {
                    tracing::info!(
                        job_id = %jid,
                        delay_secs = CLEANUP_DELAY_SECS,
                        "scheduling cleanup (response fully sent)"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(CLEANUP_DELAY_SECS)).await;
                    do_cleanup(&st, &jid).await;
                });
                None
            }
        }
    );

    let body = axum::body::Body::new(http_body_util::StreamBody::new(s));

    (StatusCode::OK, body).into_response()
}

async fn do_cleanup(state: &SharedState, job_id: &str) {
    // Collect unique worker data URLs from this job
    let worker_urls: Vec<String> = {
        let jobs = state.jobs.read().await;
        let Some(job) = jobs.get(job_id) else {
            tracing::warn!(job_id = %job_id, "job disappeared before cleanup");
            return;
        };

        let mut urls = HashSet::new();
        for (url, _) in job.intermediate_locs.values() {
            urls.insert(url.clone());
        }
        for (url, _) in &job.reduce_outputs {
            urls.insert(url.clone());
        }
        urls.into_iter().collect()
    };

    // Notify each worker to clean up its local job data
    for url in &worker_urls {
        let cleanup_url = format!("{}/jobs/{}", url, job_id);
        match reqwest::Client::new()
            .delete(&cleanup_url)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!(url = %url, job_id = %job_id, "worker cleanup acknowledged");
            }
            Ok(resp) => {
                tracing::warn!(
                    url = %url,
                    job_id = %job_id,
                    status = %resp.status(),
                    "worker cleanup failed"
                );
            }
            Err(e) => {
                tracing::warn!(
                    url = %url,
                    job_id = %job_id,
                    error = %e,
                    "worker cleanup request failed"
                );
            }
        }
    }

    // Clean up master's own job directory
    if let Err(e) = storage::remove_job_data(job_id, &state.data_dir) {
        tracing::error!(job_id = %job_id, error = %e, "failed to clean up master job data");
    } else {
        tracing::info!(job_id = %job_id, "cleaned up master job data");
    }

    // Remove in-memory state
    {
        let mut jobs = state.jobs.write().await;
        jobs.remove(job_id);
    }
    {
        let mut tasks = state.tasks.write().await;
        let to_remove: Vec<String> = tasks
            .iter()
            .filter_map(|(id, _)| if id.starts_with(job_id) { Some(id.clone()) } else { None })
            .collect();
        for id in to_remove {
            tasks.remove(&id);
        }
    }

    tracing::info!(job_id = %job_id, "cleanup complete");
}

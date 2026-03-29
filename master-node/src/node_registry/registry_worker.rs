use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::{
    message::MasterMsg,
    node_heap::{NodeRegistry, TaskPushInfo, TaskPushKind},
};
use crate::generated::worker_node::{
    SubmitMapRequest, SubmitReduceRequest, worker_node_client::WorkerNodeClient,
};

pub async fn registry_worker(
    tx: mpsc::Sender<MasterMsg>,
    mut receiver: mpsc::Receiver<MasterMsg>,
) -> Result<()> {
    let mut registry = NodeRegistry::new(tx);

    loop {
        match receiver.recv().await {
            Some(MasterMsg::RegisterWorker {
                address,
                max_slots,
                respond_to,
            }) => {
                let worker_id = registry.register_worker(address.clone(), max_slots);
                info!(%worker_id, %address, max_slots, "Worker registered");
                let _ = respond_to.send(worker_id);
            }

            Some(MasterMsg::Heartbeat {
                worker_id,
                respond_to,
            }) => {
                let alive = registry.heartbeat(worker_id);
                if !alive {
                    warn!("Heartbeat from unknown worker {worker_id}");
                }
                let _ = respond_to.send(alive);
            }

            Some(MasterMsg::RequestTask {
                worker_id,
                respond_to,
            }) => {
                let task = registry.assign_task(worker_id);
                match &task {
                    Some(t) => {
                        info!(%worker_id, task_id = %t.id, kind = ?t.kind, "Assigned task to worker");
                        if let Some(push) = registry.build_push_info(worker_id, t) {
                            tokio::spawn(push_task_to_worker(push));
                        }
                    }
                    None => {
                        info!(%worker_id, pending = registry.pending_task_count(), "No task available for worker");
                    }
                }
                let _ = respond_to.send(task);
            }

            Some(MasterMsg::ReportTask {
                worker_id,
                task_id,
                success,
                reduce_keys,
                respond_to,
            }) => {
                info!(%worker_id, %task_id, success, ?reduce_keys, "Task report received");
                let result = registry.report_task(worker_id, task_id, success, reduce_keys);
                if let Err(ref e) = result {
                    warn!("ReportTask error: {e}");
                }
                let _ = respond_to.send(result);

                for push in registry.drain_to_workers() {
                    tokio::spawn(push_task_to_worker(push));
                }
            }

            Some(MasterMsg::SubmitJob {
                task_name,
                partitions,
                num_reduce_tasks,
                respond_to,
            }) => {
                let partition_count = partitions.len();
                let job_id = registry.submit_job(task_name.clone(), partitions, num_reduce_tasks);
                info!(%job_id, task_name, partition_count, num_reduce_tasks,
                      pending = registry.pending_task_count(), "Job submitted");
                let _ = respond_to.send(job_id);

                for push in registry.drain_to_workers() {
                    tokio::spawn(push_task_to_worker(push));
                }
            }

            Some(MasterMsg::PushFailed { worker_id, task_id }) => {
                warn!(%worker_id, %task_id, "Push failed — re-queuing task");
                registry.push_failed(worker_id, task_id);

                for push in registry.drain_to_workers() {
                    tokio::spawn(push_task_to_worker(push));
                }
            }

            Some(MasterMsg::GetJobStatus { job_id, respond_to }) => {
                let _ = respond_to.send(registry.job_state(job_id).cloned());
            }

            Some(MasterMsg::GetJobResultLocations { job_id, respond_to }) => {
                let _ = respond_to.send(registry.job_result_locations(job_id));
            }

            Some(MasterMsg::EvictDeadWorkers) => {
                let evicted = registry.evict_dead_workers();
                if !evicted.is_empty() {
                    info!(count = evicted.len(), "Evicted dead workers: {:?}", evicted);
                }
            }

            None => {
                warn!("Channel closed — registry worker shutting down.");
                break;
            }
        }
    }

    Ok(())
}

async fn push_task_to_worker(info: TaskPushInfo) {
    let worker_id = info.worker_id;
    let task_id = info.task_id;

    let mut client = match WorkerNodeClient::connect(info.worker_address.clone()).await {
        Ok(c) => c,
        Err(e) => {
            warn!(%task_id, "push_task: failed to connect to {}: {e}", info.worker_address);
            return;
        }
    };

    let failed = match info.kind {
        TaskPushKind::Map {
            data,
            num_reduce_tasks,
        } => {
            match client
                .submit_map(SubmitMapRequest {
                    job_id: info.job_id.to_string(),
                    task_id: task_id.to_string(),
                    task_name: info.task_name,
                    num_reduce_tasks,
                    data,
                })
                .await
            {
                Ok(_) => {
                    info!(%task_id, "Pushed map task to {}", info.worker_address);
                    false
                }
                Err(e) => {
                    warn!(%task_id, "submit_map to {} failed: {e}", info.worker_address);
                    true
                }
            }
        }
        TaskPushKind::Reduce {
            key,
            source_locations,
        } => {
            match client
                .submit_reduce(SubmitReduceRequest {
                    job_id: info.job_id.to_string(),
                    task_id: task_id.to_string(),
                    task_name: info.task_name,
                    key,
                    locations: source_locations,
                })
                .await
            {
                Ok(_) => {
                    info!(%task_id, "Pushed reduce task to {}", info.worker_address);
                    false
                }
                Err(e) => {
                    warn!(%task_id, "submit_reduce to {} failed: {e}", info.worker_address);
                    true
                }
            }
        }
    };

    if failed {
        let _ = info
            .registry_tx
            .send(MasterMsg::PushFailed { worker_id, task_id })
            .await;
    }
}

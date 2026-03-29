use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::{collections::HashMap, sync::Arc};
use tokio::{
    spawn,
    sync::{Mutex, RwLock, Semaphore},
    task::spawn_blocking,
};
use tonic::{Code, Request, metadata::MetadataValue, transport::Channel};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    generated::{
        master_node::{
            RegisterWorkerRequest, UpdateStatusRequest, master_node_client::MasterNodeClient,
        },
        worker_node::{ReceiveIntermediateDataRequest, worker_node_client::WorkerNodeClient},
    },
    workers::wasm_executor::WasmExecutor,
};

pub type PartitionStore = DashMap<i32, Vec<(String, Bytes)>>;
pub type GlobalJobStore = DashMap<String, Arc<PartitionStore>>;
pub type ReduceStore = DashMap<String, Arc<DashMap<i32, String>>>;

pub struct WorkerPool {
    semaphore: Arc<Semaphore>,
    worker_id: Arc<RwLock<Uuid>>,
    self_address: String,
    master_client: Arc<Mutex<MasterNodeClient<Channel>>>,
    executor: Arc<WasmExecutor>,
    store: Arc<GlobalJobStore>,
    reduce_store: Arc<ReduceStore>,
}

impl WorkerPool {
    pub async fn new(
        folder: String,
        max_workers: usize,
        worker_id: Uuid,
        self_address: String,
        master_client: Arc<Mutex<MasterNodeClient<Channel>>>,
    ) -> Result<Self> {
        let semaphore = Arc::new(Semaphore::new(max_workers));
        let executor = Arc::new(WasmExecutor::new(folder)?);
        let store = Arc::new(GlobalJobStore::new());
        let reduce_store = Arc::new(ReduceStore::new());

        Ok(WorkerPool {
            semaphore,
            worker_id: Arc::new(RwLock::new(worker_id)),
            self_address,
            master_client,
            executor,
            store,
            reduce_store,
        })
    }

    pub async fn submit_map_task(
        &self,
        job_id: String,
        task_id: String,
        task_name: String,
        input_data: Vec<u8>,
        num_reduce_partitions: u32,
    ) -> Result<()> {
        let permit = Semaphore::try_acquire_owned(self.semaphore.clone())?;

        info!(
            job_id, task_id, task_name,
            input_bytes = input_data.len(),
            input_preview = %String::from_utf8_lossy(&input_data[..input_data.len().min(120)]),
            "submit_map_task: input received"
        );

        if let Err(e) = self.executor.cache_task(&task_name) {
            warn!("Failed to cache WASM for task '{task_name}': {e}");
            return Err(e);
        }

        let executor = self.executor.clone();
        let store = self.store.clone();
        let master_client = self.master_client.clone();
        let worker_id_lock = self.worker_id.clone();
        let self_address = self.self_address.clone();

        spawn(async move {
            let (success, reduce_keys) = match execute_map(
                executor,
                store,
                task_name.clone(),
                input_data,
                job_id.clone(),
                num_reduce_partitions,
            )
            .await
            {
                Ok(partition_ids) => {
                    info!(job_id, task_id, partitions = ?partition_ids, "Map task produced partitions");
                    let keys = partition_ids.iter().map(|k| k.to_string()).collect();
                    (true, keys)
                }
                Err(e) => {
                    warn!("Map task {task_name}:{task_id} for job {job_id} failed: {e}");
                    (false, Vec::new())
                }
            };

            send_update_status(
                &master_client,
                &worker_id_lock,
                &self_address,
                UpdateStatusRequest {
                    task_id: task_id.clone(),
                    success,
                    reduce_keys,
                },
            )
            .await;

            if success {
                info!("Map task {task_name}:{task_id} for job {job_id} executed successfully");
            }

            drop(permit);
        });

        Ok(())
    }

    pub async fn submit_reduce_task(
        &self,
        job_id: String,
        task_id: String,
        task_name: String,
        key: i32,
        locations: Vec<String>,
    ) -> Result<()> {
        let permit = Semaphore::try_acquire_owned(self.semaphore.clone())?;

        if let Err(e) = self.executor.cache_task(&task_name) {
            warn!("Failed to cache WASM for task '{task_name}': {e}");
            return Err(e);
        }

        let executor = self.executor.clone();
        let master_client = self.master_client.clone();
        let reduce_store = self.reduce_store.clone();
        let worker_id_lock = self.worker_id.clone();
        let self_address = self.self_address.clone();

        spawn(async move {
            let mut success = true;
            let mut merged: HashMap<String, Vec<String>> = HashMap::new();

            info!(job_id, task_id, key, locations = ?locations, "Reduce task starting");

            for loc in &locations {
                let mut client = match WorkerNodeClient::connect(loc.clone()).await {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("Failed to connect to worker {loc}: {e}");
                        success = false;
                        break;
                    }
                };

                let response = match client
                    .receive_intermediate_data(ReceiveIntermediateDataRequest {
                        job_id: job_id.clone(),
                        key,
                    })
                    .await
                {
                    Ok(r) => r.into_inner(),
                    Err(e) => {
                        warn!("RPC error from {loc}: {e}");
                        success = false;
                        break;
                    }
                };

                for (k, list) in response.data {
                    merged.entry(k).or_default().extend(list.data);
                }
            }

            info!(
                job_id,
                key,
                merged_keys = merged.len(),
                "Merged intermediate, running reduce"
            );

            if success {
                let mut all_numbers: Vec<i64> = Vec::new();

                for (inter_key, values) in merged {
                    let exec = executor.clone();
                    let task_name_clone = task_name.clone();
                    let inter_key_clone = inter_key.clone();

                    let reduce_out = spawn_blocking(move || {
                        exec.execute_freduce(&task_name_clone, inter_key_clone, values)
                    })
                    .await;

                    match reduce_out {
                        Ok(Ok(output)) => {
                            for token in output.split(',') {
                                if let Ok(n) = token.trim().parse::<i64>() {
                                    all_numbers.push(n);
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            warn!("Wasm reduce `{task_name}` failed for key `{inter_key}`: {e}");
                            success = false;
                            break;
                        }
                        Err(e) => {
                            warn!("spawn_blocking panicked for reduce `{task_name}`: {e}");
                            success = false;
                            break;
                        }
                    }
                }

                if success {
                    all_numbers.sort();
                    all_numbers.dedup();
                    let result = all_numbers
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    info!(job_id, key, result, "Reduce bucket complete");

                    let job_map = reduce_store
                        .entry(job_id.clone())
                        .or_insert_with(|| Arc::new(DashMap::new()))
                        .clone();
                    job_map.insert(key, result);
                }
            }

            send_update_status(
                &master_client,
                &worker_id_lock,
                &self_address,
                UpdateStatusRequest {
                    task_id: task_id.clone(),
                    success,
                    reduce_keys: Vec::new(),
                },
            )
            .await;

            if success {
                info!("Reduce task {task_name}:{task_id} for job {job_id} finished successfully");
            } else {
                warn!("Reduce task {task_name}:{task_id} for job {job_id} finished with errors");
            }

            drop(permit);
        });

        Ok(())
    }

    pub fn get_partition_data(
        &self,
        job_id: &str,
        partition_id: i32,
    ) -> Option<Vec<(String, Bytes)>> {
        self.store
            .get(job_id)
            .and_then(|s| s.get(&partition_id).map(|p| p.clone()))
    }

    pub fn get_reduce_result(&self, job_id: &str, key: i32) -> Option<String> {
        self.reduce_store
            .get(job_id)
            .and_then(|m| m.get(&key).map(|v| v.clone()))
    }
}

async fn send_update_status(
    master_client: &Arc<Mutex<MasterNodeClient<Channel>>>,
    worker_id_lock: &Arc<RwLock<Uuid>>,
    self_address: &str,
    inner: UpdateStatusRequest,
) {
    let worker_id = *worker_id_lock.read().await;

    let result = {
        let mut client = master_client.lock().await;
        client
            .update_status(build_update_request(worker_id, inner.clone()))
            .await
    };

    match result {
        Ok(_) => {}
        Err(e) if e.code() == Code::NotFound || e.code() == Code::Unauthenticated => {
            warn!(%worker_id, "Master rejected update_status ({e}), re-registering");

            let new_id = re_register(master_client, self_address).await;
            if let Some(new_id) = new_id {
                *worker_id_lock.write().await = new_id;
                info!(%new_id, "Re-registered with master");

                let mut client = master_client.lock().await;
                if let Err(e) = client
                    .update_status(build_update_request(new_id, inner))
                    .await
                {
                    warn!(%new_id, "Retry update_status after re-register failed: {e}");
                }
            }
        }
        Err(e) => {
            warn!(%worker_id, "update_status failed: {e}");
        }
    }
}

async fn re_register(
    master_client: &Arc<Mutex<MasterNodeClient<Channel>>>,
    self_address: &str,
) -> Option<Uuid> {
    let mut client = master_client.lock().await;
    match client
        .register_worker(RegisterWorkerRequest {
            address: self_address.to_owned(),
        })
        .await
    {
        Ok(resp) => resp.into_inner().worker_id.parse().ok(),
        Err(e) => {
            warn!("re_register failed: {e}");
            None
        }
    }
}

fn build_update_request(
    worker_id: Uuid,
    inner: UpdateStatusRequest,
) -> Request<UpdateStatusRequest> {
    let mut req = Request::new(inner);
    req.metadata_mut().insert_bin(
        "worker-id-bin",
        MetadataValue::from_bytes(worker_id.as_bytes()),
    );
    req
}

fn calculate_partition(key: &str, num_partitions: u32) -> i32 {
    const FNV_OFFSET: u64 = 14695981039346656037;
    const FNV_PRIME: u64 = 1099511628211;
    let hash = key.bytes().fold(FNV_OFFSET, |acc, b| {
        acc.wrapping_mul(FNV_PRIME) ^ (b as u64)
    });
    (hash % num_partitions as u64) as i32
}

async fn execute_map(
    executor: Arc<WasmExecutor>,
    store: Arc<GlobalJobStore>,
    task_name: String,
    input_data: Vec<u8>,
    job_id: String,
    num_reduce_partitions: u32,
) -> Result<Vec<i32>> {
    let input_len = input_data.len();
    info!(job_id, input_len, "execute_map: calling fmap");

    let result = spawn_blocking(move || executor.execute_fmap(&task_name, input_data)).await??;

    info!(job_id, input_len, pairs = result.len(), "fmap returned");
    for (k, v) in result.iter().take(3) {
        info!(job_id, key = k, value = v, "sample fmap pair");
    }

    let job_store = store
        .entry(job_id.clone())
        .or_insert_with(|| Arc::new(PartitionStore::new()))
        .clone();

    let mut partition_ids: Vec<i32> = Vec::new();

    for (key, value) in result {
        let partition_id = calculate_partition(&key, num_reduce_partitions);
        job_store
            .entry(partition_id)
            .or_default()
            .push((key, Bytes::from(value)));

        if !partition_ids.contains(&partition_id) {
            partition_ids.push(partition_id);
        }
    }

    Ok(partition_ids)
}

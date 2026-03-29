use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    generated::worker_node::{
        ReceiveIntermediateDataRequest, ReceiveIntermediateDataResponse,
        ReceiveReduceResultRequest, ReceiveReduceResultResponse, StringList, SubmitMapRequest,
        SubmitMapResponse, SubmitReduceRequest, SubmitReduceResponse,
        worker_node_server::WorkerNode,
    },
    workers::worker_pool::WorkerPool,
};

pub struct WorkerService {
    pool: WorkerPool,
}

impl WorkerService {
    pub fn new(pool: WorkerPool) -> Self {
        WorkerService { pool }
    }
}

#[async_trait::async_trait]
impl WorkerNode for WorkerService {
    async fn submit_map(
        &self,
        request: Request<SubmitMapRequest>,
    ) -> Result<Response<SubmitMapResponse>, Status> {
        if let Some(addr) = request.remote_addr() {
            info!("Received SubmitMap from {addr}");
        }

        let SubmitMapRequest {
            job_id,
            task_id,
            task_name,
            num_reduce_tasks,
            data,
        } = request.into_inner();

        let input: Vec<u8> = data.into_iter().map(|b| b as u8).collect();

        if self
            .pool
            .submit_map_task(job_id, task_id, task_name, input, num_reduce_tasks)
            .await
            .is_err()
        {
            return Err(Status::resource_exhausted("Failed to submit map task"));
        }

        Ok(SubmitMapResponse { success: true }.into())
    }

    async fn submit_reduce(
        &self,
        request: Request<SubmitReduceRequest>,
    ) -> Result<Response<SubmitReduceResponse>, Status> {
        if let Some(addr) = request.remote_addr() {
            info!("Received SubmitReduce from {addr}");
        }

        let SubmitReduceRequest {
            job_id,
            task_id,
            task_name,
            key,
            locations,
        } = request.into_inner();

        if self
            .pool
            .submit_reduce_task(job_id, task_id, task_name, key, locations)
            .await
            .is_err()
        {
            return Err(Status::resource_exhausted("Failed to submit reduce task"));
        }

        Ok(SubmitReduceResponse { success: true }.into())
    }

    async fn receive_intermediate_data(
        &self,
        request: Request<ReceiveIntermediateDataRequest>,
    ) -> Result<Response<ReceiveIntermediateDataResponse>, Status> {
        if let Some(addr) = request.remote_addr() {
            info!("Received ReceiveIntermediateData from {addr}");
        }

        let ReceiveIntermediateDataRequest { job_id, key } = request.into_inner();
        info!(job_id, key, "Looking up intermediate data");

        let partition = match self.pool.get_partition_data(&job_id, key) {
            Some(p) => {
                info!(
                    job_id,
                    key,
                    entries = p.len(),
                    "Found intermediate partition"
                );
                p
            }
            None => {
                warn!(job_id, key, "No intermediate data found for job/partition");
                return Err(Status::not_found(
                    "Intermediate data for the requested job/partition not found",
                ));
            }
        };

        let mut response_map: HashMap<String, StringList> = HashMap::new();

        for (map_key, bytes) in partition {
            let value_str = match String::from_utf8(bytes.to_vec()) {
                Ok(v) => v,
                Err(_) => {
                    warn!("Failed to decode intermediate value for key '{map_key}'");
                    continue;
                }
            };

            response_map
                .entry(map_key)
                .or_insert_with(|| StringList { data: Vec::new() })
                .data
                .push(value_str);
        }

        info!(
            job_id,
            key,
            unique_keys = response_map.len(),
            "Returning intermediate data"
        );

        Ok(Response::new(ReceiveIntermediateDataResponse {
            success: true,
            data: response_map,
        }))
    }

    async fn receive_reduce_result(
        &self,
        request: Request<ReceiveReduceResultRequest>,
    ) -> Result<Response<ReceiveReduceResultResponse>, Status> {
        if let Some(addr) = request.remote_addr() {
            info!("Received ReceiveReduceResult from {addr}");
        }

        let ReceiveReduceResultRequest { job_id, key } = request.into_inner();
        info!(job_id, key, "Looking up reduce result");

        let data = self.pool.get_reduce_result(&job_id, key);

        match &data {
            Some(v) => info!(job_id, key, value = %v, "Found reduce result"),
            None => warn!(job_id, key, "No reduce result found"),
        }

        Ok(ReceiveReduceResultResponse {
            success: true,
            data,
        }
        .into())
    }
}

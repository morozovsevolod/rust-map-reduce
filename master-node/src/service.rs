use tokio::sync::{mpsc, oneshot};
use tonic::{Request, Response, Status, metadata::MetadataMap};
use uuid::Uuid;

use crate::{
    generated::{
        master_node::{
            GetJobResultsRequest, GetJobResultsResponse, GetJobStatusRequest, GetJobStatusResponse,
            JobStatus, RegisterWorkerRequest, RegisterWorkerResponse, SubmitJobRequest,
            SubmitJobResponse, TaskAssignment, TaskKind, UpdateStatusRequest, UpdateStatusResponse,
            master_node_server::MasterNode,
        },
        worker_node::{ReceiveReduceResultRequest, worker_node_client},
    },
    node_registry::{
        WorkerId,
        job::JobState,
        message::MasterMsg,
        task::{Task, TaskKind as InternalTaskKind},
    },
};

pub struct MasterNodeService {
    tx: mpsc::Sender<MasterMsg>,
    worker_slots: u32,
}

impl MasterNodeService {
    pub fn new(tx: mpsc::Sender<MasterMsg>, worker_slots: u32) -> Self {
        Self { tx, worker_slots }
    }

    async fn send<F, R>(&self, build: F) -> Result<R, Status>
    where
        F: FnOnce(oneshot::Sender<R>) -> MasterMsg,
    {
        let (respond_to, rx) = oneshot::channel();
        self.tx
            .send(build(respond_to))
            .await
            .map_err(|_| Status::unavailable("registry actor is down"))?;

        rx.await
            .map_err(|_| Status::internal("registry actor dropped the response"))
    }
}

#[async_trait::async_trait]
impl MasterNode for MasterNodeService {
    async fn heartbeat(&self, request: Request<()>) -> Result<Response<()>, Status> {
        let worker_id = extract_worker_id(request.metadata())?;

        let alive = self
            .send(|respond_to| MasterMsg::Heartbeat {
                worker_id,
                respond_to,
            })
            .await?;

        if alive {
            Ok(Response::new(()))
        } else {
            Err(Status::not_found(format!(
                "worker {worker_id} not registered; call RegisterWorker first"
            )))
        }
    }

    async fn update_status(
        &self,
        request: Request<UpdateStatusRequest>,
    ) -> Result<Response<UpdateStatusResponse>, Status> {
        let worker_id = extract_worker_id(request.metadata())?;
        let req = request.into_inner();

        if !req.task_id.is_empty() {
            let task_id = parse_uuid(&req.task_id, "task_id")?;
            let reduce_keys = if req.reduce_keys.is_empty() {
                None
            } else {
                let keys = req
                    .reduce_keys
                    .iter()
                    .map(|s| {
                        s.parse::<i32>().map_err(|_| {
                            Status::invalid_argument(format!(
                                "`reduce_keys` contains non-integer value: {s}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<i32>, Status>>()?;
                Some(keys)
            };

            let _ = self
                .send(|respond_to| MasterMsg::ReportTask {
                    worker_id,
                    task_id,
                    success: req.success,
                    reduce_keys,
                    respond_to,
                })
                .await?;
        }

        let next_task: Option<Task> = self
            .send(|respond_to| MasterMsg::RequestTask {
                worker_id,
                respond_to,
            })
            .await?;

        Ok(Response::new(build_update_response(next_task)))
    }

    async fn submit_job(
        &self,
        request: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobResponse>, Status> {
        let req = request.into_inner();

        let raw_partitions: Vec<Vec<i32>> = serde_json::from_str(&req.input)
            .map_err(|e| Status::invalid_argument(format!("invalid `input` JSON: {e}")))?;

        if raw_partitions.is_empty() {
            return Err(Status::invalid_argument(
                "`input` must contain at least one partition",
            ));
        }

        let partitions = raw_partitions
            .into_iter()
            .map(|data| (uuid::Uuid::new_v4(), data))
            .collect::<std::collections::HashMap<_, _>>();

        let num_reduce_tasks = partitions.len() as u32;

        let job_id = self
            .send(|respond_to| MasterMsg::SubmitJob {
                task_name: req.task_name,
                partitions,
                num_reduce_tasks,
                respond_to,
            })
            .await?;

        Ok(Response::new(SubmitJobResponse {
            job_id: job_id.to_string(),
            success: true,
        }))
    }

    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();

        if req.address.is_empty() {
            return Err(Status::invalid_argument("`address` must not be empty"));
        }

        let worker_id = self
            .send(|respond_to| MasterMsg::RegisterWorker {
                address: req.address,
                max_slots: self.worker_slots,
                respond_to,
            })
            .await?;

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: worker_id.to_string(),
        }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let job_id = parse_uuid(&request.into_inner().job_id, "job_id")?;

        let state = self
            .send(|respond_to| MasterMsg::GetJobStatus { job_id, respond_to })
            .await?
            .ok_or_else(|| Status::not_found(format!("job {job_id} not found")))?;

        let (status, error) = match state {
            JobState::Mapping => (JobStatus::Mapping as i32, String::new()),
            JobState::Reducing => (JobStatus::Reducing as i32, String::new()),
            JobState::Completed => (JobStatus::Completed as i32, String::new()),
            JobState::Failed(e) => (JobStatus::Failed as i32, e),
        };

        Ok(Response::new(GetJobStatusResponse {
            job_id: job_id.to_string(),
            status,
            error,
        }))
    }

    async fn get_job_results(
        &self,
        request: Request<GetJobResultsRequest>,
    ) -> Result<Response<GetJobResultsResponse>, Status> {
        let job_id = parse_uuid(&request.into_inner().job_id, "job_id")?;

        let state = self
            .send(|respond_to| MasterMsg::GetJobStatus { job_id, respond_to })
            .await?
            .ok_or_else(|| Status::not_found(format!("job {job_id} not found")))?;

        if state != JobState::Completed {
            return Err(Status::failed_precondition("job is not yet completed"));
        }

        let locations = self
            .send(|respond_to| MasterMsg::GetJobResultLocations { job_id, respond_to })
            .await?
            .ok_or_else(|| Status::internal("could not retrieve result locations"))?;

        let job_id_str = job_id.to_string();
        let fetches = locations.into_iter().map(|(key, address)| {
            let job_id_str = job_id_str.clone();
            async move {
                let mut client = worker_node_client::WorkerNodeClient::connect(address)
                    .await
                    .map_err(|e| Status::unavailable(format!("worker connect failed: {e}")))?;

                let resp = client
                    .receive_reduce_result(ReceiveReduceResultRequest {
                        job_id: job_id_str,
                        key,
                    })
                    .await?
                    .into_inner();

                Ok::<(i32, Option<String>), Status>((key, resp.data))
            }
        });

        let pairs = futures::future::try_join_all(fetches).await?;

        let results = pairs
            .into_iter()
            .filter_map(|(key, value)| value.map(|v| (key, v)))
            .collect();

        Ok(Response::new(GetJobResultsResponse {
            success: true,
            results,
        }))
    }
}

fn build_update_response(task: Option<Task>) -> UpdateStatusResponse {
    match task {
        None => UpdateStatusResponse {
            has_task: false,
            ..Default::default()
        },
        Some(t) => {
            let assignment = match &t.kind {
                InternalTaskKind::Map {
                    partition_id,
                    num_reduce_tasks,
                } => TaskAssignment {
                    task_id: t.id.to_string(),
                    kind: TaskKind::Map as i32,
                    partition_id: partition_id.to_string(),
                    num_reduce_tasks: *num_reduce_tasks,
                    source_worker_ids: vec![],
                    reduce_key: 0,
                },
                InternalTaskKind::Reduce {
                    key,
                    source_workers,
                } => TaskAssignment {
                    task_id: t.id.to_string(),
                    kind: TaskKind::Reduce as i32,
                    partition_id: String::new(),
                    num_reduce_tasks: 0,
                    reduce_key: *key,
                    source_worker_ids: source_workers.iter().map(|id| id.to_string()).collect(),
                },
            };
            UpdateStatusResponse {
                success: true,
                has_task: true,
                assignment: Some(assignment),
            }
        }
    }
}

fn extract_worker_id(meta: &MetadataMap) -> Result<WorkerId, Status> {
    let bytes = meta
        .get_bin("worker-id-bin")
        .ok_or_else(|| Status::unauthenticated("missing `worker-id-bin` metadata"))?
        .to_bytes()
        .map_err(|_| Status::unauthenticated("`worker-id-bin` metadata is not valid bytes"))?;

    Uuid::from_slice(&bytes)
        .map_err(|_| Status::unauthenticated("`worker-id-bin` is not a valid UUID"))
}

fn parse_uuid(s: &str, field: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s)
        .map_err(|_| Status::invalid_argument(format!("`{field}` is not a valid UUID: {s}")))
}

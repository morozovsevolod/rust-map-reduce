use std::sync::Arc;

use proto::mapreduce::master_control_server::{MasterControl, MasterControlServer};
use proto::mapreduce::worker_service_server::{WorkerService, WorkerServiceServer};
use proto::mapreduce::{
    Empty, HeartbeatRequest, HeartbeatResponse, PollWorkerStatusRequest, TaskFailure, TaskRequest,
    TaskResult, WorkerStatus,
};
use tonic::{Request, Response, Status};

use crate::scheduler::{self};
use crate::state::{JobStatus, SharedState, TaskStatus, Worker};

pub struct WorkerServiceImpl {
    state: Arc<SharedState>,
}

impl WorkerServiceImpl {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let now = std::time::Instant::now();

        let mut workers = self.state.workers.write().await;
        let worker = workers.entry(req.worker_id.clone()).or_insert_with(|| {
            Worker::new(
                req.worker_id.clone(),
                req.worker_address.clone(),
                req.available_slots as u32,
            )
        });

        worker.last_heartbeat = now;
        worker.poll_counter = 0;
        if !req.worker_data_url.is_empty() {
            worker.data_url = req.worker_data_url;
        }

        Ok(Response::new(HeartbeatResponse { alive: true }))
    }

    async fn get_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<proto::mapreduce::TaskAssignment>, Status> {
        let req = request.into_inner();

        let Some(assignment) = scheduler::pick_and_assign_task(&self.state, &req.worker_id).await
        else {
            return Err(Status::unavailable("no tasks available"));
        };

        Ok(Response::new(assignment))
    }

    async fn report_result(&self, request: Request<TaskResult>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();

        // Mark task done
        {
            let mut tasks = self.state.tasks.write().await;
            if let Some(task) = tasks.get_mut(&task_id) {
                task.status = TaskStatus::Done;
                task.started_at = None;
            }
        }

        // Track output location (intermediate for map, final for reduce)
        let job_id = task_id.split(':').next().unwrap_or("").to_string();
        {
            let mut jobs = self.state.jobs.write().await;
            if let Some(job) = jobs.get_mut(&job_id) {
                for t in job.map_tasks.iter_mut().chain(job.reduce_tasks.iter_mut()) {
                    if t.id == task_id {
                        t.status = TaskStatus::Done;
                    }
                }

                if task_id.contains(":map:") {
                    // Map task — store intermediate location for shuffle
                    job.intermediate_locs
                        .insert(task_id.clone(), (req.worker_data_url, req.output_file));
                } else if task_id.contains(":reduce:") {
                    // Reduce task — store output location for merge
                    job.reduce_outputs
                        .push((req.worker_data_url, req.output_file));
                }
            }
        }

        // Free worker slot
        {
            let mut workers = self.state.workers.write().await;
            for worker in workers.values_mut() {
                if worker.active_tasks.remove(&task_id).is_some() {
                    worker.finished_tasks.insert(task_id.clone(), ());
                    break;
                }
            }
        }

        tracing::info!(task_id = %task_id, "task completed (output on worker disk)");

        scheduler::check_job_completion(&self.state, &task_id).await;

        Ok(Response::new(Empty {}))
    }

    async fn report_failure(
        &self,
        request: Request<TaskFailure>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id.clone();
        let job_id = req.task_id.split(':').next().unwrap_or("").to_string();

        let (fail_count, marked_failed) = {
            let mut tasks = self.state.tasks.write().await;
            let (count, failed) = if let Some(task) = tasks.get_mut(&task_id) {
                if !matches!(task.status, TaskStatus::Running(_)) {
                    tracing::debug!(task_id = %task_id, "ignoring stale failure report");
                    (0, false)
                } else if task.retry_count >= 1 {
                    task.status = TaskStatus::Failed(2);
                    task.started_at = None;
                    (2, true)
                } else {
                    task.retry_count += 1;
                    task.status = TaskStatus::Pending;
                    task.started_at = None;
                    (1, false)
                }
            } else {
                (0, false)
            };
            (count, failed)
        };

        if marked_failed {
            tracing::error!(
                task_id = %task_id,
                "task failed {} times, permanent failure: {}",
                fail_count,
                req.reason
            );
            let mut jobs = self.state.jobs.write().await;
            if let Some(job) = jobs.get_mut(&job_id) {
                job.status = JobStatus::Failed;
            }
        } else {
            tracing::warn!(
                task_id = %task_id,
                "task failed (attempt {}), rescheduling: {}",
                fail_count,
                req.reason
            );
        }

        // Free worker slot
        {
            let mut workers = self.state.workers.write().await;
            for worker in workers.values_mut() {
                if worker.active_tasks.remove(&task_id).is_some() {
                    break;
                }
            }
        }

        Ok(Response::new(Empty {}))
    }
}

pub struct MasterControlServiceImpl {
    state: Arc<SharedState>,
}

impl MasterControlServiceImpl {
    pub fn new(state: Arc<SharedState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl MasterControl for MasterControlServiceImpl {
    async fn poll_worker_status(
        &self,
        request: Request<PollWorkerStatusRequest>,
    ) -> Result<Response<WorkerStatus>, Status> {
        let req = request.into_inner();
        let workers = self.state.workers.read().await;
        let worker = workers
            .get(&req.worker_id)
            .ok_or(Status::not_found("worker not found"))?;

        Ok(Response::new(WorkerStatus {
            active_tasks: worker.active_tasks.keys().cloned().collect(),
            finished_tasks: worker.finished_tasks.keys().cloned().collect(),
            available_slots: worker.available_slots() as i32,
        }))
    }
}

pub fn create_grpc_server(state: Arc<SharedState>) -> WorkerServiceServer<WorkerServiceImpl> {
    WorkerServiceServer::new(WorkerServiceImpl::new(state.clone()))
}

pub fn create_master_control(
    state: Arc<SharedState>,
) -> MasterControlServer<MasterControlServiceImpl> {
    MasterControlServer::new(MasterControlServiceImpl::new(state.clone()))
}

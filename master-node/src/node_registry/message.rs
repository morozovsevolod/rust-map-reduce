use std::collections::HashMap;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::{
    JobId, PartitionId, TaskId, WorkerId,
    job::{JobState, PartitionData},
    task::Task,
};

pub type HeartbeatResult = bool;

pub enum MasterMsg {
    RegisterWorker {
        address: String,
        max_slots: u32,
        respond_to: oneshot::Sender<Uuid>,
    },
    Heartbeat {
        worker_id: WorkerId,
        respond_to: oneshot::Sender<HeartbeatResult>,
    },
    RequestTask {
        worker_id: WorkerId,
        respond_to: oneshot::Sender<Option<Task>>,
    },
    ReportTask {
        worker_id: WorkerId,
        task_id: TaskId,
        success: bool,
        reduce_keys: Option<Vec<i32>>,
        respond_to: oneshot::Sender<anyhow::Result<()>>,
    },
    SubmitJob {
        task_name: String,
        partitions: HashMap<PartitionId, PartitionData>,
        num_reduce_tasks: u32,
        respond_to: oneshot::Sender<JobId>,
    },
    GetJobStatus {
        job_id: JobId,
        respond_to: oneshot::Sender<Option<JobState>>,
    },
    GetJobResultLocations {
        job_id: JobId,
        respond_to: oneshot::Sender<Option<Vec<(i32, String)>>>,
    },
    PushFailed {
        worker_id: WorkerId,
        task_id: TaskId,
    },
    EvictDeadWorkers,
}

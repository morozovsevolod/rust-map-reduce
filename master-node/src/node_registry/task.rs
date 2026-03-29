use tokio::time::Instant;
use uuid::Uuid;

use super::{JobId, PartitionId, TaskId, WorkerId};

#[derive(Debug, Clone, PartialEq)]
pub enum TaskState {
    Idle,
    InProgress {
        worker_id: WorkerId,
        assigned_at: Instant,
    },
    Completed,
    Failed {
        reason: String,
        attempts: u32,
    },
}

#[derive(Debug, Clone)]
pub enum TaskKind {
    Map {
        partition_id: PartitionId,
        num_reduce_tasks: u32,
    },
    Reduce {
        key: i32,
        source_workers: Vec<WorkerId>,
    },
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: TaskId,
    pub job_id: JobId,
    pub kind: TaskKind,
    pub state: TaskState,
    pub attempts: u32,
}

impl Task {
    pub fn new_map(job_id: JobId, partition_id: PartitionId, num_reduce_tasks: u32) -> Self {
        Task {
            id: Uuid::new_v4(),
            job_id,
            kind: TaskKind::Map {
                partition_id,
                num_reduce_tasks,
            },
            state: TaskState::Idle,
            attempts: 0,
        }
    }

    pub fn new_reduce(job_id: JobId, key: i32, source_workers: Vec<WorkerId>) -> Self {
        Task {
            id: Uuid::new_v4(),
            job_id,
            kind: TaskKind::Reduce {
                key,
                source_workers,
            },
            state: TaskState::Idle,
            attempts: 0,
        }
    }

    pub fn is_map(&self) -> bool {
        matches!(self.kind, TaskKind::Map { .. })
    }
}

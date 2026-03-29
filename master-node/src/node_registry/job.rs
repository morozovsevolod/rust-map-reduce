use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use super::{JobId, PartitionId, TaskId, WorkerId};

pub type PartitionData = Vec<i32>;

#[derive(Debug, Clone, PartialEq)]
pub enum JobState {
    Mapping,
    Reducing,
    Completed,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: JobId,
    pub task_name: String,
    pub state: JobState,

    pub map_task_ids: HashSet<TaskId>,
    pub pending_map_count: usize,
    pub partitions: HashMap<PartitionId, PartitionData>,

    pub reduce_task_ids: HashSet<TaskId>,
    pub pending_reduce_count: usize,
    pub num_reduce_tasks: u32,
    pub completed_reduces: HashMap<i32, WorkerId>,
}

impl Job {
    pub fn new(
        task_name: String,
        partitions: HashMap<PartitionId, PartitionData>,
        num_reduce_tasks: u32,
    ) -> Self {
        Job {
            id: Uuid::new_v4(),
            task_name,
            state: JobState::Mapping,
            map_task_ids: HashSet::new(),
            pending_map_count: 0,
            partitions,
            reduce_task_ids: HashSet::new(),
            pending_reduce_count: 0,
            num_reduce_tasks,
            completed_reduces: HashMap::new(),
        }
    }

    pub fn is_map_phase_done(&self) -> bool {
        self.pending_map_count == 0 && !self.map_task_ids.is_empty()
    }

    pub fn is_reduce_phase_done(&self) -> bool {
        self.pending_reduce_count == 0 && !self.reduce_task_ids.is_empty()
    }
}

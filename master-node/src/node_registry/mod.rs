use uuid::Uuid;

pub mod job;
pub mod message;
pub mod node_heap;
pub mod registry_worker;
pub mod task;

pub type WorkerId = Uuid;
pub type TaskId = Uuid;
pub type PartitionId = Uuid;
pub type JobId = Uuid;

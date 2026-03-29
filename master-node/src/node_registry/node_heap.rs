use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::{sync::mpsc, time::Instant};
use uuid::Uuid;

use super::{
    JobId, PartitionId, TaskId, WorkerId,
    job::{Job, JobState, PartitionData},
    message::MasterMsg,
    task::{Task, TaskKind, TaskState},
};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_TASK_ATTEMPTS: u32 = 3;

pub struct WorkerNode {
    pub worker_id: WorkerId,
    pub address: String,
    pub available_slots: u32,
    pub max_slots: u32,
    pub last_seen: Instant,
    pub active_tasks: HashMap<TaskId, Task>,
    pub completed_task_ids: Vec<TaskId>,
    pub map_reduce_keys: HashMap<TaskId, Vec<i32>>,
}

pub struct TaskPushInfo {
    pub worker_id: WorkerId,
    pub worker_address: String,
    pub job_id: Uuid,
    pub task_id: Uuid,
    pub task_name: String,
    pub kind: TaskPushKind,
    pub registry_tx: mpsc::Sender<MasterMsg>,
}

pub enum TaskPushKind {
    Map {
        data: Vec<i32>,
        num_reduce_tasks: u32,
    },
    Reduce {
        key: i32,
        source_locations: Vec<String>,
    },
}

#[derive(Debug, Eq, PartialEq)]
struct LoadEntry {
    worker_id: WorkerId,
    available_slots: u32,
}
impl Ord for LoadEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.available_slots.cmp(&other.available_slots)
    }
}
impl PartialOrd for LoadEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Eq, PartialEq)]
struct HeartbeatEntry {
    worker_id: WorkerId,
    last_seen: Reverse<Instant>,
}
impl Ord for HeartbeatEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_seen.cmp(&other.last_seen)
    }
}
impl PartialOrd for HeartbeatEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct NodeRegistry {
    workers: HashMap<WorkerId, WorkerNode>,
    load_heap: BinaryHeap<LoadEntry>,
    heartbeat_heap: BinaryHeap<HeartbeatEntry>,
    pending_tasks: VecDeque<Task>,
    jobs: HashMap<JobId, Job>,
    registry_tx: mpsc::Sender<MasterMsg>,
}

impl NodeRegistry {
    pub fn new(registry_tx: mpsc::Sender<MasterMsg>) -> Self {
        NodeRegistry {
            workers: HashMap::new(),
            load_heap: BinaryHeap::new(),
            heartbeat_heap: BinaryHeap::new(),
            pending_tasks: VecDeque::new(),
            jobs: HashMap::new(),
            registry_tx,
        }
    }

    pub fn register_worker(&mut self, address: String, max_slots: u32) -> WorkerId {
        let worker_id = Uuid::new_v4();
        let now = Instant::now();

        self.workers.insert(
            worker_id,
            WorkerNode {
                worker_id,
                address,
                available_slots: max_slots,
                max_slots,
                last_seen: now,
                active_tasks: HashMap::new(),
                completed_task_ids: Vec::new(),
                map_reduce_keys: HashMap::new(),
            },
        );

        self.load_heap.push(LoadEntry {
            worker_id,
            available_slots: max_slots,
        });
        self.heartbeat_heap.push(HeartbeatEntry {
            worker_id,
            last_seen: Reverse(now),
        });
        worker_id
    }

    pub fn heartbeat(&mut self, worker_id: WorkerId) -> bool {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            return false;
        };
        let now = Instant::now();
        worker.last_seen = now;
        self.heartbeat_heap.push(HeartbeatEntry {
            worker_id,
            last_seen: Reverse(now),
        });
        true
    }

    pub fn evict_dead_workers(&mut self) -> Vec<WorkerId> {
        let mut evicted = Vec::new();
        loop {
            let Some(entry) = self.heartbeat_heap.peek() else {
                break;
            };
            let worker_id = entry.worker_id;
            let entry_last_seen = entry.last_seen.0;

            match self.workers.get(&worker_id) {
                None => {
                    self.heartbeat_heap.pop();
                }
                Some(w) if w.last_seen != entry_last_seen => {
                    self.heartbeat_heap.pop();
                }
                Some(_) if entry_last_seen.elapsed() < HEARTBEAT_TIMEOUT => {
                    break;
                }
                Some(_) => {
                    self.heartbeat_heap.pop();
                    let dead = self.workers.remove(&worker_id).unwrap();
                    for (_, task) in dead.active_tasks {
                        self.requeue_or_drop(task);
                    }
                    evicted.push(worker_id);
                }
            }
        }
        evicted
    }

    pub fn submit_job(
        &mut self,
        task_name: String,
        partitions: HashMap<PartitionId, PartitionData>,
        num_reduce_tasks: u32,
    ) -> JobId {
        let mut job = Job::new(task_name, partitions.clone(), num_reduce_tasks);

        for partition_id in partitions.keys() {
            let task = Task::new_map(job.id, *partition_id, num_reduce_tasks);
            job.map_task_ids.insert(task.id);
            job.pending_map_count += 1;
            self.pending_tasks.push_back(task);
        }

        let job_id = job.id;
        self.jobs.insert(job_id, job);
        job_id
    }

    pub fn job_state(&self, job_id: JobId) -> Option<&JobState> {
        self.jobs.get(&job_id).map(|j| &j.state)
    }

    pub fn job_result_locations(&self, job_id: JobId) -> Option<Vec<(i32, String)>> {
        let job = self.jobs.get(&job_id)?;

        let locations = job
            .completed_reduces
            .iter()
            .filter_map(|(&key, &worker_id)| {
                let address = self.workers.get(&worker_id)?.address.clone();
                Some((key, address))
            })
            .collect();

        Some(locations)
    }

    pub fn partition_data(
        &self,
        job_id: JobId,
        partition_id: PartitionId,
    ) -> Option<&PartitionData> {
        self.jobs.get(&job_id)?.partitions.get(&partition_id)
    }

    pub fn pending_task_count(&self) -> usize {
        self.pending_tasks.len()
    }

    pub fn drain_to_workers(&mut self) -> Vec<TaskPushInfo> {
        let idle: Vec<WorkerId> = self
            .workers
            .iter()
            .filter(|(_, w)| w.available_slots > 0)
            .map(|(&id, _)| id)
            .collect();

        let mut pushes = Vec::new();

        for worker_id in idle {
            loop {
                match self.workers.get(&worker_id) {
                    Some(w) if w.available_slots > 0 && !self.pending_tasks.is_empty() => {}
                    _ => break,
                }
                if let Some(task) = self.assign_task(worker_id) {
                    if let Some(push) = self.build_push_info(worker_id, &task) {
                        pushes.push(push);
                    }
                } else {
                    break;
                }
            }
            if self.pending_tasks.is_empty() {
                break;
            }
        }

        pushes
    }

    pub fn push_failed(&mut self, worker_id: WorkerId, task_id: TaskId) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            return;
        };

        if let Some(mut task) = worker.active_tasks.remove(&task_id) {
            worker.available_slots = (worker.available_slots + 1).min(worker.max_slots);
            self.load_heap.push(LoadEntry {
                worker_id,
                available_slots: worker.available_slots,
            });
            task.attempts = task.attempts.saturating_sub(1);
            task.state = TaskState::Idle;
            self.pending_tasks.push_front(task);
        }
    }

    pub fn build_push_info(&self, worker_id: WorkerId, task: &Task) -> Option<TaskPushInfo> {
        let worker = self.workers.get(&worker_id)?;
        let job = self.jobs.get(&task.job_id)?;

        let kind = match &task.kind {
            TaskKind::Map {
                partition_id,
                num_reduce_tasks,
            } => {
                let data = job.partitions.get(partition_id)?.clone();
                TaskPushKind::Map {
                    data,
                    num_reduce_tasks: *num_reduce_tasks,
                }
            }
            TaskKind::Reduce {
                key,
                source_workers,
            } => {
                let source_locations = source_workers
                    .iter()
                    .filter_map(|wid| self.workers.get(wid).map(|w| w.address.clone()))
                    .collect();
                TaskPushKind::Reduce {
                    key: *key,
                    source_locations,
                }
            }
        };

        Some(TaskPushInfo {
            worker_id,
            worker_address: worker.address.clone(),
            job_id: task.job_id,
            task_id: task.id,
            task_name: job.task_name.clone(),
            kind,
            registry_tx: self.registry_tx.clone(),
        })
    }

    pub fn assign_task(&mut self, worker_id: WorkerId) -> Option<Task> {
        let worker = self.workers.get_mut(&worker_id)?;
        if worker.available_slots == 0 || self.pending_tasks.is_empty() {
            return None;
        }

        let mut task = self.pending_tasks.pop_front()?;
        task.state = TaskState::InProgress {
            worker_id,
            assigned_at: Instant::now(),
        };
        task.attempts += 1;

        worker.available_slots -= 1;
        worker.active_tasks.insert(task.id, task.clone());
        self.load_heap.push(LoadEntry {
            worker_id,
            available_slots: worker.available_slots,
        });

        Some(task)
    }

    pub fn report_task(
        &mut self,
        worker_id: WorkerId,
        task_id: TaskId,
        success: bool,
        reduce_keys: Option<Vec<i32>>,
    ) -> Result<()> {
        let worker = self
            .workers
            .get_mut(&worker_id)
            .ok_or_else(|| anyhow!("Unknown worker {worker_id}"))?;

        let task = worker
            .active_tasks
            .remove(&task_id)
            .ok_or_else(|| anyhow!("Task {task_id} not active on worker {worker_id}"))?;

        worker.available_slots = (worker.available_slots + 1).min(worker.max_slots);
        self.load_heap.push(LoadEntry {
            worker_id,
            available_slots: worker.available_slots,
        });

        let job_id = task.job_id;

        if success {
            if let Some(w) = self.workers.get_mut(&worker_id) {
                w.completed_task_ids.push(task_id);
            }

            if task.is_map() {
                if let Some(keys) = reduce_keys
                    && let Some(w) = self.workers.get_mut(&worker_id) {
                        w.map_reduce_keys.insert(task_id, keys);
                    }

                let should_schedule = if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.pending_map_count = job.pending_map_count.saturating_sub(1);
                    if job.is_map_phase_done() {
                        job.state = JobState::Reducing;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };

                if should_schedule {
                    self.schedule_reduce_phase(job_id);
                }
            } else if let Some(job) = self.jobs.get_mut(&job_id) {
                if let TaskKind::Reduce { key, .. } = task.kind {
                    job.completed_reduces.insert(key, worker_id);
                }
                job.pending_reduce_count = job.pending_reduce_count.saturating_sub(1);
                if job.is_reduce_phase_done() {
                    job.state = JobState::Completed;
                }
            }
        } else {
            self.requeue_or_drop(task);
        }

        Ok(())
    }

    fn schedule_reduce_phase(&mut self, job_id: JobId) {
        let Some(job) = self.jobs.get(&job_id) else {
            return;
        };
        let map_task_ids: HashSet<TaskId> = job.map_task_ids.clone();

        let mut key_to_workers: HashMap<i32, Vec<WorkerId>> = HashMap::new();
        for (worker_id, worker) in &self.workers {
            for (task_id, keys) in &worker.map_reduce_keys {
                if map_task_ids.contains(task_id) {
                    for &key in keys {
                        key_to_workers.entry(key).or_default().push(*worker_id);
                    }
                }
            }
        }

        if key_to_workers.is_empty() {
            if let Some(job) = self.jobs.get_mut(&job_id) {
                job.state = JobState::Completed;
            }
            return;
        }

        let job = self.jobs.get_mut(&job_id).unwrap();
        for (key, source_workers) in key_to_workers {
            let task = Task::new_reduce(job_id, key, source_workers);
            job.reduce_task_ids.insert(task.id);
            job.pending_reduce_count += 1;
            self.pending_tasks.push_back(task);
        }
    }

    fn requeue_or_drop(&mut self, mut task: Task) {
        if task.attempts < MAX_TASK_ATTEMPTS {
            task.state = TaskState::Idle;
            self.pending_tasks.push_back(task);
        } else if let Some(job) = self.jobs.get_mut(&task.job_id) {
            job.state = JobState::Failed(format!("task {} exceeded max attempts", task.id));
        }
    }
}

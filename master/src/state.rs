use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Metadata-only state. All data files live on disk.

#[derive(Debug, Clone, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub wasm_hash: String,
    pub num_reduces: usize,
    pub status: JobStatus,
    pub map_tasks: Vec<Task>,
    pub reduce_tasks: Vec<Task>,
    /// Chunk file paths on master disk
    pub chunk_files: Vec<PathBuf>,
    /// task_id -> (worker_data_url, output_file_path) for completed map tasks
    pub intermediate_locs: HashMap<String, (String, String)>,
    /// Reduce output file locations: (worker_data_url, output_file_path)
    pub reduce_outputs: Vec<(String, String)>,
    /// Final merged result path on master disk
    pub result_file: Option<PathBuf>,
    pub result_downloaded: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running(String),
    Done,
    Failed(u32),
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub task_type: String,
    pub index: usize,
    pub status: TaskStatus,
    pub started_at: Option<Instant>,
    pub retry_count: u32,
}

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub address: String,
    pub total_slots: u32,
    pub active_tasks: HashMap<String, ()>,
    pub finished_tasks: HashMap<String, ()>,
    pub last_heartbeat: Instant,
    pub poll_counter: u32,
    /// Base URL for fetching intermediate files (e.g., "http://10.0.0.5:8080")
    pub data_url: String,
}

impl Worker {
    pub fn new(id: String, address: String, slots: u32) -> Self {
        Self {
            id,
            address,
            total_slots: slots,
            active_tasks: HashMap::new(),
            finished_tasks: HashMap::new(),
            last_heartbeat: Instant::now(),
            poll_counter: 0,
            data_url: String::new(),
        }
    }

    pub fn available_slots(&self) -> u32 {
        self.total_slots
            .saturating_sub(self.active_tasks.len() as u32)
    }
}

pub struct SharedState {
    pub workers: RwLock<HashMap<String, Worker>>,
    pub jobs: RwLock<HashMap<String, Job>>,
    pub tasks: RwLock<HashMap<String, Task>>,
    pub data_dir: PathBuf,
}

impl SharedState {
    pub fn new(data_dir: PathBuf) -> Arc<Self> {
        std::fs::create_dir_all(&data_dir).ok();
        Arc::new(Self {
            workers: RwLock::new(HashMap::new()),
            jobs: RwLock::new(HashMap::new()),
            tasks: RwLock::new(HashMap::new()),
            data_dir,
        })
    }
}

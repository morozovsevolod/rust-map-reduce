use std::collections::HashSet;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;

/// Worker-local state.
pub struct WorkerState {
    pub id: String,
    pub address: String,
    pub status_address: String,
    pub data_url: String,
    pub master_http_url: String,
    pub data_dir: std::path::PathBuf,
    pub available_slots: Arc<AtomicI32>,
    pub active_tasks: Arc<tokio::sync::RwLock<HashSet<String>>>,
    pub finished_tasks: Arc<tokio::sync::RwLock<HashSet<String>>>,
}

impl WorkerState {
    pub fn new(
        id: String,
        address: String,
        status_address: String,
        data_url: String,
        master_http_url: String,
        data_dir: std::path::PathBuf,
        slots: i32,
    ) -> Arc<Self> {
        std::fs::create_dir_all(&data_dir).ok();
        Arc::new(Self {
            id,
            address,
            status_address,
            data_url,
            master_http_url,
            data_dir,
            available_slots: Arc::new(AtomicI32::new(slots)),
            active_tasks: Arc::new(tokio::sync::RwLock::new(HashSet::new())),
            finished_tasks: Arc::new(tokio::sync::RwLock::new(HashSet::new())),
        })
    }
}

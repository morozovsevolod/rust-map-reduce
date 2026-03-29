use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;
use tracing::info;

use crate::{
    config::{Config, load_config},
    generated::{
        master_node::{RegisterWorkerRequest, master_node_client::MasterNodeClient},
        worker_node::worker_node_server::WorkerNodeServer,
    },
    service::worker_service::WorkerService,
    workers::worker_pool::WorkerPool,
};

pub mod config;
pub mod generated;
pub mod service;
pub mod workers;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let Config {
        master,
        folder,
        max_workers,
        address,
    } = load_config()?;

    let addr = address.parse().unwrap();
    let self_address = format!("http://{}", address);
    info!(%addr, "worker node listening");

    let mut master_client = MasterNodeClient::connect(format!("http://{}", master)).await?;
    info!("Connected to the master");

    let register_resp = master_client
        .register_worker(RegisterWorkerRequest {
            address: self_address.clone(),
        })
        .await?
        .into_inner();
    let worker_id = register_resp.worker_id.parse()?;
    info!(%worker_id, "Registered with master");

    let shared_client = Arc::new(Mutex::new(master_client));
    let pool = WorkerPool::new(folder, max_workers, worker_id, self_address, shared_client).await?;

    Server::builder()
        .max_frame_size(2 * 1024 * 1024)
        .add_service(WorkerNodeServer::new(WorkerService::new(pool)))
        .serve(addr)
        .await?;

    Ok(())
}

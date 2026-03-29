use std::time::Duration;

use anyhow::Result;
use tokio::{spawn, sync::mpsc, time::sleep};
use tonic::transport::Server;
use tracing::{error, info};

use crate::{
    config::{Config, load_config},
    generated::master_node::master_node_server::MasterNodeServer,
    node_registry::registry_worker::registry_worker,
    service::MasterNodeService,
};

pub mod config;
pub mod generated;
pub mod node_registry;
pub mod service;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    info!("Starting server");
    let Config { addr, .. } = load_config()?;

    let (tx, rx) = mpsc::channel(32).into();

    let tx_clone = tx.clone();
    spawn(async move {
        let _ = registry_worker(tx_clone, rx).await;
        error!("Oh no!");
    });

    sleep(Duration::from_secs(5)).await;

    let addr = format!("{addr}").parse().unwrap();
    let _ = Server::builder()
        .add_service(MasterNodeServer::new(MasterNodeService::new(tx, 16)))
        .serve(addr)
        .await;

    error!("Shouldnt be here");

    Ok(())
}

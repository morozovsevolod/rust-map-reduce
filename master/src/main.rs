mod cleanup;
mod heartbeat_monitor;
mod routes;
mod scheduler;
mod state;
mod storage;

use state::SharedState;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let data_dir = std::path::PathBuf::from("/tmp/mapreduce_data");
    std::fs::create_dir_all(&data_dir).ok();

    let state = SharedState::new(data_dir.clone());
    let http_app = routes::create_router(state.clone());
    let grpc_service = routes::create_grpc_server(state.clone());
    let master_control = routes::create_master_control(state.clone());

    let http_listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("failed to bind HTTP");

    let grpc_listener = tokio::net::TcpListener::bind("0.0.0.0:50051")
        .await
        .expect("failed to bind gRPC");

    tracing::info!(
        "master HTTP on 0.0.0.0:3000, gRPC on 0.0.0.0:50051, data_dir={:?}",
        data_dir
    );

    tokio::spawn(heartbeat_monitor::heartbeat_monitor(state.clone()));

    if let Err(e) = tokio::try_join!(
        async {
            axum::serve(http_listener, http_app)
                .await
                .map_err(|e| e.to_string())
        },
        async {
            tonic::transport::Server::builder()
                .add_service(grpc_service)
                .add_service(master_control)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    grpc_listener,
                ))
                .await
                .map_err(|e| e.to_string())
        }
    ) {
        tracing::error!("server stopped: {}", e);
    }
}

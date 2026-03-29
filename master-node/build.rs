fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("./src/generated")
        .compile_protos(&[format!("./proto/worker.proto")], &[])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("./src/generated")
        .compile_protos(&[format!("./proto/master.proto")], &[])?;

    Ok(())
}

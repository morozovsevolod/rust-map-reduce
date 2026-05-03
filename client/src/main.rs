use std::io::Write;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mapreduce-client")]
#[command(about = "Submit and monitor MapReduce jobs")]
struct Cli {
    #[arg(short, long, default_value = "http://127.0.0.1:3000")]
    master: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Submit a new MapReduce job
    Submit {
        /// Path to compiled WASM module
        #[arg(short, long)]
        wasm: PathBuf,

        /// Path to input file (mutually exclusive with --size-mb)
        #[arg(short, long)]
        input: Option<PathBuf>,

        /// Generate random input of this many megabytes (mutually exclusive with --input)
        #[arg(short, long)]
        size_mb: Option<u32>,

        /// Number of reduce tasks
        #[arg(short, long, default_value_t = 4)]
        num_reduces: usize,
    },
    /// Poll job status
    Status {
        /// Job ID
        #[arg(short, long)]
        job_id: String,
    },
    /// Download job result
    Result {
        /// Job ID
        #[arg(short, long)]
        job_id: String,

        /// Output file path (default: print to stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Submit {
            wasm,
            input,
            size_mb,
            num_reduces,
        } => {
            if input.is_none() && size_mb.is_none() {
                eprintln!("error: must provide either --input or --size-mb");
                std::process::exit(1);
            }
            if input.is_some() && size_mb.is_some() {
                eprintln!("error: cannot use both --input and --size-mb");
                std::process::exit(1);
            }

            let wasm_data = std::fs::read(&wasm)?;
            let mut form = reqwest::multipart::Form::new().part(
                "wasm",
                reqwest::multipart::Part::bytes(wasm_data).file_name("task.wasm"),
            );

            if let Some(input_path) = input {
                let data = std::fs::read(&input_path)?;
                form = form.part(
                    "input",
                    reqwest::multipart::Part::bytes(data).file_name(
                        input_path
                            .file_name()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .into_owned(),
                    ),
                );
            } else if let Some(mb) = size_mb {
                form = form.part(
                    "generate_mb",
                    reqwest::multipart::Part::text(mb.to_string()),
                );
            }

            form = form.part(
                "num_reduces",
                reqwest::multipart::Part::text(num_reduces.to_string()),
            );

            let resp = reqwest::Client::new()
                .post(format!("{}/api/v1/jobs", cli.master))
                .multipart(form)
                .send()
                .await?;

            let body: serde_json::Value = resp.json().await?;
            let job_id = body["job_id"].as_str().unwrap_or("");
            println!("job_id: {}", job_id);
        }
        Commands::Status { job_id } => {
            let resp = reqwest::Client::new()
                .get(format!("{}/api/v1/jobs/{}/status", cli.master, job_id))
                .send()
                .await?;

            match resp.status() {
                reqwest::StatusCode::OK => {
                    let body: serde_json::Value = resp.json().await?;
                    println!("status: {}", body["status"].as_str().unwrap_or("unknown"));
                    println!(
                        "map_progress: {:.0}%",
                        body["map_progress"].as_f64().unwrap_or(0.0) * 100.0
                    );
                    println!(
                        "reduce_progress: {:.0}%",
                        body["reduce_progress"].as_f64().unwrap_or(0.0) * 100.0
                    );
                }
                reqwest::StatusCode::NOT_FOUND => {
                    eprintln!("job not found: {}", job_id);
                    std::process::exit(1);
                }
                _ => {
                    eprintln!("error: HTTP {}", resp.status());
                    std::process::exit(1);
                }
            }
        }
        Commands::Result { job_id, output } => {
            let resp = reqwest::Client::new()
                .get(format!("{}/api/v1/jobs/{}/result", cli.master, job_id))
                .send()
                .await?;

            match resp.status() {
                reqwest::StatusCode::OK => {
                    let bytes = resp.bytes().await?;
                    if let Some(out_path) = output {
                        std::fs::write(&out_path, &bytes)?;
                        println!("result written to {}", out_path.display());
                    } else {
                        std::io::stdout().write_all(&bytes)?;
                    }
                }
                reqwest::StatusCode::NOT_FOUND => {
                    eprintln!("job not found: {}", job_id);
                    std::process::exit(1);
                }
                reqwest::StatusCode::IM_A_TEAPOT => {
                    eprintln!("job not yet complete: {}", job_id);
                    std::process::exit(1);
                }
                _ => {
                    eprintln!("error: HTTP {}", resp.status());
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use tracing::error;

use md5rs_client::{log, process, Config, ExportFormat};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    folder: String,
    #[arg(short, long, default_value = "https://md5rs.hinature.cn")]
    url: String,
    #[arg(short, long)]
    token: String,
    #[arg(long, default_value = "3")]
    max_frames: Option<usize>,
    #[arg(long, short, default_value_t = true)]
    iframe_only: bool,
    #[arg(long, default_value_t = 0.45)]
    iou: f32,
    #[arg(long, default_value_t = 0.2)]
    conf: f32,
    #[arg(long, default_value_t = 70f32)]
    quality: f32,
    #[arg(short, long, value_enum, default_value_t = CliExportFormat::Json)]
    export: CliExportFormat,
    #[arg(long, default_value = "info")]
    log_level: String,
    #[arg(long, default_value = "md5rs.log")]
    log_file: String,
    #[arg(long, default_value_t = 100)]
    checkpoint: usize,
    #[arg(long)]
    resume_from: Option<String>,
    #[arg(long)]
    buffer_path: Option<String>,
    #[arg(long, default_value_t = 20)]
    buffer_size: usize,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
enum CliExportFormat {
    Json,
    Csv,
}

impl From<CliExportFormat> for ExportFormat {
    fn from(f: CliExportFormat) -> Self {
        match f {
            CliExportFormat::Json => ExportFormat::Json,
            CliExportFormat::Csv => ExportFormat::Csv,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let (progress_sender, progress_receiver) = crossbeam_channel::unbounded();

    let guard = log::init_logger(args.log_level, args.log_file).expect("Failed to init logger");

    let config = Config {
        folder: args.folder,
        url: args.url,
        token: args.token,
        max_frames: args.max_frames,
        iframe_only: args.iframe_only,
        iou: args.iou,
        conf: args.conf,
        quality: args.quality,
        export: args.export.into(),
        checkpoint: args.checkpoint,
        resume_from: args.resume_from,
        buffer_path: args.buffer_path,
        buffer_size: args.buffer_size,
    };

    let total_files =
        md5rs_client::utils::index_files_and_folders(&PathBuf::from(&config.folder)).len();
    let pb = ProgressBar::new(total_files as u64);
    pb.set_style(ProgressStyle::default_bar().template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
    )?);

    let progress_thread = std::thread::spawn(move || {
        for _ in progress_receiver.iter() {
            pb.inc(1);
        }
        pb.finish();
    });

    match process(config, progress_sender).await {
        Ok(_) => {}
        Err(e) => {
            error!("Error: {:?}", e);
        }
    }

    progress_thread.join().unwrap();

    drop(guard);

    Ok(())
}

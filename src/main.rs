use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, unbounded};
use indicatif::{ParallelProgressIterator, ProgressBar, ProgressStyle};
use rayon::prelude::*;
use tonic::{
    transport::{Certificate, Channel, ClientTlsConfig},
    Request,
};
use tracing::{error, info};
use url::Url;
use uuid::Uuid;

use md5rs::md5rs_client::Md5rsClient;
use md5rs::{AuthRequest, DetectRequest};

pub mod md5rs {
    tonic::include_proto!("md5rs");
}

mod export;
mod io;
mod log;
mod media;
mod utils;

use export::{export, export_worker, parse_export_csv, Bbox, ExportFrame};
use log::init_logger;
use media::{media_worker, WebpItem};
use utils::{index_files_and_folders, FileItem};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// folder to process
    #[arg(short, long)]
    folder: String,

    /// grpc url
    #[arg(short, long, default_value = "https://md5rs.hinature.cn")]
    url: String,

    /// certificate path
    #[arg(long, default_value = "certs/cert.pem")]
    cert: String,

    /// Access token
    #[arg(short, long)]
    token: String,

    #[arg(long, default_value = "3")]
    max_frames: Option<usize>,

    #[arg(long, short, default_value_t = true)]
    iframe_only: bool,

    /// NMS IoU threshold
    #[arg(long, default_value_t = 0.45)]
    iou: f32,

    /// NMS confidence threshold
    #[arg(long, default_value_t = 0.2)]
    conf: f32,

    /// Webp encode quality
    #[arg(long, default_value_t = 70f32)]
    quality: f32,

    /// export format
    #[arg(short, long, value_enum, default_value_t = ExportFormat::Json)]
    export: ExportFormat,

    /// log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// log file
    #[arg(long, default_value = "md5rs.log")]
    log_file: String,

    /// checkpoint interval.
    /// Will export data to disk every N frames(not files!).
    /// Set it too low could affect performance
    #[arg(long, default_value_t = 100)]
    checkpoint: usize,

    /// resume from checkpoint file(the same path as export file unless you renamed it)
    #[arg(long)]
    resume_from: Option<String>,

    /// SSD buffer path. Could help if speed is IO bound when data stored in HDD(make sure it is a SSD path!)
    #[arg(long)]
    buffer_path: Option<String>,

    /// buffer size. Max files to keep in buffer, adjust on SSD free space
    #[arg(long, default_value_t = 20)]
    buffer_size: usize,
}

/// Enum for export formats
#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "kebab-case")]
enum ExportFormat {
    /// JSON format
    Json,

    /// CSV format
    Csv,
}

#[tokio::main]
async fn run(args: Args) -> Result<()> {
    let url = Url::parse(&args.url)?;

    let host = url.host_str().unwrap();

    let cert_path = Path::new(&args.cert);

    let pem = std::fs::read_to_string(cert_path)?;
    let ca = Certificate::from_pem(pem);

    let tls = ClientTlsConfig::new().ca_certificate(ca).domain_name(host);

    // Create a channel to the server
    let channel = Channel::from_shared(url.to_string())?
        .tls_config(tls)?
        .connect()
        .await?;

    // Create a client
    let mut client = Md5rsClient::new(channel);

    let session_token = auth(&mut client, &args.token).await?;

    let buffer_path = args.buffer_path.clone();

    info!("Cleaning up buffer");
    match cleanup_buffer(&buffer_path) {
        Ok(_) => {}
        Err(e) => {
            error!("Error cleaning up buffer: {:?}", e);
        }
    }

    if args.checkpoint == 0 {
        error!("Checkpoint should be greater than 0");
        return Ok(());
    }

    if args.checkpoint == 0 {
        error!("Checkpoint should be greater than 0");
        return Ok(());
    }

    let folder_path = std::path::PathBuf::from(&args.folder);

    let folder_path = std::fs::canonicalize(folder_path)?;

    let imgsz = 1280;

    let max_frames = args.max_frames;

    let start = Instant::now();

    let mut file_paths = index_files_and_folders(&folder_path);

    let export_data = Arc::new(Mutex::new(Vec::new()));

    let frames = Arc::new(Mutex::new(HashMap::<String, ExportFrame>::new()));

    let file_paths = match args.resume_from {
        Some(checkpoint_path) => {
            let all_files =
                resume_from_checkpoint(&checkpoint_path, &mut file_paths, &export_data)?;
            all_files.to_owned()
        }
        None => file_paths,
    };

    // let mut export_handles = vec![];

    let (media_q_s, media_q_r) = bounded(8);

    let pb = ProgressBar::new(file_paths.len() as u64);

    pb.set_style(ProgressStyle::default_bar().template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
    )?);

    let (io_q_s, io_q_r) = bounded(args.buffer_size);

    let (export_q_s, export_q_r) = unbounded();

    let checkpoint_counter = Arc::new(Mutex::new(0 as usize));

    let buffer_path = args.buffer_path.clone();

    let folder_path_clone = folder_path.clone();

    let export_data_clone = Arc::clone(&export_data);

    let finish = Arc::new(Mutex::new(false));

    let finish_clone = Arc::clone(&finish);

    thread::spawn(move || {
        let export_data = Arc::clone(&export_data);
        let folder_path = folder_path.clone();
        let checkpoint_counter = Arc::clone(&checkpoint_counter);
        let export_handle = std::thread::spawn(move || {
            export_worker(
                args.checkpoint,
                &checkpoint_counter,
                &args.export,
                &folder_path,
                export_q_r,
                &export_data,
            );
        });
        export_handle.join().unwrap();
        let mut finish_lock = finish.lock().unwrap();
        *finish_lock = true;
    });

    match buffer_path {
        Some(buffer_path) => {
            rayon::spawn(move || {
                let buffer_path = std::path::PathBuf::from(buffer_path);
                std::fs::create_dir_all(&buffer_path).unwrap();
                let buffer_path = std::fs::canonicalize(buffer_path).unwrap();

                let io_handle = std::thread::spawn(move || {
                    for file in file_paths.iter() {
                        io::io_worker(&buffer_path, file, io_q_s.clone()).unwrap();
                    }
                    drop(io_q_s);
                });

                io_q_r
                    .iter()
                    .par_bridge()
                    .progress_with(pb.clone())
                    .for_each(|file| {
                        let media_q_s = media_q_s.clone();
                        media_worker(
                            file,
                            imgsz,
                            args.quality,
                            args.iframe_only,
                            max_frames,
                            media_q_s,
                        );
                    });
                io_handle.join().unwrap();
            });
        }
        None => {
            rayon::spawn(move || {
                file_paths
                    .par_iter()
                    .progress_with(pb.clone())
                    .for_each(|file| {
                        let media_q_s = media_q_s.clone();
                        media_worker(
                            file.clone(),
                            imgsz,
                            args.quality,
                            args.iframe_only,
                            max_frames,
                            media_q_s,
                        );
                    });
                pb.finish();
                drop(media_q_s);
            });
        }
    }

    let frames_clone = Arc::clone(&frames);
    let export_q_s_clone = export_q_s.clone();
    let outbound = async_stream::stream! {
        while let Ok(item) = media_q_r.recv() {
            match item {
                WebpItem::Frame(frame) => {
                    let webp = frame.webp;
                    let uuid = Uuid::new_v4().to_string();
                    {
                        let mut frames = frames_clone.lock().unwrap();
                        let shoot_time: Option<String> = match frame.shoot_time {
                            Some(shoot_time) => Some(shoot_time.to_string()),
                            None => None,
                        };
                        let export_frame = ExportFrame {
                            file: frame.file.clone(),
                            frame_index: frame.iframe_index,
                            shoot_time,
                            total_frames: frame.total_frames,
                            is_iframe: args.iframe_only,
                            bboxes: None,
                            label: None,
                            error: None,
                        };
                        frames.insert(uuid.clone(), export_frame);
                    }
                    yield DetectRequest { uuid, image: webp, width: frame.width as i32, height: frame.height as i32, iou: args.iou, score: args.conf };
                }
                WebpItem::ErrFile(file) => {
                    export_q_s_clone.send(ExportFrame {
                        file: file.file.clone(),
                        frame_index: 0,
                        shoot_time: None,
                        total_frames: 0,
                        is_iframe: false,
                        bboxes: None,
                        label: None,
                        error: Some(file.error.to_string()),
                    }).unwrap();
                }
            }
        }
    };

    let mut request = Request::new(outbound);
    request
        .metadata_mut()
        .insert("authorization", session_token.parse().unwrap());

    let response = client.detect(request).await;

    let mut inbound;

    match response {
        Ok(response) => {
            inbound = response.into_inner();
        }
        Err(status) => {
            error!("{}", status.message());
            cleanup_buffer(&args.buffer_path)?;
            info!("Exiting in 5 seconds");
            thread::sleep(Duration::from_secs(5));
            std::process::exit(0);
        }
    };

    loop {
        let result = inbound.message().await;

        match result {
            Ok(Some(response)) => {
                let uuid = response.uuid.clone();
                let bboxes = response.bboxs;
                let frames = Arc::clone(&frames);
                let export_q_s = export_q_s.clone();
                {
                    let mut frames = frames.lock().unwrap();
                    let mut frame = frames.remove(&uuid).unwrap();
                    let bboxes = bboxes
                        .iter()
                        .map(|bbox| Bbox {
                            x1: bbox.x1,
                            y1: bbox.y1,
                            x2: bbox.x2,
                            y2: bbox.y2,
                            class: bbox.class.clone() as usize,
                            score: bbox.score,
                        })
                        .collect();
                    frame.bboxes = Some(bboxes);
                    frame.label = Some(response.label);
                    export_q_s.send(frame).unwrap();
                }
            }
            Ok(None) => {
                drop(export_q_s);
                while !finish_clone.lock().unwrap().clone() {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
                export(&folder_path_clone, export_data_clone, &args.export)?;
                cleanup_buffer(&args.buffer_path)?;
                break;
            }
            Err(status) => {
                if status.code() == tonic::Code::Unauthenticated {
                    error!("Unauthenticated");
                } else if status.code() == tonic::Code::ResourceExhausted {
                    error!("Resource exhausted");
                } else {
                    error!("{}", status.message());
                }
                drop(export_q_s);
                while !finish_clone.lock().unwrap().clone() {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
                export(&folder_path_clone, export_data_clone, &args.export)?;
                cleanup_buffer(&args.buffer_path)?;
                break;
            }
        }
    }

    let elapsed = start.elapsed();
    info!("Elapsed time: {:?}", elapsed);

    Ok(())
}

fn main() -> Result<()> {
    let args: Args = Args::parse();

    let guard = init_logger(args.log_level.clone(), args.log_file.clone())
        .expect("Failed to initialize logger");

    run(args)?;

    drop(guard);

    Ok(())
}

async fn auth(client: &mut Md5rsClient<Channel>, token: &str) -> Result<String> {
    let response = client
        .auth(Request::new(AuthRequest {
            token: token.to_string(),
        }))
        .await?;

    let auth_response = response.into_inner();

    if auth_response.success {
        Ok(auth_response.token)
    } else {
        Err(anyhow::anyhow!("Auth failed"))
    }
}

fn cleanup_buffer(buffer_path: &Option<String>) -> Result<()> {
    if let Some(buff_path) = buffer_path {
        let buff_path = std::path::PathBuf::from(buff_path);
        if buff_path.exists() {
            std::fs::remove_dir_all(&buff_path)?;
        }
    }
    Ok(())
}

fn resume_from_checkpoint<'a>(
    checkpoint_path: &str,
    all_files: &'a mut HashSet<FileItem>,
    export_data: &Arc<Mutex<Vec<ExportFrame>>>,
) -> Result<&'a mut HashSet<FileItem>> {
    let checkpoint = Path::new(checkpoint_path);
    if !checkpoint.exists() {
        error!("Checkpoint file does not exist");
        return Err(anyhow::anyhow!("Checkpoint file does not exist"));
    }
    if !checkpoint.is_file() {
        error!("Checkpoint path is not a file");
        return Err(anyhow::anyhow!("Checkpoint path is not a file"));
    }
    match checkpoint.extension() {
        Some(ext) => {
            let ext = ext.to_str().unwrap();
            if ext != "json" && ext != "csv" {
                error!("Invalid checkpoint file extension: {}", ext);
                return Err(anyhow::anyhow!(
                    "Invalid checkpoint file extension: {}",
                    ext
                ));
            } else {
                let frames;
                if ext == "json" {
                    let json = std::fs::read_to_string(checkpoint)?;
                    frames = serde_json::from_str(&json)?;
                } else {
                    frames = parse_export_csv(checkpoint)?;
                }
                let mut file_frame_count = HashMap::new();
                let mut file_total_frames = HashMap::new();
                for f in &frames {
                    let file = &f.file;
                    let count = file_frame_count.entry(file.clone()).or_insert(0);
                    *count += 1;
                    file_total_frames
                        .entry(file.clone())
                        .or_insert(f.total_frames);

                    if let Some(total_frames) = file_total_frames.get(&file) {
                        if let Some(frame_count) = file_frame_count.get(&file) {
                            if total_frames == frame_count {
                                all_files.remove(&file);
                            }
                        }
                    }
                }
                export_data.lock().unwrap().extend_from_slice(&frames);
                Ok(all_files)
            }
        }
        None => {
            error!("Invalid checkpoint file extension");
            return Err(anyhow::anyhow!("Invalid checkpoint file extension"));
        }
    }
}

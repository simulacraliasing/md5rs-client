use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossbeam_channel::{bounded, unbounded};
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

pub mod export;
pub mod io;
pub mod log;
pub mod media;
pub mod utils;

pub use export::{export_worker, parse_export_csv, Bbox, ExportFrame};
pub use media::{media_worker, WebpItem};
pub use utils::FileItem;

#[derive(Debug, Clone)]
pub struct Config {
    pub folder: String,
    pub url: String,
    pub token: String,
    pub max_frames: Option<usize>,
    pub iframe_only: bool,
    pub iou: f32,
    pub conf: f32,
    pub quality: f32,
    pub export: ExportFormat,
    pub checkpoint: usize,
    pub resume_from: Option<String>,
    pub buffer_path: Option<String>,
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    Json,
    Csv,
}

pub async fn process(config: Config, progress_sender: crossbeam_channel::Sender<usize>) -> Result<()> {
    let url = Url::parse(&config.url)?;
    let host = url.host_str().unwrap();

    let pem = utils::get_tls_certificate(&config.url)?;
    let ca = Certificate::from_pem(pem);
    let tls = ClientTlsConfig::new().ca_certificate(ca).domain_name(host);

    let channel = Channel::from_shared(url.to_string())?
        .tls_config(tls)?
        .connect()
        .await?;

    let mut client = Md5rsClient::new(channel);
    let session_token = auth(&mut client, &config.token).await?;

    cleanup_buffer(&config.buffer_path)?;

    if config.checkpoint == 0 {
        error!("Checkpoint should be greater than 0");
        return Ok(());
    }

    let folder_path = std::path::PathBuf::from(&config.folder);
    let folder_path = std::fs::canonicalize(folder_path)?;

    let imgsz = 1280;
    let start = Instant::now();

    let mut file_paths = utils::index_files_and_folders(&folder_path);

    let export_data = Arc::new(Mutex::new(Vec::new()));
    let frames = Arc::new(Mutex::new(HashMap::<String, ExportFrame>::new()));

    let file_paths = match config.resume_from {
        Some(checkpoint_path) => {
            let all_files =
                resume_from_checkpoint(&checkpoint_path, &mut file_paths, &export_data)?;
            all_files.to_owned()
        }
        None => file_paths,
    };

    let (media_q_s, media_q_r) = bounded(8);
    let (io_q_s, io_q_r) = bounded(config.buffer_size);
    let (export_q_s, export_q_r) = unbounded();
    let checkpoint_counter = Arc::new(Mutex::new(0 as usize));

    let buffer_path = config.buffer_path.clone();
    let folder_path_clone = folder_path.clone();
    let export_data_clone = Arc::clone(&export_data);
    let finish = Arc::new(Mutex::new(false));
    let finish_clone = Arc::clone(&finish);

    thread::spawn(move || {
        let export_data = Arc::clone(&export_data);
        let folder_path = folder_path.clone();
        let checkpoint_counter = Arc::clone(&checkpoint_counter);
        export_worker(
            config.checkpoint,
            &checkpoint_counter,
            &config.export,
            &folder_path,
            export_q_r,
            &export_data,
        );
        let mut finish_lock = finish.lock().unwrap();
        *finish_lock = true;
    });

    if let Some(buffer_path) = buffer_path {
        rayon::spawn(move || {
            std::fs::create_dir_all(&buffer_path).unwrap();
            let buffer_path = std::fs::canonicalize(buffer_path).unwrap();

            let io_handle = thread::spawn(move || {
                for file in file_paths.iter() {
                    io::io_worker(&buffer_path, file, io_q_s.clone()).unwrap();
                }
                drop(io_q_s);
            });

            io_q_r.iter().par_bridge().for_each(|file| {
                media_worker(
                    file,
                    imgsz,
                    config.quality,
                    config.iframe_only,
                    config.max_frames,
                    media_q_s.clone(),
                    progress_sender.clone(),
                );
            });
            io_handle.join().unwrap();
        });
    } else {
        rayon::spawn(move || {
            file_paths.par_iter().for_each(|file| {
                media_worker(
                    file.clone(),
                    imgsz,
                    config.quality,
                    config.iframe_only,
                    config.max_frames,
                    media_q_s.clone(),
                    progress_sender.clone(),
                );
            });
            drop(media_q_s);
        });
    }

    let frames_clone = Arc::clone(&frames);
    let export_q_s_clone = export_q_s.clone();
    let outbound = async_stream::stream! {
        while let Ok(item) = media_q_r.recv() {
            match item {
                WebpItem::Frame(frame) => {
                    let uuid = Uuid::new_v4().to_string();
                    let export_frame = ExportFrame {
                        file: frame.file.clone(),
                        frame_index: frame.frame_index,
                        shoot_time: frame.shoot_time.map(|t| t.to_string()),
                        total_frames: frame.total_frames,
                        bboxes: None,
                        label: None,
                        error: None,
                    };
                    frames_clone.lock().unwrap().insert(uuid.clone(), export_frame);
                    yield DetectRequest { uuid, image: frame.webp, width: frame.width as i32, height: frame.height as i32, iou: config.iou, score: config.conf };
                }
                WebpItem::ErrFile(file) => {
                    export_q_s_clone.send(ExportFrame {
                        file: file.file.clone(),
                        frame_index: 0,
                        shoot_time: None,
                        total_frames: 0,
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
    let mut inbound = match response {
        Ok(response) => response.into_inner(),
        Err(status) => {
            error!("{}", status.message());
            cleanup_buffer(&config.buffer_path)?;
            return Ok(());
        }
    };

    loop {
        match inbound.message().await {
            Ok(Some(response)) => {
                let uuid = response.uuid.clone();
                let mut frames = frames.lock().unwrap();
                if let Some(mut frame) = frames.remove(&uuid) {
                    frame.bboxes = Some(
                        response
                            .bboxs
                            .into_iter()
                            .map(|bbox| Bbox {
                                x1: bbox.x1,
                                y1: bbox.y1,
                                x2: bbox.x2,
                                y2: bbox.y2,
                                class: bbox.class as usize,
                                score: bbox.score,
                            })
                            .collect(),
                    );
                    frame.label = Some(response.label);
                    export_q_s.send(frame).unwrap();
                }
            }
            Ok(None) => {
                drop(export_q_s);
                while !*finish_clone.lock().unwrap() {
                    thread::sleep(Duration::from_millis(100));
                }
                export::export(&folder_path_clone, export_data_clone, &config.export)?;
                cleanup_buffer(&config.buffer_path)?;
                break;
            }
            Err(e) => {
                error!("Error receiving detection: {}", e);
                drop(export_q_s);
                while !*finish_clone.lock().unwrap() {
                    thread::sleep(Duration::from_millis(100));
                }
                export::export(&folder_path_clone, export_data_clone, &config.export)?;
                cleanup_buffer(&config.buffer_path)?;
                break;
            }
        }
    }

    info!("Elapsed time: {:?}", start.elapsed());
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
    if let Some(path) = buffer_path {
        let path = std::path::PathBuf::from(path);
        if path.exists() {
            std::fs::remove_dir_all(path)?;
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

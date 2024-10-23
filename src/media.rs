use std::fs::{metadata, File};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use crossbeam_channel::Sender;
use fast_image_resize::{ResizeAlg, ResizeOptions, Resizer};
use ffmpeg_sidecar::child::FfmpegChild;
use ffmpeg_sidecar::command::FfmpegCommand;
use ffmpeg_sidecar::event::{FfmpegEvent, LogLevel, OutputVideoFrame};
use image::{DynamicImage, GenericImageView, ImageReader};
use jpeg_decoder::Decoder;
use nom_exif::{Exif, ExifIter, ExifTag, MediaParser, MediaSource};
use thiserror::Error;
use tracing::{debug, error, warn};
use webp::Encoder;

use crate::utils::{sample_evenly, FileItem};

//define meadia error
#[derive(Error, Debug)]
pub enum MediaError {
    #[error("Failed to open file: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Failed to decode: {0}")]
    ImageDecodeError(#[from] jpeg_decoder::Error),

    #[error("Failed to decode: {0}")]
    VideoDecodeError(String),

    #[error("Failed to encode: {0}")]
    WebpEncodeError(String),
}

pub struct Frame {
    pub file: FileItem,
    pub webp: Vec<u8>,
    pub width: usize,
    pub height: usize,
    pub iframe_index: usize,
    pub total_frames: usize,
    pub shoot_time: Option<DateTime<Local>>,
}

pub struct ErrFile {
    pub file: FileItem,
    pub error: anyhow::Error,
}

pub enum WebpItem {
    Frame(Frame),
    ErrFile(ErrFile),
}

pub fn media_worker(
    file: FileItem,
    imgsz: usize,
    quality: f32,
    iframe: bool,
    max_frames: Option<usize>,
    array_q_s: Sender<WebpItem>,
) {
    let mut parser = MediaParser::new();
    let mut resizer = Resizer::new();
    if let Some(extension) = file.file_path.extension() {
        let array_q_s = array_q_s.clone();
        match extension.to_str().unwrap().to_lowercase().as_str() {
            "jpg" | "jpeg" | "png" => {
                process_image(&file, imgsz, quality, &mut parser, &mut resizer, array_q_s).unwrap();
            }
            "mp4" | "avi" | "mkv" | "mov" => {
                process_video(&file, imgsz, quality, iframe, max_frames, array_q_s).unwrap();
            }
            _ => (),
        }
        if &file.file_path != &file.tmp_path {
            remove_file_with_retries(&file.tmp_path, 3, Duration::from_secs(1))
                .expect("Failed to remove file");
        }
    }
}

fn remove_file_with_retries(file_path: &PathBuf, max_retries: u32, delay: Duration) -> Result<()> {
    let mut attempts = 0;

    while attempts < max_retries {
        match std::fs::remove_file(file_path) {
            Ok(_) => {
                debug!("File removed successfully.");
                return Ok(());
            }
            Err(e) => {
                error!(
                    "Failed to remove file: {}. Attempt {} of {}",
                    e,
                    attempts + 1,
                    max_retries
                );
                attempts += 1;

                if attempts < max_retries {
                    thread::sleep(delay);
                }
            }
        }
    }

    Ok(())
}

fn decode_image(file: &FileItem) -> Result<DynamicImage> {
    let img = match ImageReader::open(file.tmp_path.as_path())
        .map_err(MediaError::IoError)?
        .decode()
    {
        Ok(img) => DynamicImage::ImageRgb8(img.to_rgb8()),
        Err(_e) => {
            warn!(
                "Failed to decode image with ImageReader. Trying jpeg_decoder. {:?}",
                _e
            );
            let img_reader = File::open(file.tmp_path.as_path()).map_err(MediaError::IoError)?;
            let mut decoder = Decoder::new(BufReader::new(img_reader));
            let pixels = decoder.decode().map_err(MediaError::ImageDecodeError)?;
            let img = DynamicImage::ImageRgb8(
                image::ImageBuffer::from_raw(
                    decoder.info().unwrap().width as u32,
                    decoder.info().unwrap().height as u32,
                    pixels,
                )
                .unwrap(),
            );
            img
        }
    };
    Ok(img)
}

pub fn process_image(
    file: &FileItem,
    imgsz: usize,
    quality: f32,
    parser: &mut MediaParser,
    resizer: &mut Resizer,
    array_q_s: Sender<WebpItem>,
) -> Result<()> {
    let frame_data = match decode_image(file) {
        Ok(img) => {
            let webp: Option<Vec<u8>> = match resize_encode(&img, imgsz as u32, quality, resizer) {
                Ok(webp) => Some(webp),
                Err(_e) => None,
            };
            let shoot_time: Option<DateTime<Local>> =
                match get_image_date(parser, file.tmp_path.as_path()) {
                    Ok(shoot_time) => Some(shoot_time),
                    Err(_e) => None,
                };
            if webp.is_none() {
                WebpItem::ErrFile(ErrFile {
                    file: file.clone(),
                    error: MediaError::WebpEncodeError("Failed to encode image".to_string()).into(),
                })
            } else {
                let webp = webp.unwrap();
                let frame_data = Frame {
                    webp,
                    file: file.clone(),
                    width: img.width() as usize,
                    height: img.height() as usize,
                    iframe_index: 0,
                    total_frames: 1,
                    shoot_time,
                };
                WebpItem::Frame(frame_data)
            }
        }
        Err(error) => WebpItem::ErrFile(ErrFile {
            file: file.clone(),
            error,
        }),
    };
    match array_q_s.send(frame_data) {
        Ok(_) => (),
        Err(_e) => error!("Failed to send frame data, channel disconnected"),
    }
    Ok(())
}

fn resize_encode(
    img: &DynamicImage,
    imgsz: u32,
    quality: f32,
    resizer: &mut Resizer,
) -> Result<Vec<u8>> {
    // Get the dimensions of the original image
    let (width, height) = img.dimensions();
    let mut resized_width = imgsz;
    let mut resized_height = imgsz;
    let ratio: f32;

    if width > height {
        ratio = width as f32 / imgsz as f32;
        resized_height = (height as f32 / ratio) as u32;
        resized_height = resized_height % 2 + resized_height;
    } else {
        ratio = height as f32 / imgsz as f32;
        resized_width = (width as f32 / ratio) as u32;
        resized_width = resized_width % 2 + resized_width;
    }

    let mut resized_img = DynamicImage::new(resized_width, resized_height, img.color());

    let resize_option = ResizeOptions::new().resize_alg(ResizeAlg::Nearest);

    resizer
        .resize(img, &mut resized_img, &resize_option)
        .unwrap();

    let encoder = Encoder::from_image(&resized_img);

    match encoder {
        Ok(encoder) => {
            let webp = encoder.encode(quality);
            let data = (&*webp).to_vec();
            Ok(data)
        }
        Err(e) => {
            error!("Failed to encode image: {:?}", e);
            Err(MediaError::WebpEncodeError(e.to_string()).into())
        }
    }
}

pub fn process_video(
    file: &FileItem,
    imgsz: usize,
    quality: f32,
    iframe: bool,
    max_frames: Option<usize>,
    array_q_s: Sender<WebpItem>,
) -> Result<()> {
    let video_path = file.tmp_path.to_string_lossy();
    let input = create_ffmpeg_command(&video_path, imgsz, iframe)?;

    handle_ffmpeg_output(input, array_q_s, file, quality, max_frames)?;

    Ok(())
}

fn create_ffmpeg_command(video_path: &str, imgsz: usize, iframe: bool) -> Result<FfmpegChild> {
    let mut ffmpeg_command = FfmpegCommand::new();
    if iframe {
        ffmpeg_command.args(["-skip_frame", "nokey"]);
    }
    let command = ffmpeg_command
        .input(video_path)
        .args(&[
            "-an",
            "-vf",
            &format!(
                "scale=w={}:h={}:force_original_aspect_ratio=decrease",
                imgsz, imgsz
            ),
            "-f",
            "rawvideo",
            "-pix_fmt",
            "rgb24",
            "-vsync",
            "vfr",
        ])
        .output("-")
        .spawn()?;
    Ok(command)
}

fn decode_video(
    mut input: FfmpegChild,
) -> Result<(Vec<OutputVideoFrame>, Option<usize>, Option<usize>)> {
    let mut width = None;
    let mut height = None;
    let mut frames = vec![];

    for e in input.iter()? {
        match e {
            FfmpegEvent::Log(LogLevel::Error, e) => {
                if e.contains("decode_slice_header error")
                    || e.contains("Frame num change")
                    || e.contains("error while decoding MB")
                {
                    continue;
                } else {
                    return Err(MediaError::VideoDecodeError(e).into());
                }
            }
            FfmpegEvent::ParsedInputStream(i) => {
                if i.stream_type.to_lowercase() == "video" {
                    width = Some(i.width as usize);
                    height = Some(i.height as usize);
                }
            }
            FfmpegEvent::OutputFrame(f) => {
                frames.push(f);
            }
            _ => {}
        }
    }

    Ok((frames, width, height))
}

fn handle_ffmpeg_output(
    input: FfmpegChild,
    s: Sender<WebpItem>,
    file: &FileItem,
    quality: f32,
    max_frames: Option<usize>,
) -> Result<()> {
    match decode_video(input) {
        Ok((frames, width, height)) => {
            let (sampled_frames, sampled_indexes) =
                sample_evenly(&frames, max_frames.unwrap_or(frames.len()));

            let shoot_time: Option<DateTime<Local>> = match get_video_date(&file.tmp_path.as_path())
            {
                Ok(shoot_time) => Some(shoot_time),
                Err(_e) => None,
            };

            //calculate ratio and padding
            let width = width.expect("Failed to get video width");
            let height = height.expect("Failed to get video height");

            let frames_length = sampled_frames.len();

            for (f, i) in sampled_frames.into_iter().zip(sampled_indexes.into_iter()) {
                let encoder = Encoder::from_rgb(&f.data, f.width, f.height);

                let webp = encoder.encode(quality);

                let webp = (&*webp).to_vec();

                let frame_data = WebpItem::Frame(Frame {
                    webp,
                    file: file.clone(),
                    width,
                    height,
                    iframe_index: i,
                    total_frames: frames_length,
                    shoot_time,
                });
                s.send(frame_data).expect("Send video frame failed");
            }
        }
        Err(error) => {
            let frame_data = WebpItem::ErrFile(ErrFile {
                file: file.clone(),
                error,
            });
            s.send(frame_data).expect("Send video frame failed");
        }
    }

    Ok(())
}

fn get_image_date(parser: &mut MediaParser, image: &Path) -> Result<DateTime<Local>> {
    let ms = MediaSource::file_path(image)?;

    let iter: ExifIter = parser.parse(ms)?;
    let exif: Exif = iter.into();
    let shoot_time = exif
        .get(ExifTag::DateTimeOriginal)
        .or_else(|| exif.get(ExifTag::ModifyDate))
        .context("Neither DateTimeOriginal nor ModifyDate found")?;
    let shoot_time = shoot_time.as_time().unwrap().with_timezone(&Local);

    Ok(shoot_time)
}

fn get_video_date(video: &Path) -> Result<DateTime<Local>> {
    let metadata = metadata(video)?;
    #[cfg(target_os = "windows")]
    {
        let m_time = metadata.modified()?;
        let shoot_time: DateTime<Local> = m_time.clone().into();

        Ok(shoot_time)
    }

    #[cfg(target_os = "linux")]
    #[allow(deprecated)]
    {
        use chrono::NaiveDateTime;
        use std::os::linux::fs::MetadataExt;
        let m_time: i64 = metadata.st_mtime();
        let c_time: i64 = metadata.st_ctime();
        let shoot_time = m_time.min(c_time);
        let offset = Local::now().offset().to_owned();
        let shoot_time = NaiveDateTime::from_timestamp(shoot_time, 0);
        let shoot_time = DateTime::<Local>::from_naive_utc_and_offset(shoot_time, offset);

        Ok(shoot_time)
    }

    #[cfg(target_os = "macos")]
    {
        use chrono::NaiveDateTime;
        use std::os::unix::fs::MetadataExt;
        let m_time: i64 = metadata.mtime();
        let c_time: i64 = metadata.ctime();
        let shoot_time = m_time.min(c_time);
        let offset = Local::now().offset().to_owned();
        let shoot_time = NaiveDateTime::from_timestamp(shoot_time, 0);
        let shoot_time = DateTime::<Local>::from_naive_utc_and_offset(shoot_time, offset);

        Ok(shoot_time)
    }
}

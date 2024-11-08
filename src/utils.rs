use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use walkdir::{DirEntry, WalkDir};

pub fn sample_evenly<T: Clone>(list: &[T], sample_size: usize) -> Vec<T> {
    let len = list.len();
    if sample_size == 0 || len == 0 {
        return Vec::new();
    }

    let step = len as f64 / sample_size as f64;
    let mut sampled_elements = Vec::with_capacity(sample_size);
    for i in 0..sample_size {
        let index = (i as f64 * step).floor() as usize;
        sampled_elements.push(list[index].clone());
    }
    sampled_elements
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub struct FileItem {
    pub folder_id: usize,
    pub file_id: usize,
    pub file_path: PathBuf,
    #[serde(skip_serializing)]
    pub tmp_path: PathBuf,
}

impl Eq for FileItem {}

impl FileItem {
    pub fn new(
        folder_id: usize,
        file_id: usize,
        file_path: PathBuf,
        tmp_path: Option<PathBuf>,
    ) -> Self {
        match tmp_path {
            Some(tmp_path) => Self {
                folder_id,
                file_id,
                file_path,
                tmp_path: tmp_path,
            },
            None => Self {
                folder_id,
                file_id,
                file_path: file_path.clone(),
                tmp_path: file_path,
            },
        }
    }
}

fn is_skip(entry: &DirEntry) -> bool {
    let skip_dirs = ["Animal", "Person", "Vehicle", "Blank"];
    entry
        .file_name()
        .to_str()
        .map(|s| {
            skip_dirs.contains(&s) || s.starts_with('.') || s == "result.csv" || s == "result.json"
        })
        .unwrap_or(false)
}

pub fn index_files_and_folders(folder_path: &PathBuf) -> HashSet<FileItem> {
    let mut folder_id: usize = 0;
    let mut file_id: usize = 0;
    let mut file_paths = HashSet::new();

    for entry in WalkDir::new(folder_path)
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|e| !is_skip(e))
    {
        let entry = entry.unwrap();
        if entry.file_type().is_dir() {
            folder_id += 1;
        } else if entry.file_type().is_file() {
            if is_video_photo(entry.path()) {
                file_paths.insert(FileItem::new(
                    folder_id,
                    file_id,
                    entry.path().to_path_buf(),
                    None,
                ));
                file_id += 1;
            }
        }
    }

    file_paths
}

fn is_video_photo(path: &Path) -> bool {
    if let Some(extension) = path.extension() {
        match extension.to_str().unwrap().to_lowercase().as_str() {
            "mp4" | "avi" | "mkv" | "mov" => true,
            "jpg" | "jpeg" | "png" => true,
            _ => false,
        }
    } else {
        false
    }
}

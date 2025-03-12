use std::collections::HashSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::net::TcpStream;

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls_native_certs::load_native_certs;
use rustls_pki_types::{CertificateDer, ServerName};
use serde::{Deserialize, Serialize};
use tracing::error;
use url::Url;
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

pub fn get_tls_certificate(url_str: &str) -> Result<String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    // Parse the URL to extract domain
    let url = Url::parse(url_str)?;
    let domain_str = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("No host in URL"))?;
    let domain = domain_str.to_string();
    let port = url.port().unwrap_or(443);

    let server_name = ServerName::try_from(domain.clone())?;

    // Load root certificates from the system
    let mut root_store = RootCertStore::empty();
    let native_certs = load_native_certs();
    for cert in native_certs.certs {
        root_store.add(cert)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let mut client = rustls::ClientConnection::new(Arc::new(config), server_name)?;

    let mut stream = TcpStream::connect(format!("{}:{}", domain, port))?;
    let mut tls_stream = rustls::Stream::new(&mut client, &mut stream);

    // Write HTTP request
    tls_stream.write_all(
        format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            url.path(),
            domain
        )
        .as_bytes(),
    )?;
    let mut pem = String::new();
    // Get certificate information
    if let Some(certs) = tls_stream.conn.peer_certificates() {
        for cert in certs.iter() {
            let pem_content = cert_to_pem(cert)?;
            pem.push_str(&pem_content);
        }
    } else {
        error!("No certificate found for {}", domain);
    }

    Ok(pem)
}

fn cert_to_pem(cert: &CertificateDer<'_>) -> Result<String> {
    // Convert the certificate data to base64
    let b64_data = BASE64.encode(cert.as_ref());

    // Format with PEM headers and line wrapping (64 characters per line)
    let mut pem_content = String::from("-----BEGIN CERTIFICATE-----\n");

    // Add base64 data with line breaks every 64 characters
    for chunk in b64_data.as_bytes().chunks(64) {
        pem_content.push_str(&String::from_utf8_lossy(chunk));
        pem_content.push('\n');
    }

    pem_content.push_str("-----END CERTIFICATE-----\n");

    Ok(pem_content)
}

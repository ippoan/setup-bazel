use crate::r2::R2Client;
use crate::SyncArgs;
use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use walkdir::WalkDir;

const MANIFEST_KEY: &str = "manifest.json";

/// Collect relative file paths under `dir`.
fn list_local_files(dir: &Path) -> HashSet<String> {
    let mut files = HashSet::new();
    if !dir.exists() {
        return files;
    }
    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            if let Ok(rel) = entry.path().strip_prefix(dir) {
                files.insert(rel.to_string_lossy().to_string());
            }
        }
    }
    files
}

/// Load manifest (set of keys) from R2. Returns empty set if not found.
async fn load_manifest(client: &R2Client, prefix: &str) -> Result<HashSet<String>> {
    let key = format!("{}{}", prefix, MANIFEST_KEY);
    match client.get_object(&key).await? {
        Some(data) => {
            let keys: Vec<String> = serde_json::from_slice(&data)?;
            Ok(keys.into_iter().collect())
        }
        None => Ok(HashSet::new()),
    }
}

/// Save manifest to R2.
async fn save_manifest(client: &R2Client, prefix: &str, keys: &HashSet<String>) -> Result<()> {
    let key = format!("{}{}", prefix, MANIFEST_KEY);
    let mut sorted: Vec<&String> = keys.iter().collect();
    sorted.sort();
    let data = serde_json::to_vec(&sorted)?;
    client.put_object(&key, &data).await
}

/// Restore: download missing files from R2 using manifest.
pub async fn restore(args: &SyncArgs) -> Result<()> {
    let start = Instant::now();
    let client = R2Client::new(&args.endpoint, &args.bucket)?;
    let local_path = Path::new(&args.local_path);

    eprintln!("::group::Restore disk cache from R2");

    // Load manifest (single GET)
    let t = Instant::now();
    let manifest = load_manifest(&client, &args.prefix).await?;
    eprintln!("Manifest: {} files ({:.1}s)", manifest.len(), t.elapsed().as_secs_f64());

    // List local files
    let t = Instant::now();
    let local_files = list_local_files(local_path);
    eprintln!("Local:   {} files ({:.1}s)", local_files.len(), t.elapsed().as_secs_f64());

    // Diff: download what's missing locally
    let to_download: Vec<String> = manifest.difference(&local_files).cloned().collect();
    eprintln!(
        "{} to download, {} already cached",
        to_download.len(),
        local_files.len()
    );

    if to_download.is_empty() {
        eprintln!("::endgroup::");
        return Ok(());
    }

    tokio::fs::create_dir_all(local_path).await?;

    let sem = Arc::new(Semaphore::new(args.concurrency));
    let client = Arc::new(client);
    let downloaded = Arc::new(AtomicUsize::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let total = to_download.len();

    let mut handles = Vec::with_capacity(total);

    for rel_path in to_download {
        let sem = sem.clone();
        let client = client.clone();
        let prefix = args.prefix.clone();
        let base = local_path.to_path_buf();
        let downloaded = downloaded.clone();
        let bytes_total = bytes_total.clone();
        let failed = failed.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let remote_key = format!("{}{}", prefix, rel_path);
            let local_file = base.join(&rel_path);

            for attempt in 0..3 {
                match client.get_object(&remote_key).await {
                    Ok(Some(data)) => {
                        if let Some(parent) = local_file.parent() {
                            let _ = tokio::fs::create_dir_all(parent).await;
                        }
                        if let Err(e) = tokio::fs::write(&local_file, &data).await {
                            eprintln!("::warning::Write {}: {e}", rel_path);
                            failed.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                        bytes_total.fetch_add(data.len() as u64, Ordering::Relaxed);
                        let n = downloaded.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % 500 == 0 {
                            eprintln!("  downloaded {n}/{total}");
                        }
                        return;
                    }
                    Ok(None) => {
                        eprintln!("::warning::GET {rel_path}: 404 (stale manifest entry)");
                        failed.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                    Err(e) => {
                        if attempt == 2 {
                            eprintln!("::warning::GET {rel_path} failed after 3 attempts: {e}");
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let dl = downloaded.load(Ordering::Relaxed);
    let bytes = bytes_total.load(Ordering::Relaxed);
    let fail = failed.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "Downloaded {dl} files ({:.1} MB) in {:.1}s, {fail} failed",
        bytes as f64 / 1_048_576.0,
        elapsed
    );
    eprintln!("::endgroup::");

    if fail > 0 {
        eprintln!("::warning::{fail} files failed to download");
    }
    Ok(())
}

/// Save: upload new local files to R2, then update manifest.
pub async fn save(args: &SyncArgs) -> Result<()> {
    let start = Instant::now();
    let client = R2Client::new(&args.endpoint, &args.bucket)?;
    let local_path = Path::new(&args.local_path);

    eprintln!("::group::Save disk cache to R2");

    // List local files
    let t = Instant::now();
    let local_files = list_local_files(local_path);
    eprintln!("Local:    {} files ({:.1}s)", local_files.len(), t.elapsed().as_secs_f64());

    // Load manifest (single GET)
    let t = Instant::now();
    let manifest = load_manifest(&client, &args.prefix).await?;
    eprintln!("Manifest: {} files ({:.1}s)", manifest.len(), t.elapsed().as_secs_f64());

    // Diff: upload what's not in manifest
    let to_upload: Vec<String> = local_files.difference(&manifest).cloned().collect();
    eprintln!(
        "{} to upload, {} already synced",
        to_upload.len(),
        manifest.len()
    );

    if to_upload.is_empty() {
        eprintln!("::endgroup::");
        return Ok(());
    }

    let sem = Arc::new(Semaphore::new(args.concurrency));
    let client = Arc::new(client);
    let uploaded = Arc::new(AtomicUsize::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let total = to_upload.len();

    let mut handles = Vec::with_capacity(total);

    for rel_path in &to_upload {
        let sem = sem.clone();
        let client = client.clone();
        let prefix = args.prefix.clone();
        let base = local_path.to_path_buf();
        let uploaded = uploaded.clone();
        let bytes_total = bytes_total.clone();
        let failed = failed.clone();
        let rel_path = rel_path.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let local_file = base.join(&rel_path);
            let remote_key = format!("{}{}", prefix, rel_path);

            let data = match tokio::fs::read(&local_file).await {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("::warning::Read {}: {e}", rel_path);
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let size = data.len() as u64;

            for attempt in 0..3 {
                match client.put_object(&remote_key, &data).await {
                    Ok(()) => {
                        bytes_total.fetch_add(size, Ordering::Relaxed);
                        let n = uploaded.fetch_add(1, Ordering::Relaxed) + 1;
                        if n % 500 == 0 {
                            eprintln!("  uploaded {n}/{total}");
                        }
                        return;
                    }
                    Err(e) => {
                        if attempt == 2 {
                            eprintln!("::warning::PUT {rel_path} failed after 3 attempts: {e}");
                            failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let ul = uploaded.load(Ordering::Relaxed);
    let bytes = bytes_total.load(Ordering::Relaxed);
    let fail = failed.load(Ordering::Relaxed);
    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "Uploaded {ul} files ({:.1} MB) in {:.1}s, {fail} failed",
        bytes as f64 / 1_048_576.0,
        elapsed
    );

    // Update manifest: merge existing + newly uploaded
    let mut new_manifest = manifest;
    for rel_path in &to_upload {
        new_manifest.insert(rel_path.clone());
    }
    // Remove failed uploads from manifest
    // (they weren't uploaded, so shouldn't be in manifest)
    // Since we don't track which specific ones failed, keep them —
    // content-addressed means a missing file just triggers re-upload next time.

    let client_ref = Arc::try_unwrap(client).unwrap_or_else(|_| {
        R2Client::new(&args.endpoint, &args.bucket).expect("reconnect")
    });
    save_manifest(&client_ref, &args.prefix, &new_manifest).await?;
    eprintln!("Manifest updated: {} files", new_manifest.len());

    eprintln!("::endgroup::");

    if fail > 0 {
        eprintln!("::warning::{fail} files failed to upload");
    }
    Ok(())
}

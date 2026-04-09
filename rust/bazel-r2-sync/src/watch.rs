use crate::r2::R2Client;
use crate::WatchArgs;
use anyhow::Result;
use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use walkdir::WalkDir;

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

/// Watch mode: poll disk_cache for new files and upload them incrementally.
/// Runs until SIGTERM/SIGINT. On shutdown, does a final sweep and updates manifest.
pub async fn run(args: &WatchArgs) -> Result<()> {
    let client = Arc::new(R2Client::new(&args.endpoint, &args.bucket)?);
    let local_path = Path::new(&args.local_path);
    let interval = Duration::from_secs(args.interval);

    // Write PID file if requested
    if let Some(ref pid_path) = args.pid_file {
        tokio::fs::write(pid_path, std::process::id().to_string()).await?;
    }

    eprintln!("[watch] started, polling every {}s", args.interval);

    // Load manifest to know what's already uploaded
    let manifest_key = format!("{}manifest.json", args.prefix);
    let mut known: HashSet<String> = match client.get_object(&manifest_key).await? {
        Some(data) => {
            let keys: Vec<String> = serde_json::from_slice(&data)?;
            keys.into_iter().collect()
        }
        None => HashSet::new(),
    };

    // Also include files already on disk (they were just restored, no need to re-upload)
    let initial_local = list_local_files(local_path);
    for f in &initial_local {
        known.insert(f.clone());
    }
    eprintln!("[watch] known: {} files (manifest + local)", known.len());

    let total_uploaded = Arc::new(AtomicUsize::new(0));

    // Set up graceful shutdown
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                eprintln!("[watch] shutdown signal received, final sweep...");
                break;
            }
            _ = tokio::time::sleep(interval) => {
                let new_files = find_new_files(local_path, &known);
                if new_files.is_empty() {
                    continue;
                }

                eprintln!("[watch] found {} new files", new_files.len());

                let uploaded = upload_batch(
                    &client,
                    local_path,
                    &args.prefix,
                    &new_files,
                    args.concurrency,
                ).await;

                for f in &uploaded {
                    known.insert(f.clone());
                }

                let n = total_uploaded.fetch_add(uploaded.len(), Ordering::Relaxed) + uploaded.len();
                eprintln!("[watch] uploaded {} (total: {})", uploaded.len(), n);
            }
        }
    }

    // Final sweep: catch anything written during the last interval
    let final_new = find_new_files(local_path, &known);
    if !final_new.is_empty() {
        eprintln!("[watch] final sweep: {} files", final_new.len());
        let uploaded = upload_batch(
            &client,
            local_path,
            &args.prefix,
            &final_new,
            args.concurrency,
        ).await;
        for f in &uploaded {
            known.insert(f.clone());
        }
    }

    // Update manifest
    let mut sorted: Vec<&String> = known.iter().collect();
    sorted.sort();
    let manifest_data = serde_json::to_vec(&sorted)?;
    client.put_object(&manifest_key, &manifest_data).await?;
    eprintln!("[watch] manifest updated: {} files", known.len());

    // Cleanup PID file
    if let Some(ref pid_path) = args.pid_file {
        let _ = tokio::fs::remove_file(pid_path).await;
    }

    let n = total_uploaded.load(Ordering::Relaxed);
    eprintln!("[watch] done, uploaded {} files total", n);
    Ok(())
}

fn find_new_files(dir: &Path, known: &HashSet<String>) -> Vec<String> {
    let current = list_local_files(dir);
    current.difference(known).cloned().collect()
}

/// Upload a batch of files, return the list of successfully uploaded relative paths.
async fn upload_batch(
    client: &Arc<R2Client>,
    local_path: &Path,
    prefix: &str,
    files: &[String],
    concurrency: usize,
) -> Vec<String> {
    let sem = Arc::new(Semaphore::new(concurrency));
    let uploaded: Arc<tokio::sync::Mutex<Vec<String>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut handles = Vec::with_capacity(files.len());

    for rel_path in files {
        let sem = sem.clone();
        let client = client.clone();
        let prefix = prefix.to_string();
        let base = local_path.to_path_buf();
        let uploaded = uploaded.clone();
        let rel_path = rel_path.clone();

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let local_file = base.join(&rel_path);
            let remote_key = format!("{}{}", prefix, rel_path);

            let data = match tokio::fs::read(&local_file).await {
                Ok(d) => d,
                Err(_) => return, // file may have been deleted
            };

            for attempt in 0..3 {
                match client.put_object(&remote_key, &data).await {
                    Ok(()) => {
                        uploaded.lock().await.push(rel_path);
                        return;
                    }
                    Err(e) => {
                        if attempt == 2 {
                            eprintln!("[watch] PUT {} failed: {e}", rel_path);
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    Arc::try_unwrap(uploaded)
        .unwrap_or_else(|_| {
            tokio::sync::Mutex::new(Vec::new())
        })
        .into_inner()
}

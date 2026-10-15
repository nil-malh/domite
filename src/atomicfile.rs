// Adapted from nklmilojevic/sofka (MIT OR Apache-2.0)
// Copyright (c) Nikola Milojević and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Crash-safe replacement of small state and token files domite rewrites.
#![allow(dead_code)]

use std::ffi::OsString;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
const TEMP_ATTEMPTS: u32 = 16;

/// Write `contents` to `path` as one indivisible replacement, creating the
/// parent directory if it is missing.
pub fn write(path: &Path, contents: &str) -> Result<(), String> {
    write_with_mode(path, contents, None)
}

/// Write `contents` to `path` atomically with explicit Unix permissions (e.g. `0o600` for JWT cache).
pub fn write_secret(path: &Path, contents: &str) -> Result<(), String> {
    write_with_mode(path, contents, Some(0o600))
}

fn write_with_mode(path: &Path, contents: &str, mode: Option<u32>) -> Result<(), String> {
    write_with(path, contents, mode, temp_path)
}

fn write_with(
    path: &Path,
    contents: &str,
    mode: Option<u32>,
    mut candidate: impl FnMut(&Path) -> PathBuf,
) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let (file, temp) = create_temp(path, mode, &mut candidate).map_err(|e| e.to_string())?;
    write_then_rename(file, &temp, path, contents).map_err(|e| {
        let _ = std::fs::remove_file(&temp);
        e.to_string()
    })
}

fn create_temp(
    path: &Path,
    mode: Option<u32>,
    candidate: &mut impl FnMut(&Path) -> PathBuf,
) -> std::io::Result<(std::fs::File, PathBuf)> {
    let mut taken = None;
    for _ in 0..TEMP_ATTEMPTS {
        let temp = candidate(path);
        let mut opts = std::fs::File::options();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        if let Some(m) = mode {
            use std::os::unix::fs::OpenOptionsExt as _;
            opts.mode(m);
        }
        #[cfg(not(unix))]
        let _ = mode;
        match opts.open(&temp) {
            Ok(file) => return Ok((file, temp)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => taken = Some(e),
            Err(e) => return Err(e),
        }
    }
    Err(taken.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "no free temp name beside the state file",
        )
    }))
}

fn write_then_rename(
    mut file: std::fs::File,
    temp: &Path,
    path: &Path,
    contents: &str,
) -> std::io::Result<()> {
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temp, path)
}

fn temp_path(path: &Path) -> PathBuf {
    let n = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let mut name = OsString::from(path.as_os_str());
    name.push(format!(".tmp{}.{n:x}.{nanos:x}", std::process::id()));
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "domite-atomicfile-{}-{tag}-{}",
            std::process::id(),
            NEXT_TEMP.load(Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn creates_missing_parents_and_replaces_in_place() {
        let dir = scratch("replace");
        let path = dir.join("nested").join("sort.toml");
        write(&path, "first").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "first");
        write(&path, "second").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "second");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn write_secret_sets_0600_permissions() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = scratch("secret");
        let path = dir.join("jwt");
        write_secret(&path, "secret-jwt").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

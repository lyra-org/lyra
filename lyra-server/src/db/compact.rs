// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Pre-open storage compaction for the on-disk agdb file: agdb's mmap mode
//! materializes the entire file, so a fragmented DB can refuse to open even
//! when its logical content fits in RAM. We compact via `DbFile` first.
//! Scope: fragmentation OOM only. See https://github.com/agnesoft/agdb/discussions/1781.

use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    path::Path,
};

use anyhow::{
    Context,
    Result,
    anyhow,
};

use super::bootstrap;
use crate::config::{
    DbConfig,
    DbKind,
};

/// Shared with `services::maintenance` so the runtime sweeper and the pre-open
/// path agree on what counts as "fragmented."
pub(crate) const FRAGMENTATION_RATIO_THRESHOLD: f64 = 1.3;

/// Floor for the pre-open check — small DBs can't realistically OOM in mmap.
const PRE_OPEN_MIN_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Higher than the runtime sweeper's 50 MB — cold-start latency is user-visible.
const PRE_OPEN_MIN_WASTE_BYTES: u64 = 100 * 1024 * 1024;

/// Cushion on top of `file_bytes + logical_bytes` (see `ensure_space_for_optimize`).
const PRE_OPEN_DISK_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;

/// Compaction pass for an mmap-configured `config` whose file warrants it.
/// Disk-full is a hard error; everything else (open errors, optimize errors,
/// panics) is logged and swallowed so the caller falls through to the main open.
pub(crate) fn pre_open(config: &DbConfig) -> Result<()> {
    if !matches!(config.kind, DbKind::Mmap) {
        return Ok(());
    }

    let db_path = &config.path;
    let Some(file_bytes) = file_size(db_path) else {
        return Ok(());
    };

    if file_bytes < PRE_OPEN_MIN_FILE_BYTES {
        return Ok(());
    }

    tracing::info!(
        path = %db_path.display(),
        file_bytes,
        "opening db in file mode for pre-open fragmentation check (may apply WAL recovery)"
    );

    let db_path_str = db_path.to_string_lossy();
    let mut db = match bootstrap::open(DbKind::File, db_path_str.as_ref()) {
        Ok(db) => db,
        Err(err) => {
            tracing::warn!(
                error = %err,
                path = %db_path.display(),
                "pre-open: failed to open db in file mode; main open will surface the underlying error"
            );
            return Ok(());
        }
    };

    let logical_bytes = db.size();
    if logical_bytes == 0 {
        // Nothing to assess; also avoids the ratio divide-by-zero.
        return Ok(());
    }

    let waste_bytes = file_bytes.saturating_sub(logical_bytes);
    let ratio = file_bytes as f64 / logical_bytes as f64;
    if waste_bytes < PRE_OPEN_MIN_WASTE_BYTES || ratio < FRAGMENTATION_RATIO_THRESHOLD {
        return Ok(());
    }

    let parent = db_path.parent().unwrap_or_else(|| Path::new("."));
    ensure_space_for_optimize(parent, file_bytes, logical_bytes)
        .context("pre-open compaction aborted before partial rewrite")?;

    tracing::info!(
        path = %db_path.display(),
        file_bytes,
        logical_bytes,
        waste_bytes,
        ratio = format!("{ratio:.2}x"),
        "running pre-open compaction"
    );

    match run_optimize_swallowing_panics(|| {
        let result = db.optimize_storage();
        drop(db);
        result
    }) {
        OptimizeOutcome::Ok => {
            let new_file_bytes = file_size(db_path).unwrap_or(file_bytes);
            tracing::info!(
                path = %db_path.display(),
                file_bytes_before = file_bytes,
                file_bytes_after = new_file_bytes,
                reclaimed_bytes = file_bytes.saturating_sub(new_file_bytes),
                "pre-open compaction complete"
            );
        }
        OptimizeOutcome::Err(err) => {
            tracing::warn!(
                error = %err,
                path = %db_path.display(),
                "pre-open compaction returned an error; falling through to main open (WAL replay on next open is the recovery path)"
            );
        }
        OptimizeOutcome::Panicked => {
            tracing::warn!(
                path = %db_path.display(),
                "pre-open compaction panicked; falling through to main open (WAL replay on next open is the recovery path)"
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
enum OptimizeOutcome {
    Ok,
    Err(agdb::DbError),
    Panicked,
}

/// `AssertUnwindSafe` is sound because the caller moves the `DbAny` handle
/// into `f` and drops it there — no poisoned handle escapes. The wrapper
/// gives the panic path a testable seam; the regression test asserts it.
fn run_optimize_swallowing_panics<F>(f: F) -> OptimizeOutcome
where
    F: FnOnce() -> Result<(), agdb::DbError>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => OptimizeOutcome::Ok,
        Ok(Err(err)) => OptimizeOutcome::Err(err),
        Err(_) => OptimizeOutcome::Panicked,
    }
}

/// Pre-flight disk-space check. agdb's WAL records "previous bytes before
/// writes," so a full rewrite peaks at ~`file_bytes` (WAL) + `logical_bytes`
/// (new file) + headroom.
pub(crate) fn ensure_space_for_optimize(
    parent: &Path,
    file_bytes: u64,
    logical_bytes: u64,
) -> Result<()> {
    let needed = file_bytes
        .saturating_add(logical_bytes)
        .saturating_add(PRE_OPEN_DISK_HEADROOM_BYTES);

    let available = match fs4::available_space(parent) {
        Ok(bytes) => bytes,
        Err(err) => {
            // Treat probe failure as zero — auto-disengaging would defeat the guard.
            tracing::warn!(
                error = %err,
                path = %parent.display(),
                "available_space probe failed; treating as zero free bytes for the disk-full guard"
            );
            0
        }
    };

    if available < needed {
        return Err(anyhow!(
            "optimize_storage needs ~{needed} bytes free in {} but only {available} available",
            parent.display()
        ));
    }
    Ok(())
}

fn file_size(path: &Path) -> Option<u64> {
    std::fs::metadata(path).ok().map(|m| m.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_for_memory_kind() -> Result<()> {
        let config = DbConfig {
            kind: DbKind::Memory,
            path: std::path::PathBuf::from("ignored"),
        };
        pre_open(&config)
    }

    #[test]
    fn skips_for_file_kind() -> Result<()> {
        let config = DbConfig {
            kind: DbKind::File,
            path: std::path::PathBuf::from("ignored"),
        };
        pre_open(&config)
    }

    #[test]
    fn skips_when_file_missing() -> Result<()> {
        let config = DbConfig {
            kind: DbKind::Mmap,
            path: std::path::PathBuf::from("/nonexistent/lyra-pre-open-missing.db"),
        };
        pre_open(&config)
    }

    #[test]
    fn ensure_space_uses_file_plus_logical_plus_headroom() {
        // Can't mock fs4; impossibly large sizes must trip the formula on /tmp.
        let parent = std::env::temp_dir();
        let huge = u64::MAX / 4;
        let err = ensure_space_for_optimize(&parent, huge, huge)
            .expect_err("must refuse impossibly large optimize");
        assert!(
            err.to_string().contains("only"),
            "error should name available bytes: {err}"
        );
    }

    #[test]
    fn run_optimize_catches_panics() {
        // Regression guard: if a future PR removes `catch_unwind`, drops the
        // `AssertUnwindSafe`, or restructures this wrapper such that a panic
        // in the closure escapes, this test must fail.
        let outcome = run_optimize_swallowing_panics(|| panic!("simulated optimize panic"));
        assert!(
            matches!(outcome, OptimizeOutcome::Panicked),
            "expected Panicked, got {outcome:?}"
        );
    }

    #[test]
    fn run_optimize_propagates_returned_err() {
        let outcome =
            run_optimize_swallowing_panics(|| Err(agdb::DbError::from("simulated db error")));
        assert!(matches!(outcome, OptimizeOutcome::Err(_)));
    }

    #[test]
    fn run_optimize_propagates_ok() {
        let outcome = run_optimize_swallowing_panics(|| Ok(()));
        assert!(matches!(outcome, OptimizeOutcome::Ok));
    }
}

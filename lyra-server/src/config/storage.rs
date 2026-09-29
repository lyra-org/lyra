// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

use std::{
    io,
    path::Path,
};

use anyhow::Result;

pub(crate) fn ensure_writable_directory(path: &Path, description: &str) -> Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|error| write_error(&format!("create {description}"), path, error))?;
    let probe = tempfile::Builder::new()
        .prefix(".lyra-write-check-")
        .tempfile_in(path)
        .map_err(|error| write_error(&format!("write to {description}"), path, error))?;
    probe.close().map_err(|error| {
        write_error(&format!("remove write check in {description}"), path, error)
    })?;
    Ok(())
}

pub(crate) fn write_error(operation: &str, path: &Path, error: io::Error) -> anyhow::Error {
    let mut context = format!("failed to {operation} at '{}'", path.display());
    match error.kind() {
        #[cfg(unix)]
        io::ErrorKind::PermissionDenied => {
            // These calls read process identity and have no preconditions.
            let (uid, gid) = unsafe { (libc::geteuid(), libc::getegid()) };
            context.push_str(&format!(
                ": Lyra runs as user {uid} (group {gid}), which cannot write to this folder. \
                 Make that user the owner of the folder and its files, \
                 or see \"Storage permissions\" in the installation guide"
            ));
        }
        #[cfg(not(unix))]
        io::ErrorKind::PermissionDenied => {
            context.push_str(": Lyra cannot write to this folder. Give it write access to the folder and its files");
        }
        io::ErrorKind::ReadOnlyFilesystem => {
            context.push_str("; this storage must be writable: remove the read-only mount option or choose a writable path");
        }
        _ => {}
    }
    anyhow::Error::new(error).context(context)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_check_leaves_existing_contents_unchanged() -> Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("storage");
        ensure_writable_directory(&path, "test directory")?;
        std::fs::write(path.join("existing"), "keep")?;
        ensure_writable_directory(&path, "test directory")?;
        assert_eq!(std::fs::read_to_string(path.join("existing"))?, "keep");
        assert_eq!(std::fs::read_dir(path)?.count(), 1);
        Ok(())
    }

    #[test]
    fn recovery_matches_the_io_failure() {
        for (kind, expected) in [
            (
                io::ErrorKind::PermissionDenied,
                "cannot write to this folder",
            ),
            (io::ErrorKind::ReadOnlyFilesystem, "read-only mount"),
            (io::ErrorKind::StorageFull, ""),
        ] {
            let error = write_error("write", Path::new("/storage"), io::Error::from(kind));
            let message = error.to_string();
            assert!(message.contains("write at '/storage'"));
            assert!(message.contains(expected));
            assert_eq!(
                message.contains("read-only mount"),
                kind == io::ErrorKind::ReadOnlyFilesystem
            );
            assert_eq!(error.downcast_ref::<io::Error>().unwrap().kind(), kind);
            #[cfg(unix)]
            assert_eq!(
                message.contains(&format!(
                    "runs as user {} (group {})",
                    unsafe { libc::geteuid() },
                    unsafe { libc::getegid() }
                )),
                kind == io::ErrorKind::PermissionDenied
            );
        }
    }
}

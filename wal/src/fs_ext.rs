//! Cross-platform durable-rename helpers.
//!
//! This module is prepared for your use so that you can focus on the
//! learning objectives of the exercise (framing, replay, compaction)
//! rather than on the POSIX-vs-Win32 differences in directory-metadata
//! durability. Call these two helpers from your own code:
//!
//! - [`rename_durably(tmp, final, dir)`](rename_durably) every time you
//!   commit a new snapshot; this is the atomic-rename step.
//! - [`fsync_dir(dir)`](fsync_dir) whenever you add a new file to the
//!   store directory, e.g., the first time you create the log file, and
//!   want its directory entry to survive a crash.
//!
//! `rename_durably` encodes the same guarantee on every supported
//! platform: once it returns `Ok`, `dst` points at the old contents of
//! `src` and the directory-metadata change has been committed to stable
//! storage. That is the property the snapshot-commit path relies on to
//! survive a crash between "rename scheduled" and "rename visible after
//! reboot".
//!
//! - POSIX: `fs::rename` (atomic within a filesystem) followed by `fsync`
//!   on the parent directory handle.
//! - Windows: `MoveFileExW` with
//!   `MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH`. The
//!   `WRITE_THROUGH` flag forces the NTFS transaction log to commit the
//!   directory-entry change synchronously.

use std::io;
use std::path::Path;

/// Fsync the directory itself. On POSIX, opens the directory as a file
/// and calls `sync_all`. On non-Unix, a no-op: Windows commits directory
/// updates via the NTFS journal and our renames use `MOVEFILE_WRITE_THROUGH`,
/// so nothing extra is required here.
#[cfg(unix)]
pub(crate) fn fsync_dir(path: &Path) -> io::Result<()> {
	std::fs::File::open(path)?.sync_all()
}

#[cfg(not(unix))]
pub(crate) fn fsync_dir(_path: &Path) -> io::Result<()> {
	Ok(())
}

/// Rename `src` to `dst` and make the directory-metadata change durable
/// before returning. `dir` is the parent directory of both paths.
#[cfg(unix)]
pub(crate) fn rename_durably(src: &Path, dst: &Path, dir: &Path) -> io::Result<()> {
	std::fs::rename(src, dst)?;
	fsync_dir(dir)
}

#[cfg(windows)]
pub(crate) fn rename_durably(src: &Path, dst: &Path, _dir: &Path) -> io::Result<()> {
	use std::os::windows::ffi::OsStrExt;
	use windows_sys::Win32::Storage::FileSystem::{
		MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
	};

	fn wide(path: &Path) -> Vec<u16> {
		path.as_os_str()
			.encode_wide()
			.chain(std::iter::once(0))
			.collect()
	}

	let src_w = wide(src);
	let dst_w = wide(dst);
	// SAFETY: both pointers refer to NUL-terminated UTF-16 buffers that
	// outlive the call.
	let ok = unsafe {
		MoveFileExW(
			src_w.as_ptr(),
			dst_w.as_ptr(),
			MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
		)
	};
	if ok == 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn rename_durably(src: &Path, dst: &Path, _dir: &Path) -> io::Result<()> {
	// Best-effort fallback for exotic targets: rename without a
	// durability guarantee. The rest of the code keeps working; only
	// crash-safety during compaction degrades.
	std::fs::rename(src, dst)
}

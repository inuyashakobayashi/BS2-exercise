//! Write-ahead-log-backed key-value store.
//!
//! The public API below is fixed: the follow-up exercise will depend on
//! these signatures, and the integration tests shipped with this template
//! talk to this surface only. Implement the bodies however you like
//! (your own framing, your own in-memory representation, your own serialiser)
//! as long as the behaviour of every method matches its doc comment.
//!
//! The helper module [`fs_ext`] is provided ready-to-use. Call
//! `fs_ext::rename_durably(tmp, final, dir)` for the atomic rename during
//! compaction and `fs_ext::fsync_dir(dir)` after you first create the
//! store file to make its directory entry durable. You should not need to
//! modify it.

mod fs_ext;

use std::fmt;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;

use serde::{Serialize, de::DeserializeOwned};

/// A crash-safe key-value store backed by a single on-disk file.
///
/// Reads must be served from memory; every mutation must be persisted
/// (framed, checksummed, and fsynced) before the method returns. See
/// the individual method docs for the exact semantics.
pub struct Store<K, V> {
	// TODO: replace this with the fields your implementation needs.
	// The `PhantomData` is only here so that the template compiles
	// with type parameters that are otherwise unused.
	_marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Store<K, V>
where
	K: Ord + Clone + Serialize + DeserializeOwned,
	V: Serialize + DeserializeOwned,
{
	/// Open (or create) the store at `path`. Must reconstruct the
	/// in-memory state by replaying the file. A torn or corrupted
	/// tail entry must be truncated; every entry before it must survive.
	pub fn open(_path: impl AsRef<Path>) -> Result<Self, Error> {
		todo!()
	}

	/// Return a reference to the value stored under `key`, if any.
	/// Purely in-memory; must not touch the disk.
	pub fn get(&self, _key: &K) -> Option<&V> {
		todo!()
	}

	/// Insert or overwrite `key`. An acknowledged `set` is crash-safe.
	pub fn set(&mut self, _key: K, _value: V) -> Result<(), Error> {
		todo!()
	}

	/// Remove `key` and is crash-safe. Deleting a key that was
	/// never present is not an error.
	pub fn delete(&mut self, _key: &K) -> Result<(), Error> {
		todo!()
	}

	/// Iterate over the key-value pairs whose keys fall within `range`,
	/// in ascending key order. References borrow from the store; the
	/// caller clones explicitly when ownership is needed.
	pub fn scan<R>(&self, _range: R) -> impl Iterator<Item = (&K, &V)>
	where
		R: RangeBounds<K>,
	{
		// Placeholder iterator so that the signature compiles. Replace
		// with the real implementation.
		std::iter::empty()
	}

	/// Compact the store. Must be crash-safe; a crash during compaction
	/// must not lose acknowledged data.
	pub fn compact(&mut self) -> Result<(), Error> {
		todo!()
	}

	/// Number of entries currently visible in the store.
	pub fn len(&self) -> usize {
		todo!()
	}

	/// `true` iff the store contains no entries.
	pub fn is_empty(&self) -> bool {
		todo!()
	}
}

/// Errors surfaced by the public API.
#[derive(Debug)]
pub enum Error {
	Io(io::Error),
	// TODO: add variants.
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Io(e) => write!(f, "i/o error: {e}"),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::Io(e) => Some(e),
		}
	}
}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Self {
		Error::Io(e)
	}
}

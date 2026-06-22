//! In-memory key-value store. Keeps the same `Store<K, V>` interface as
//! the WAL exercise so chunk-server code reads identically, but without
//! any durable-storage machinery. A chunk-server restart loses all local
//! replicas; the bonus re-replication feature compensates for this.

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::path::Path;

// --------------------------------------------------------------------------
// Error
// --------------------------------------------------------------------------

/// Error type for store operations. The in-memory implementation never
/// fails, but the empty enum preserves the `Result<(), Error>` signatures
/// from the WAL exercise so call sites stay uniform.
pub enum Error {}

impl std::fmt::Display for Error {
	fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match *self {}
	}
}

impl std::fmt::Debug for Error {
	fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match *self {}
	}
}

impl std::error::Error for Error {}

// --------------------------------------------------------------------------
// Store
// --------------------------------------------------------------------------

pub struct Store<K: Ord, V> {
	map: BTreeMap<K, V>,
}

impl<K: Ord, V> Store<K, V> {
	/// Open (or create) a store. The path is accepted for call-site parity
	/// with the WAL exercise but is ignored; no data is ever written to disk.
	pub fn open(_path: impl AsRef<Path>) -> Result<Self, Error> {
		Ok(Self {
			map: BTreeMap::new(),
		})
	}

	pub fn get(&self, key: &K) -> Option<&V> {
		self.map.get(key)
	}

	pub fn set(&mut self, key: K, value: V) -> Result<(), Error> {
		self.map.insert(key, value);
		Ok(())
	}

	pub fn delete(&mut self, key: &K) -> Result<(), Error> {
		self.map.remove(key);
		Ok(())
	}

	pub fn scan<R: RangeBounds<K>>(&self, range: R) -> impl Iterator<Item = (&K, &V)> {
		self.map.range(range)
	}

	pub fn len(&self) -> usize {
		self.map.len()
	}

	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}
}

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

use postcard::to_stdvec;
use serde::Deserialize;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
/// A crash-safe key-value store backed by a single on-disk file.
///
/// Reads must be served from memory; every mutation must be persisted
/// (framed, checksummed, and fsynced) before the method returns. See
/// the individual method docs for the exact semantics.
pub struct Store<K, V> {
    // TODO: replace this with the fields your implementation needs.
    // The `PhantomData` is only here so that the template compiles
    // with type parameters that are otherwise unused.
    file: File,
    btree_map: BTreeMap<K, V>,
    path: PathBuf,
}
#[derive(Serialize, Deserialize)]
enum Record<K, V> {
    Set { key: K, value: V },
    Delete { key: K },
}
impl<K, V> Store<K, V>
where
    K: Ord + Clone + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned + Clone, // hier we add a new trait Clone for V,so that  we can travel BtreeMap to make a snapshot
{
    /// Open (or create) the store at `path`. Must reconstruct the
    /// in-memory state by replaying the file. A torn or corrupted
    /// tail entry must be truncated; every entry before it must survive.
    pub fn open(_path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = _path.as_ref().to_path_buf();
        let path_temp = path.with_extension("tmp");
        if path_temp.exists() {
            std::fs::remove_file(path_temp)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)?;
        let mut btree_map = BTreeMap::new();
        file.seek(SeekFrom::Start(0))?;
        const MAX_RECORD: u32 = 64 * 1024 * 1024;
        let mut file_size = file.metadata()?.len();
        loop {
            if file_size < 8 {
                break;
            } else {
                let mut buf4 = [0u8; 4];
                let mut buf4_crc = [0u8; 4];
                file.read_exact(&mut buf4)?;
                let len = u32::from_le_bytes(buf4);
                if len > MAX_RECORD {
                    break;
                }
                let mut buf_payload = vec![0u8; len as usize];
                file.read_exact(&mut buf4_crc)?;
                let crc = u32::from_le_bytes(buf4_crc);
                file.read_exact(&mut buf_payload)?;
                if crc32fast::hash(&buf_payload) != crc {
                    break;
                }

                match postcard::from_bytes::<Record<K, V>>(&buf_payload) {
                    Ok(Record::Set { key, value }) => {
                        btree_map.insert(key, value);
                    }
                    Ok(Record::Delete { key }) => {
                        btree_map.remove(&key);
                    }
                    Err(_) => break,
                }
                file_size -= 8 + len as u64;
            }
        }
        Ok(Store {
            btree_map,
            file,
            path,
        })
    }

    /// Return a reference to the value stored under `key`, if any.
    /// Purely in-memory; must not touch the disk.
    pub fn get(&self, _key: &K) -> Option<&V> {
        self.btree_map.get(_key)
    }

    /// Insert or overwrite `key`. An acknowledged `set` is crash-safe.
    pub fn set(&mut self, _key: K, _value: V) -> Result<(), Error> {
        let record = Record::Set {
            key: _key,
            value: _value,
        };
        let payload = to_stdvec(&record).unwrap();
        let checksum = crc32fast::hash(&payload);
        let len = payload.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.sync_all()?;
        if let Record::Set { key, value } = record {
            self.btree_map.insert(key, value);
        }

        Ok(())
    }

    /// Remove `key` and is crash-safe. Deleting a key that was
    /// never present is not an error.
    pub fn delete(&mut self, _key: &K) -> Result<(), Error> {
        let record: Record<_, V> = Record::Delete { key: _key.clone() };
        let payload = to_stdvec(&record).unwrap();
        let checksum = crc32fast::hash(&payload);
        let len = payload.len() as u32;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&checksum.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.sync_all()?;
        self.btree_map.remove(_key);
        Ok(())
    }

    /// Iterate over the key-value pairs whose keys fall within `range`,
    /// in ascending key order. References borrow from the store; the
    /// caller clones explicitly when ownership is needed.
    pub fn scan<R>(&self, _range: R) -> impl Iterator<Item = (&K, &V)>
    where
        R: RangeBounds<K>,
    {
        self.btree_map.range(_range)
        // Placeholder iterator so that the signature compiles. Replace
        // with the real implementation.
    }

    /// Compact the store. Must be crash-safe; a crash during compaction
    /// must not lose acknowledged data.
    pub fn compact(&mut self) -> Result<(), Error> {
        let path_temp = self.path.with_extension("tmp");
        let mut file_temp = File::create(&path_temp)?;
        for (k, v) in &self.btree_map {
            let record = Record::Set {
                key: k.clone(),
                value: v.clone(),
            };
            let payload = to_stdvec(&record).unwrap();
            let checksum = crc32fast::hash(&payload);
            let len = payload.len() as u32;
            file_temp.write_all(&len.to_le_bytes())?;
            file_temp.write_all(&checksum.to_le_bytes())?;
            file_temp.write_all(&payload)?;
        }

        file_temp.sync_all()?;
        let parent = self.path.parent().expect("path has no parent");
        fs_ext::rename_durably(&path_temp, &self.path, parent)?;

        self.file = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&self.path)?;

        Ok(())
    }

    /// Number of entries currently visible in the store.
    pub fn len(&self) -> usize {
        self.btree_map.len()
    }

    /// `true` iff the store contains no entries.
    pub fn is_empty(&self) -> bool {
        self.btree_map.is_empty()
    }
}

/// Errors surfaced by the public API.
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Serialization(postcard::Error), // TODO: add variants.
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "i/o error: {e}"),
            Error::Serialization(e) => write!(f, "seriazation error : {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Serialization(e) => Some(e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<postcard::Error> for Error {
    fn from(e: postcard::Error) -> Self {
        Error::Serialization(e)
    }
}

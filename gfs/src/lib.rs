//! Simplified Google File System.
//!
//! # Architecture
//!
//! The system has three roles (master, chunk server, client) that
//! communicate over two distinct planes:
//!
//! * Control plane: `tarpc` over JSON. Carries metadata only: chunk
//!   allocation, lookups, and heartbeats. Defined by the [`ClientMaster`]
//!   and [`ChunkMaster`] service traits in this module.
//! * Data plane: raw TCP with a length-prefixed `postcard` frame.
//!   Carries chunk bytes between client and primary and along the
//!   primary -> secondary chain. The tarpc JSON codec is not sized for
//!   multi-MB payloads; keeping data off the RPC channel mirrors real
//!   GFS.
//!
//! # Design choices worth flagging
//!
//! * Master state is in memory. If the master crashes the namespace
//!   is lost. Chunk contents are also held in memory per chunk server
//!   (see [`store`]), so a chunk-server restart loses its local replicas.
//!   Real GFS persists both the op log and chunk data durably.
//! * Heartbeats are the only chunk-server -> master signal beyond
//!   registration. The heartbeat carries the full `chunks_held` list as
//!   a *chunk report*; the master reconciles that list against its
//!   metadata to learn that a freshly-written chunk has landed and that
//!   a previously-held chunk is gone after a lazy drop. The reply (see
//!   [`HeartbeatResponse`]) is the *only* master -> chunk-server
//!   channel: it piggy-backs lazy-deletion commands.
//! * Replication factor is 2. One primary, one secondary.
//!   [`ClientMaster::allocate`] always returns both (`secondary:
//!   Some(_)`); [`ClientMaster::lookup`] may report `secondary: None`
//!   after a failure and promotes the surviving replica to `primary` so
//!   the client's write/read path stays uniform.
//! * Chunk size is per-call. [`CHUNK_SIZE`] below is the documented
//!   default (4 MB per the spec) but [`ClientMaster::allocate`] takes
//!   `chunk_size` as an argument. Integration tests pass a tiny size
//!   so a "multi-chunk file" is a few KB rather than megabytes. Chunk
//!   servers store `Vec<u8>` payloads of arbitrary length and never see
//!   this constant.

use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub mod chunk;
pub mod data;
pub mod master;

/// In-memory key-value store with the same `Store<K,V>` interface as
/// the WAL exercise. Chunk data lives only in RAM; a chunk-server restart
/// loses local replicas.
pub mod store;

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Default chunk size (4 MB), matching the exercise spec. Clients may
/// override this on a per-call basis via [`ClientMaster::allocate`].
pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// Number of replicas per chunk (one primary, one secondary).
pub const REPLICATION_FACTOR: usize = 2;

/// Period at which each chunk server sends [`ChunkMaster::heartbeat`].
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// A chunk server is marked offline once this many consecutive
/// heartbeats are missed, so the effective timeout is
/// `MISSED_HEARTBEATS * HEARTBEAT_INTERVAL`.
pub const MISSED_HEARTBEATS: u32 = 3;

// --------------------------------------------------------------------------
// Identifiers
// --------------------------------------------------------------------------

/// Globally unique chunk identifier. The master hands out IDs from a
/// single monotonic counter and never reuses them, even across
/// file-deletion cycles.
pub type ChunkId = u64;

/// Identifier assigned by the master to each chunk server on
/// registration. Lets heartbeats and acknowledgements refer to a server
/// without re-sending its socket address.
pub type ServerId = u64;

// --------------------------------------------------------------------------
// Replica set
// --------------------------------------------------------------------------

/// Addresses of the replicas holding a chunk, in write order.
///
/// `allocate` always returns `secondary: Some(_)` (both replicas were
/// online at allocation time). `lookup` may return `secondary: None` if
/// the secondary has since gone offline; if the original *primary* is
/// down but the secondary is still up, the master promotes the
/// secondary to `primary` in the returned [`ReplicaSet`], so callers
/// can always read/write `primary` without reasoning about failure
/// direction. If both replicas are offline, `lookup` errors.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSet {
	pub primary: SocketAddr,
	pub secondary: Option<SocketAddr>,
}

// --------------------------------------------------------------------------
// Heartbeat response
// --------------------------------------------------------------------------

/// Reply to a chunk server's heartbeat. This is the master's steering
/// channel: lazy-deletion commands ride on the heartbeat reply instead
/// of needing a dedicated master -> chunk-server RPC service.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HeartbeatResponse {
	/// Chunks the server should delete locally. Populated when a file
	/// was deleted and some of its chunks are still present on this
	/// server. Idempotent: as long as the chunk keeps appearing in the
	/// server's `chunks_held`, the master keeps re-issuing the drop
	/// command; the master notices the deletion landed once the chunk
	/// stops being reported.
	pub drop_chunks: Vec<ChunkId>,
}

// --------------------------------------------------------------------------
// Error type
// --------------------------------------------------------------------------

/// Errors reported to clients across the control plane. Kept
/// `Serialize + Deserialize` so tarpc can return them across the wire.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MasterError {
	/// The requested path has no mapping in the namespace.
	NotFound,
	/// The path already exists on `allocate`.
	AlreadyExists,
	/// Fewer than [`REPLICATION_FACTOR`] chunk servers are online.
	NotEnoughServers,
	/// At least one of the file's chunks has zero live replicas.
	NoLiveReplicas,
	/// Client passed a zero chunk size to `allocate`.
	InvalidChunkSize,
}

impl fmt::Display for MasterError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			MasterError::NotFound => f.write_str("path not found"),
			MasterError::AlreadyExists => f.write_str("path already exists"),
			MasterError::NotEnoughServers => f.write_str("not enough chunk servers online"),
			MasterError::NoLiveReplicas => f.write_str("at least one chunk has no live replicas"),
			MasterError::InvalidChunkSize => f.write_str("chunk size must be greater than zero"),
		}
	}
}

impl std::error::Error for MasterError {}

// --------------------------------------------------------------------------
// Control-plane services
// --------------------------------------------------------------------------

/// RPC surface between a client and the master server.
#[tarpc::service]
pub trait ClientMaster {
	/// Allocate `ceil(len / chunk_size)` chunks for a new file at
	/// `path`, round-robin across currently online chunk servers.
	/// Returns chunk IDs and their replica sets in file order. Errors
	/// if the path already exists or fewer than [`REPLICATION_FACTOR`]
	/// servers are online.
	async fn allocate(
		path: String,
		len: u64,
		chunk_size: u64,
	) -> Result<Vec<(ChunkId, ReplicaSet)>, MasterError>;

	/// Look up an existing file and return its chunks with current
	/// replica addresses. Offline replicas are filtered out; if the
	/// original primary is down but the secondary is live, the
	/// secondary is promoted to `primary` in the response.
	async fn lookup(path: String) -> Result<Vec<(ChunkId, ReplicaSet)>, MasterError>;

	/// Remove a path from the namespace. Chunk bytes are deleted
	/// lazily: the master marks the chunks as orphaned and commands the
	/// holding chunk servers to drop them via the next heartbeat.
	async fn delete(path: String) -> Result<(), MasterError>;
}

/// RPC surface between a chunk server and the master server.
#[tarpc::service]
pub trait ChunkMaster {
	/// Register a new chunk server. `socket_addr` is the server's
	/// data-plane TCP address the endpoint clients and peer
	/// replicas will open raw TCP connections to. Returns the
	/// master-assigned [`ServerId`].
	async fn register(socket_addr: SocketAddr) -> ServerId;

	/// Periodic heartbeat. `chunks_held` is the full set of chunk IDs
	/// currently in the server's local store and serves as the master's
	/// authoritative chunk report for this server: a chunk appearing
	/// here for the first time confirms a successful write, a chunk
	/// disappearing tells the master this server no longer holds it
	/// (e.g. after a lazy-GC drop), and unknown chunks land in
	/// `drop_chunks` of the reply (see [`HeartbeatResponse`]).
	async fn heartbeat(server_id: ServerId, chunks_held: Vec<ChunkId>) -> HeartbeatResponse;
}

//! Master server: namespace, chunk placement, heartbeat tracking.
//!
//! Hold the master state in memory behind a [`tokio::sync::Mutex`].
//! None of the operations exposed here are on a hot path; readability
//! beats fine-grained concurrency.
//!
//! A background task ([`MasterServer::detect_timeouts`]) ticks at least
//! once per heartbeat interval and marks a server offline once it has
//! missed `MISSED_HEARTBEATS` consecutive heartbeats.
//!
//! Placement is round-robin over the currently-online chunk servers.
//! Advance the cursor once per replica (not once per chunk) so that
//! allocations stay evenly spread when `REPLICATION_FACTOR` is small
//! relative to the cluster size.
//!
//! All chunk-server -> master state changes flow through `heartbeat`:
//! a chunk reported in `chunks_held` whose metadata exists is treated
//! as confirmed-present on the reporter (covers the initial chain-
//! replicated write); a deleted chunk that this server no longer
//! reports is treated as locally dropped, and the chunk is forgotten
//! once its last replica reports the drop.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use futures::{future, prelude::*};
use tarpc::context::Context;
use tarpc::serde_transport::tcp;
use tarpc::server::{BaseChannel, Channel};
use tarpc::tokio_serde::formats::Json;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::instrument;

use crate::{
	ChunkId, ChunkMaster, ClientMaster, HEARTBEAT_INTERVAL, HeartbeatResponse, MISSED_HEARTBEATS,
	MasterError, REPLICATION_FACTOR, ReplicaSet, ServerId,
};

// --------------------------------------------------------------------------
// State
// --------------------------------------------------------------------------

/// What the master knows about one registered chunk server.
struct ServerInfo {
	/// Data-plane TCP address handed out to clients in [`ReplicaSet`]s.
	addr: SocketAddr,
	/// Wall-clock time of the most recent heartbeat. Used by
	/// [`MasterServer::detect_timeouts`] to flip stale servers offline.
	last_seen: Instant,
	/// `false` once the server has missed `MISSED_HEARTBEATS` beats.
	/// Offline servers are filtered out of `lookup` and skipped by
	/// placement, but kept in the map so a returning server can come back.
	online: bool,
}

/// Master-side metadata for one chunk.
struct ChunkInfo {
	/// Server IDs expected to hold this chunk, in write order
	/// (`[primary, secondary]`). Pruned as servers report (via heartbeat)
	/// that they have dropped the chunk.
	replicas: Vec<ServerId>,
	/// Set when the owning file was deleted; the chunk lingers only so the
	/// master can command its holders to drop it via heartbeat replies,
	/// and is forgotten once `replicas` empties.
	deleted: bool,
}

#[derive(Default)]
struct State {
	/// Namespace: path -> the file's chunk IDs in file order.
	files: HashMap<String, Vec<ChunkId>>,
	/// Per-chunk placement and GC metadata.
	chunks: HashMap<ChunkId, ChunkInfo>,
	/// Registered chunk servers by ID.
	servers: HashMap<ServerId, ServerInfo>,
	/// Monotonic, never-reused chunk ID counter.
	next_chunk_id: ChunkId,
	/// Monotonic chunk-server ID counter.
	next_server_id: ServerId,
	/// Round-robin placement cursor over the online-server list. Advanced
	/// once per replica so allocations spread evenly.
	rr_cursor: usize,
}

impl State {
	/// Online server IDs in ascending order, for deterministic placement.
	fn online_server_ids(&self) -> Vec<ServerId> {
		let mut ids: Vec<ServerId> = self
			.servers
			.iter()
			.filter(|(_, s)| s.online)
			.map(|(&id, _)| id)
			.collect();
		ids.sort_unstable();
		ids
	}
}

/// Cheap-clone handle to the master. Clones share state via `Arc`.
#[derive(Clone, Default)]
pub struct MasterServer(Arc<Mutex<State>>);

// --------------------------------------------------------------------------
// Lifecycle
// --------------------------------------------------------------------------

impl MasterServer {
	pub fn new() -> Self {
		Self::default()
	}

	/// Bind the client-facing and chunk-server-facing tarpc listeners
	/// and spawn their accept loops. Returns the actually-bound
	/// addresses (so callers can pass a `port: 0` bind and discover
	/// what the OS handed out) together with the two join handles.
	///
	/// Used by the `master_server` binary to stand up the service and
	/// by integration tests to run a master inside the test process.
	pub async fn spawn_listeners(
		&self,
		client_bind: SocketAddr,
		chunk_bind: SocketAddr,
	) -> io::Result<(SocketAddr, SocketAddr, JoinHandle<()>, JoinHandle<()>)> {
		let mut client_listener = tcp::listen(&client_bind, Json::default).await?;
		client_listener.config_mut().max_frame_length(usize::MAX);
		let client_addr = client_listener.local_addr();

		let mut chunk_listener = tcp::listen(&chunk_bind, Json::default).await?;
		chunk_listener.config_mut().max_frame_length(usize::MAX);
		let chunk_addr = chunk_listener.local_addr();

		// One accept loop per service. Each accepted transport becomes a
		// tarpc channel; every in-flight request is spawned as its own task
		// so a slow request never blocks the channel.
		let client_master = self.clone();
		let client_task = tokio::spawn(async move {
			client_listener
				.filter_map(|r| future::ready(r.ok()))
				.map(BaseChannel::with_defaults)
				.for_each_concurrent(None, |channel| {
					let server = client_master.clone();
					channel.execute(ClientMaster::serve(server)).for_each(spawn)
				})
				.await;
		});

		let chunk_master = self.clone();
		let chunk_task = tokio::spawn(async move {
			chunk_listener
				.filter_map(|r| future::ready(r.ok()))
				.map(BaseChannel::with_defaults)
				.for_each_concurrent(None, |channel| {
					let server = chunk_master.clone();
					channel.execute(ChunkMaster::serve(server)).for_each(spawn)
				})
				.await;
		});

		Ok((client_addr, chunk_addr, client_task, chunk_task))
	}

	/// Background loop: mark servers offline once they miss
	/// `MISSED_HEARTBEATS` consecutive heartbeats. Runs forever; abort
	/// the spawned task (or drop the future) to stop it.
	pub async fn detect_timeouts(self) {
		let timeout = HEARTBEAT_INTERVAL * MISSED_HEARTBEATS;
		// Poll several times per heartbeat interval so a server is flagged
		// promptly once it crosses `timeout`, rather than up to a full
		// interval late (which a coarse, interval-aligned poll would risk).
		let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL / 4);
		loop {
			ticker.tick().await;
			let now = Instant::now();
			let mut state = self.0.lock().await;
			for info in state.servers.values_mut() {
				if info.online && now.duration_since(info.last_seen) >= timeout {
					info.online = false;
				}
			}
		}
	}
}

/// Drive one tarpc request future to completion on its own task. Used as
/// the per-request sink for each channel's `execute` stream.
async fn spawn(fut: impl std::future::Future<Output = ()> + Send + 'static) {
	tokio::spawn(fut);
}

// --------------------------------------------------------------------------
// ClientMaster impl
// --------------------------------------------------------------------------

impl ClientMaster for MasterServer {
	#[instrument(skip_all, fields(trace_id = %ctx.trace_context.trace_id, %path))]
	async fn allocate(
		self,
		ctx: Context,
		path: String,
		len: u64,
		chunk_size: u64,
	) -> Result<Vec<(ChunkId, ReplicaSet)>, MasterError> {
		if chunk_size == 0 {
			return Err(MasterError::InvalidChunkSize);
		}

		let mut state = self.0.lock().await;
		if state.files.contains_key(&path) {
			return Err(MasterError::AlreadyExists);
		}

		let online = state.online_server_ids();
		if online.len() < REPLICATION_FACTOR {
			return Err(MasterError::NotEnoughServers);
		}

		// Empty files occupy a namespace slot but own no chunks.
		let n_chunks = if len == 0 { 0 } else { len.div_ceil(chunk_size) };

		let mut chunk_ids = Vec::with_capacity(n_chunks as usize);
		let mut result = Vec::with_capacity(n_chunks as usize);
		for _ in 0..n_chunks {
			let chunk_id = state.next_chunk_id;
			state.next_chunk_id += 1;

			// Round-robin REPLICATION_FACTOR distinct servers, advancing the
			// cursor once per replica. With >= 2 online servers the picks
			// are always distinct.
			let mut replicas = Vec::with_capacity(REPLICATION_FACTOR);
			for _ in 0..REPLICATION_FACTOR {
				let sid = online[state.rr_cursor % online.len()];
				state.rr_cursor = state.rr_cursor.wrapping_add(1);
				replicas.push(sid);
			}

			let addrs: Vec<SocketAddr> = replicas.iter().map(|s| state.servers[s].addr).collect();
			let replica_set = ReplicaSet {
				primary: addrs[0],
				secondary: Some(addrs[1]),
			};

			state.chunks.insert(
				chunk_id,
				ChunkInfo {
					replicas,
					deleted: false,
				},
			);
			chunk_ids.push(chunk_id);
			result.push((chunk_id, replica_set));
		}

		state.files.insert(path, chunk_ids);
		Ok(result)
	}

	#[instrument(skip_all, fields(trace_id = %ctx.trace_context.trace_id, %path))]
	async fn lookup(
		self,
		ctx: Context,
		path: String,
	) -> Result<Vec<(ChunkId, ReplicaSet)>, MasterError> {
		let state = self.0.lock().await;
		let chunk_ids = state.files.get(&path).ok_or(MasterError::NotFound)?;

		let mut result = Vec::with_capacity(chunk_ids.len());
		for &cid in chunk_ids {
			let info = state.chunks.get(&cid).ok_or(MasterError::NoLiveReplicas)?;
			// Keep write order, drop offline replicas. The first survivor
			// becomes primary, so a downed primary is transparently replaced
			// by its secondary.
			let live: Vec<SocketAddr> = info
				.replicas
				.iter()
				.filter_map(|sid| state.servers.get(sid).filter(|s| s.online).map(|s| s.addr))
				.collect();
			let primary = *live.first().ok_or(MasterError::NoLiveReplicas)?;
			let secondary = live.get(1).copied();
			result.push((cid, ReplicaSet { primary, secondary }));
		}
		Ok(result)
	}

	#[instrument(skip_all, fields(trace_id = %ctx.trace_context.trace_id, %path))]
	async fn delete(self, ctx: Context, path: String) -> Result<(), MasterError> {
		let mut state = self.0.lock().await;
		let chunk_ids = state.files.remove(&path).ok_or(MasterError::NotFound)?;
		// Drop the bytes lazily: flag each chunk so heartbeat replies tell
		// its holders to delete it; the chunk is forgotten once they do.
		for cid in chunk_ids {
			if let Some(info) = state.chunks.get_mut(&cid) {
				info.deleted = true;
			}
		}
		Ok(())
	}
}

// --------------------------------------------------------------------------
// ChunkMaster impl
// --------------------------------------------------------------------------

impl ChunkMaster for MasterServer {
	#[instrument(skip_all, fields(trace_id = %ctx.trace_context.trace_id, %socket_addr))]
	async fn register(self, ctx: Context, socket_addr: SocketAddr) -> ServerId {
		let mut state = self.0.lock().await;
		let id = state.next_server_id;
		state.next_server_id += 1;
		state.servers.insert(
			id,
			ServerInfo {
				addr: socket_addr,
				last_seen: Instant::now(),
				online: true,
			},
		);
		id
	}

	#[instrument(skip_all, fields(trace_id = %ctx.trace_context.trace_id, %server_id))]
	async fn heartbeat(
		self,
		ctx: Context,
		server_id: ServerId,
		chunks_held: Vec<ChunkId>,
	) -> HeartbeatResponse {
		let held: HashSet<ChunkId> = chunks_held.iter().copied().collect();
		let mut state = self.0.lock().await;

		// A heartbeat is also the liveness signal; a server that was timed
		// out but is in fact alive rejoins here.
		if let Some(info) = state.servers.get_mut(&server_id) {
			info.last_seen = Instant::now();
			info.online = true;
		}

		// (a) Anything reported that the master no longer wants on this
		// server — unknown, deleted, or not assigned here — is dropped.
		let mut drop_chunks = Vec::new();
		for &cid in &chunks_held {
			let wanted = matches!(
				state.chunks.get(&cid),
				Some(info) if !info.deleted && info.replicas.contains(&server_id)
			);
			if !wanted {
				drop_chunks.push(cid);
			}
		}

		// (b) A deleted chunk this server was holding but no longer reports
		// has been dropped locally: remove this server from its replica
		// list, and forget the chunk once no replica remains.
		let mut forget = Vec::new();
		for (&cid, info) in state.chunks.iter_mut() {
			if info.deleted && !held.contains(&cid) && info.replicas.contains(&server_id) {
				info.replicas.retain(|&s| s != server_id);
				if info.replicas.is_empty() {
					forget.push(cid);
				}
			}
		}
		for cid in forget {
			state.chunks.remove(&cid);
		}

		HeartbeatResponse { drop_chunks }
	}
}

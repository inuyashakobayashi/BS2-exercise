//! Integration tests for gfs.
//!
//! Each test spawns a complete cluster (master + N chunk servers) in
//! the test process on OS-assigned ports, exercises it through the
//! public RPC and data-plane APIs, then lets `Drop` tear everything
//! down. All state is in-memory and per-test, so `cargo test`
//! parallelism is safe.
//!
//! The `Cluster` harness at the top of this file mirrors the logic
//! that lives in `src/main.rs` for the CLI client: allocate -> write
//! chain, lookup -> read per chunk, and a few direct data-plane
//! helpers the failure-path tests need.

use std::net::{Ipv6Addr, SocketAddr};
use std::time::Duration;

use tarpc::{client, context, serde_transport::tcp::connect, tokio_serde::formats::Json};
use tempfile::NamedTempFile;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use gfs::chunk::{ChunkServer, ChunkServerHandles};
use gfs::data::{DataRequest, DataResponse, read_frame, write_frame};
use gfs::master::MasterServer;
use gfs::{
	ChunkId, ClientMasterClient, HEARTBEAT_INTERVAL, MISSED_HEARTBEATS, MasterError, ReplicaSet,
};

// --------------------------------------------------------------------------
// Cluster harness
// --------------------------------------------------------------------------

struct ChunkNode {
	data_addr: SocketAddr,
	_wal: NamedTempFile,
	handles: Option<ChunkServerHandles>,
}

impl ChunkNode {
	async fn spawn(master_chunk_addr: SocketAddr) -> Self {
		let wal = NamedTempFile::new().expect("tempfile");
		let bind: SocketAddr = (Ipv6Addr::LOCALHOST, 0).into();
		let listener = TcpListener::bind(bind).await.expect("bind data listener");
		let data_addr = listener.local_addr().expect("local_addr");
		let server = ChunkServer::register(master_chunk_addr, data_addr, wal.path())
			.await
			.expect("register with master");
		let handles = server.spawn(listener);
		Self {
			data_addr,
			_wal: wal,
			handles: Some(handles),
		}
	}

	fn kill(&mut self) {
		if let Some(h) = self.handles.take() {
			h.abort();
		}
	}
}

struct Cluster {
	client: ClientMasterClient,
	servers: Vec<ChunkNode>,
	_client_listener: JoinHandle<()>,
	_chunk_listener: JoinHandle<()>,
	_timeout_task: JoinHandle<()>,
}

impl Cluster {
	async fn new(n_servers: usize) -> Self {
		let master = MasterServer::new();
		let timeout_task = tokio::spawn(master.clone().detect_timeouts());
		let bind: SocketAddr = (Ipv6Addr::LOCALHOST, 0).into();
		let (client_addr, chunk_addr, client_listener, chunk_listener) = master
			.spawn_listeners(bind, bind)
			.await
			.expect("bind master listeners");

		let transport = connect(client_addr, Json::default)
			.await
			.expect("dial master");
		let client = ClientMasterClient::new(client::Config::default(), transport).spawn();

		let mut servers = Vec::with_capacity(n_servers);
		for _ in 0..n_servers {
			servers.push(ChunkNode::spawn(chunk_addr).await);
		}

		Self {
			client,
			servers,
			_client_listener: client_listener,
			_chunk_listener: chunk_listener,
			_timeout_task: timeout_task,
		}
	}

	async fn allocate(
		&self,
		path: &str,
		len: u64,
		chunk_size: u64,
	) -> Result<Vec<(ChunkId, ReplicaSet)>, MasterError> {
		self.client
			.allocate(context::current(), path.to_string(), len, chunk_size)
			.await
			.expect("allocate RPC")
	}

	async fn lookup(&self, path: &str) -> Result<Vec<(ChunkId, ReplicaSet)>, MasterError> {
		self.client
			.lookup(context::current(), path.to_string())
			.await
			.expect("lookup RPC")
	}

	async fn delete(&self, path: &str) -> Result<(), MasterError> {
		self.client
			.delete(context::current(), path.to_string())
			.await
			.expect("delete RPC")
	}

	async fn put(&self, path: &str, data: &[u8], chunk_size: u64) {
		let len = data.len() as u64;
		let chunks = self
			.allocate(path, len, chunk_size)
			.await
			.expect("allocate");
		for (i, (chunk_id, replicas)) in chunks.iter().enumerate() {
			let start = i as u64 * chunk_size;
			let end = ((i as u64 + 1) * chunk_size).min(len);
			let payload = data[start as usize..end as usize].to_vec();
			write_chunk_chain(*chunk_id, replicas, payload)
				.await
				.expect("write_chunk");
		}
	}

	/// Download via lookup -> read-per-chunk. Falls back to `secondary`
	/// on any primary read error so a just-killed replica doesn't
	/// poison the read path before the timeout detector catches up.
	async fn get(&self, path: &str) -> Vec<u8> {
		let chunks = self.lookup(path).await.expect("lookup");
		let mut out = Vec::new();
		for (chunk_id, replicas) in &chunks {
			let bytes = read_chunk_with_fallback(replicas, *chunk_id)
				.await
				.expect("read_chunk");
			out.extend_from_slice(&bytes);
		}
		out
	}
}

async fn write_chunk_chain(
	chunk_id: ChunkId,
	replicas: &ReplicaSet,
	payload: Vec<u8>,
) -> Result<(), String> {
	let mut stream = TcpStream::connect(replicas.primary)
		.await
		.map_err(|e| format!("connect primary {}: {e}", replicas.primary))?;
	let req = DataRequest::Write {
		chunk_id,
		forward_to: replicas.secondary,
		payload,
	};
	write_frame(&mut stream, &req)
		.await
		.map_err(|e| format!("write_frame: {e}"))?;
	match read_frame::<_, DataResponse>(&mut stream)
		.await
		.map_err(|e| format!("read_frame: {e}"))?
	{
		DataResponse::Written => Ok(()),
		DataResponse::Error(msg) => Err(msg),
		DataResponse::NotFound => Err("unexpected NotFound for Write".into()),
		DataResponse::Data(_) => Err("unexpected Data for Write".into()),
	}
}

async fn read_chunk_from(addr: SocketAddr, chunk_id: ChunkId) -> DataResponse {
	let mut stream = match TcpStream::connect(addr).await {
		Ok(s) => s,
		Err(e) => return DataResponse::Error(format!("connect {addr}: {e}")),
	};
	let req = DataRequest::Read { chunk_id };
	if let Err(e) = write_frame(&mut stream, &req).await {
		return DataResponse::Error(format!("write_frame: {e}"));
	}
	match read_frame::<_, DataResponse>(&mut stream).await {
		Ok(r) => r,
		Err(e) => DataResponse::Error(format!("read_frame: {e}")),
	}
}

/// Issue a byte-range read (bonus data-plane extension) and return the
/// returned slice, panicking on any non-`Data` reply.
async fn read_range(addr: SocketAddr, chunk_id: ChunkId, offset: u64, len: u64) -> Vec<u8> {
	let mut stream = TcpStream::connect(addr).await.expect("connect");
	let req = DataRequest::ReadRange {
		chunk_id,
		offset,
		len,
	};
	write_frame(&mut stream, &req).await.expect("write_frame");
	match read_frame::<_, DataResponse>(&mut stream)
		.await
		.expect("read_frame")
	{
		DataResponse::Data(b) => b,
		other => panic!("expected Data, got {other:?}"),
	}
}

async fn read_chunk_with_fallback(
	replicas: &ReplicaSet,
	chunk_id: ChunkId,
) -> Result<Vec<u8>, String> {
	match read_chunk_from(replicas.primary, chunk_id).await {
		DataResponse::Data(b) => Ok(b),
		other => {
			let Some(sec) = replicas.secondary else {
				return Err(format!("primary {}: {other:?}", replicas.primary));
			};
			match read_chunk_from(sec, chunk_id).await {
				DataResponse::Data(b) => Ok(b),
				sec_resp => Err(format!(
					"primary {}: {other:?}; secondary {sec}: {sec_resp:?}",
					replicas.primary
				)),
			}
		}
	}
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[tokio::test]
async fn put_get_roundtrip_small() {
	let cluster = Cluster::new(2).await;
	let data = b"hello gfs".to_vec();
	cluster.put("/small", &data, 1024).await;
	assert_eq!(cluster.get("/small").await, data);
}

#[tokio::test]
async fn put_get_roundtrip_multi_chunk() {
	let cluster = Cluster::new(2).await;
	let data: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
	cluster.put("/multi", &data, 1024).await;
	// ceil(3000 / 1024) = 3 chunks
	let chunks = cluster.lookup("/multi").await.unwrap();
	assert_eq!(chunks.len(), 3);
	assert_eq!(cluster.get("/multi").await, data);
}

#[tokio::test]
async fn secondary_crash_during_write() {
	let mut cluster = Cluster::new(2).await;
	let chunks = cluster.allocate("/sec-crash", 512, 1024).await.unwrap();
	assert_eq!(chunks.len(), 1);
	let (chunk_id, replicas) = chunks[0].clone();
	let secondary = replicas.secondary.expect("allocate returns two replicas");

	let idx = cluster
		.servers
		.iter()
		.position(|n| n.data_addr == secondary)
		.expect("secondary addr belongs to a server");
	cluster.servers[idx].kill();
	// Let the aborted listener actually close.
	tokio::time::sleep(Duration::from_millis(50)).await;

	let result = write_chunk_chain(chunk_id, &replicas, vec![0xAB; 512]).await;
	assert!(
		result.is_err(),
		"chain-replicated write should fail when secondary is down"
	);
}

#[tokio::test]
async fn primary_crash_between_allocate_and_write() {
	let mut cluster = Cluster::new(2).await;
	let chunks = cluster.allocate("/prim-crash", 256, 1024).await.unwrap();
	let (chunk_id, replicas) = chunks[0].clone();
	let primary = replicas.primary;

	let idx = cluster
		.servers
		.iter()
		.position(|n| n.data_addr == primary)
		.expect("primary addr belongs to a server");
	cluster.servers[idx].kill();
	tokio::time::sleep(Duration::from_millis(50)).await;

	let result = write_chunk_chain(chunk_id, &replicas, vec![0xCD; 256]).await;
	assert!(result.is_err(), "write should fail when primary is down");
}

#[tokio::test]
async fn heartbeat_timeout_marks_offline() {
	let mut cluster = Cluster::new(2).await;
	cluster.put("/timeout", b"payload", 1024).await;

	cluster.servers[0].kill();
	tokio::time::sleep(HEARTBEAT_INTERVAL * MISSED_HEARTBEATS + Duration::from_millis(500)).await;

	let chunks = cluster.lookup("/timeout").await.unwrap();
	assert_eq!(chunks.len(), 1);
	let (_, replicas) = &chunks[0];
	assert!(
		replicas.secondary.is_none(),
		"timed-out replica should be filtered out of lookup"
	);
}

#[tokio::test]
async fn byte_range_read() {
	let cluster = Cluster::new(2).await;
	let data: Vec<u8> = (0..500u32).map(|i| (i % 251) as u8).collect();
	// One chunk (chunk_size > len) keeps the offsets aligned with `data`.
	cluster.put("/ranged", &data, 4096).await;
	let chunks = cluster.lookup("/ranged").await.unwrap();
	assert_eq!(chunks.len(), 1);
	let (chunk_id, replicas) = chunks[0].clone();

	// A range strictly inside the chunk returns exactly that slice.
	assert_eq!(
		read_range(replicas.primary, chunk_id, 100, 50).await,
		data[100..150]
	);
	// A range running past the end is clamped to the chunk length.
	assert_eq!(
		read_range(replicas.primary, chunk_id, 480, 100).await,
		data[480..500]
	);
	// An offset at/after the end yields an empty slice, not an error.
	assert!(read_range(replicas.primary, chunk_id, 500, 10).await.is_empty());
}

#[tokio::test]
async fn delete_propagates_via_heartbeat() {
	let cluster = Cluster::new(2).await;
	cluster.put("/doomed", b"bye", 1024).await;
	let chunks = cluster.lookup("/doomed").await.unwrap();
	let (chunk_id, replicas) = chunks[0].clone();
	let primary = replicas.primary;

	cluster.delete("/doomed").await.unwrap();
	// Lazy GC takes one heartbeat to deliver `drop_chunks` and one more
	// for the master to observe the chunk's absence and prune.
	tokio::time::sleep(HEARTBEAT_INTERVAL * 3).await;

	let resp = read_chunk_from(primary, chunk_id).await;
	assert!(
		matches!(resp, DataResponse::NotFound),
		"expected NotFound after lazy GC, got {resp:?}"
	);
	assert_eq!(cluster.lookup("/doomed").await, Err(MasterError::NotFound));
}

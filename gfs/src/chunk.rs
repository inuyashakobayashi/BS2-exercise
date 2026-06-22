//! Chunk-server implementation: in-memory store, TCP data plane,
//! heartbeat loop.
//!
//! A chunk server plays two roles simultaneously. Over tarpc it is a
//! *client* of the master's [`ChunkMaster`](crate::ChunkMaster) service.
//! It calls [`register`](crate::ChunkMaster::register) once at
//! startup, then emits a [`heartbeat`](crate::ChunkMaster::heartbeat)
//! every [`HEARTBEAT_INTERVAL`]. Over a raw TCP data plane it is a
//! *server* that handles [`DataRequest`] frames from clients and peer
//! replicas.
//!
//! Chunk data is held in a [`Store<ChunkId, Vec<u8>>`] behind [`Mutex`].
//! A chunk-server restart loses all stored chunks.

#![allow(unused_imports)]

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};

use tarpc::serde_transport::tcp::connect;
use tarpc::tokio_serde::formats::Json;
use tarpc::{client, context};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::data::{DataRequest, DataResponse, read_frame, write_frame};
use crate::store::Store;
use crate::{ChunkId, ChunkMasterClient, HEARTBEAT_INTERVAL, ServerId};

// --------------------------------------------------------------------------
// Server handle
// --------------------------------------------------------------------------

/// Runtime handle to a chunk server. Cheaply cloneable: the store and
/// the tarpc client are shared across tasks via `Arc`.
#[derive(Clone)]
pub struct ChunkServer {
	/// Master-assigned identifier, sent with every heartbeat.
	server_id: ServerId,
	/// Externally visible data-plane address; reported to the master at
	/// registration and handed out in [`ReplicaSet`](crate::ReplicaSet)s.
	data_addr: SocketAddr,
	/// In-memory chunk store, shared across the data-plane and heartbeat
	/// tasks. A restart loses everything here.
	store: Arc<Mutex<Store<ChunkId, Vec<u8>>>>,
	/// tarpc client used to call back into the master's `ChunkMaster`
	/// service (registration and heartbeats).
	master: ChunkMasterClient,
}

impl ChunkServer {
	/// Open the in-memory store, dial the master at `master_addr`,
	/// register our data-plane address, and return a handle ready for
	/// [`serve`](Self::serve).
	///
	/// `_path` is accepted for call-site stability but ignored.
	/// The store holds no data on disk.
	///
	/// `data_addr` must be the externally visible address the chunk
	/// server will actually listen on as it is what the master hands out
	/// in [`ReplicaSet`]s.
	pub async fn register(
		master_addr: SocketAddr,
		data_addr: SocketAddr,
		_path: &Path,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
		// [zh] chunk-server 启动三步走：开内存 store -> 用 tarpc 连到 master(控制平面，
		// [zh] Json 编码) -> 调 register 拿到自己的 ServerId。这台机器对 master 来说是
		// [zh] “客户端”，对真正的客户端来说是“数据服务器”——身兼两角。
		// The in-memory store never fails to open; `_path` is ignored.
		let store = Store::open(_path).expect("in-memory store open is infallible");

		let transport = connect(master_addr, Json::default).await?;
		let master = ChunkMasterClient::new(client::Config::default(), transport).spawn();

		let server_id = master.register(context::current(), data_addr).await?;
		info!(%server_id, %data_addr, "registered with master");

		Ok(Self {
			server_id,
			data_addr,
			store: Arc::new(Mutex::new(store)),
			master,
		})
	}

	/// Spawn the heartbeat and data-plane loops as independent tokio
	/// tasks and return handles to them. Callers that just want to run
	/// forever use [`serve`](Self::serve); integration tests keep the
	/// handles so they can [`abort`](ChunkServerHandles::abort) a chunk
	/// server mid-flight.
	// [zh] 两个独立任务同时跑：心跳循环(控制平面，定期上报+执行删除指令) 和
	// [zh] 数据循环(数据平面，处理客户端/对端的 chunk 读写)。返回句柄方便测试随时 abort。
	pub fn spawn(self, listener: TcpListener) -> ChunkServerHandles {
		let hb = self.clone();
		let heartbeat = tokio::spawn(hb.heartbeat_loop());
		let data = tokio::spawn(self.data_loop(listener));
		ChunkServerHandles { heartbeat, data }
	}

	/// Run forever: spawn the loops and join them. Returns only when
	/// one of the tasks exits (which, in steady state, only happens on
	/// panic or shutdown).
	pub async fn serve(self, listener: TcpListener) {
		self.spawn(listener).join().await;
	}

	// ----------------------------------------------------------------------
	// Loops
	// ----------------------------------------------------------------------

	/// Emit a heartbeat every [`HEARTBEAT_INTERVAL`] carrying the full set
	/// of locally-held chunk IDs (the master's chunk report) and act on any
	/// `drop_chunks` the master returns by deleting them locally.
	async fn heartbeat_loop(self) {
		let mut ticker = tokio::time::interval(HEARTBEAT_INTERVAL);
		loop {
			ticker.tick().await;

			// [zh] 把本地 store 里当前所有 chunk 的 ID 收集起来，作为“chunk 报告”发给
			// [zh] master。注意：锁是 std::sync::Mutex，收集完必须在 .await 前放掉锁
			// [zh] (这个 {} 块结束就 drop)，否则跨 await 持锁会出问题。
			let chunks_held: Vec<ChunkId> = {
				let store = self.store.lock().expect("store mutex poisoned");
				store.scan(..).map(|(&id, _)| id).collect()
			};

			match self
				.master
				.heartbeat(context::current(), self.server_id, chunks_held)
				.await
			{
				// [zh] master 在心跳回复里捎回 drop_chunks(要本地删的 chunk)，照做即可。
				// [zh] 这是 master->chunk-server 的唯一指挥通道(没有专门的删除 RPC)。
				Ok(resp) => {
					if !resp.drop_chunks.is_empty() {
						let mut store = self.store.lock().expect("store mutex poisoned");
						for cid in resp.drop_chunks {
							debug!(chunk_id = cid, "dropping chunk on master command");
							let _ = store.delete(&cid);
						}
					}
				}
				Err(e) => warn!(error = %e, "heartbeat failed"),
			}
		}
	}

	/// Accept data-plane connections forever, handling each on its own task.
	async fn data_loop(self, listener: TcpListener) {
		info!(data_addr = %self.data_addr, "serving data plane");
		// [zh] 数据平面是裸 TCP(不走 tarpc)，因为 chunk 内容可能很大，JSON-RPC 编码扛不住。
		// [zh] 每来一个连接就 spawn 一个任务去处理，互不阻塞。
		loop {
			match listener.accept().await {
				Ok((stream, peer)) => {
					let server = self.clone();
					tokio::spawn(async move {
						if let Err(e) = server.handle_conn(stream).await {
							debug!(%peer, error = %e, "data connection ended with error");
						}
					});
				}
				Err(e) => error!(error = %e, "accept failed"),
			}
		}
	}

	// ----------------------------------------------------------------------
	// Data-plane request handling
	// ----------------------------------------------------------------------

	/// Serve framed [`DataRequest`]s on one connection until the peer
	/// closes it. Clients open one connection per request, but looping
	/// keeps a reused connection working too.
	async fn handle_conn(&self, mut stream: TcpStream) -> io::Result<()> {
		loop {
			let req: DataRequest = match read_frame(&mut stream).await {
				Ok(req) => req,
				Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
				Err(e) => return Err(e),
			};
			let resp = self.handle_request(req).await;
			write_frame(&mut stream, &resp).await?;
		}
	}

	/// Apply one request to the local store. A `Write` carrying
	/// `forward_to` is a chain-replicated write: store locally, then
	/// forward to the secondary before acknowledging, so the ack means the
	/// data reached both replicas.
	async fn handle_request(&self, req: DataRequest) -> DataResponse {
		match req {
			// [zh] 写：这是链式复制(client -> 主 -> 从)。本节点先存到本地，再看 forward_to：
			// [zh] 如果是 Some(从副本地址)，说明自己是主副本，必须把同样的数据转发给从副本、
			// [zh] 等从副本确认后才回 Written。从副本收到的请求 forward_to 是 None，存完直接回。
			// [zh] 转发失败就回 Error(测试里把从副本杀掉，写就会失败)。
			DataRequest::Write {
				chunk_id,
				forward_to,
				payload,
			} => {
				{
					let mut store = self.store.lock().expect("store mutex poisoned");
					if let Err(e) = store.set(chunk_id, payload.clone()) {
						return DataResponse::Error(format!("store: {e}"));
					}
				}
				match forward_to {
					Some(addr) => match self.forward(addr, chunk_id, payload).await {
						Ok(()) => DataResponse::Written,
						Err(e) => DataResponse::Error(format!("forward to {addr}: {e}")),
					},
					None => DataResponse::Written,
				}
			}
			DataRequest::Read { chunk_id } => {
				let bytes = {
					let store = self.store.lock().expect("store mutex poisoned");
					store.get(&chunk_id).cloned()
				};
				match bytes {
					Some(bytes) => DataResponse::Data(bytes),
					None => DataResponse::NotFound,
				}
			}
			// [zh] 【Bonus】按字节区间读：不传整块，只切出 [offset, offset+len) 这一段返回。
			// [zh] 区间会被钳制到 chunk 实际长度，越界就少返回甚至返回空，而不是报错。
			DataRequest::ReadRange {
				chunk_id,
				offset,
				len,
			} => {
				let slice = {
					let store = self.store.lock().expect("store mutex poisoned");
					store.get(&chunk_id).map(|full| {
						// Clamp the range to the chunk so an over-long or
						// out-of-bounds request returns fewer bytes (possibly
						// none) instead of failing.
						let start = (offset as usize).min(full.len());
						let end = start.saturating_add(len as usize).min(full.len());
						full[start..end].to_vec()
					})
				};
				match slice {
					Some(slice) => DataResponse::Data(slice),
					None => DataResponse::NotFound,
				}
			}
		}
	}

	/// Forward a payload down the replication chain to the secondary
	/// (`forward_to: None`, so it stores without forwarding further) and
	/// require its `Written` ack.
	async fn forward(
		&self,
		addr: SocketAddr,
		chunk_id: ChunkId,
		payload: Vec<u8>,
	) -> io::Result<()> {
		// [zh] 主副本转发给从副本：自己再开一条 TCP，发同样的 Write 但 forward_to=None
		// [zh] (从副本不再往下转)，并要求它回 Written 确认，确认后整条链才算写成功。
		let mut stream = TcpStream::connect(addr).await?;
		let req = DataRequest::Write {
			chunk_id,
			forward_to: None,
			payload,
		};
		write_frame(&mut stream, &req).await?;
		match read_frame::<_, DataResponse>(&mut stream).await? {
			DataResponse::Written => Ok(()),
			DataResponse::Error(msg) => Err(io::Error::other(msg)),
			other => Err(io::Error::other(format!("unexpected reply: {other:?}"))),
		}
	}
}

// --------------------------------------------------------------------------
// Task handles
// --------------------------------------------------------------------------

/// Join handles for the heartbeat and data-plane loops spawned by
/// [`ChunkServer::spawn`]. [`abort`](Self::abort) stops both tasks;
/// [`join`](Self::join) runs them to completion.
pub struct ChunkServerHandles {
	pub heartbeat: tokio::task::JoinHandle<()>,
	pub data: tokio::task::JoinHandle<()>,
}

impl ChunkServerHandles {
	/// Abort both loops.
	///
	/// The chunk server stops answering data-plane connections and
	/// stops sending heartbeats. From the master's point of view this
	/// is indistinguishable from a process crash.
	pub fn abort(&self) {
		self.heartbeat.abort();
		self.data.abort();
	}

	/// Wait for both loops to finish. Either task ending normally is
	/// fine; `JoinError`s (panics, aborts) are swallowed.
	pub async fn join(self) {
		let _ = tokio::join!(self.heartbeat, self.data);
	}
}

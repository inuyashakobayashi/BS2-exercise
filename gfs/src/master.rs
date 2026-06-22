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

// [zh] Master 的全部状态都在这一个结构里，外面用 Arc<Mutex<State>> 包起来。
// [zh] 三张表（路径->chunk、chunk->元数据、server->信息）+ 两个自增 ID + 一个
// [zh] 轮询游标，全在内存。master 一重启这些就全没了（GFS 里 master 会持久化，
// [zh] 这里为了简化省掉了）。
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
		// [zh] 控制平面有两套 RPC 服务（面向客户端的 ClientMaster、面向 chunk-server
		// [zh] 的 ChunkMaster），所以开两个 tarpc 监听器，各跑一个 accept 循环。
		// [zh] tcp::listen 拿到监听器后先取 local_addr（端口传 0 时由 OS 分配，要回报
		// [zh] 给调用方），max_frame_length 放大以防元数据帧（如 chunk 列表）超限。
		let mut client_listener = tcp::listen(&client_bind, Json::default).await?;
		client_listener.config_mut().max_frame_length(usize::MAX);
		let client_addr = client_listener.local_addr();

		let mut chunk_listener = tcp::listen(&chunk_bind, Json::default).await?;
		chunk_listener.config_mut().max_frame_length(usize::MAX);
		let chunk_addr = chunk_listener.local_addr();

		// One accept loop per service. Each accepted transport becomes a
		// tarpc channel; every in-flight request is spawned as its own task
		// so a slow request never blocks the channel.
		// [zh] accept 循环：每个进来的连接(filter_map 丢掉出错的)变成一个 tarpc
		// [zh] channel，channel.execute(...) 产出一串“每个请求一个 future”，再交给
		// [zh] spawn 各自 tokio::spawn，这样一个慢请求不会卡住整条连接。
		// [zh] 关键坑：MasterServer 同时实现了两个服务 trait，直接 .serve() 有歧义，
		// [zh] 必须写成 ClientMaster::serve(server) / ChunkMaster::serve(server)。
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
		// [zh] 超时阈值 = 漏掉 MISSED_HEARTBEATS(3) 个心跳 = 3 秒没收到心跳就判离线。
		// [zh] 这里每 1/4 个心跳周期(250ms)轮询一次，比“每秒一次”更细——否则轮询点和
		// [zh] 3 秒阈值正好对齐时，会出现“真正抓到离线”的那次 tick 落到第 4 秒，导致
		// [zh] 只给 3.5 秒窗口的那个超时测试卡边界失败。
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
		// [zh] allocate = 客户端写文件前问 master 要 chunk。先做三道校验：
		// [zh] chunk_size 不能为 0；路径不能已存在（没有客户端缓存，每次都过 master）；
		// [zh] 在线 server 数必须 >= 副本数(2)，否则没法放够副本。
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

		// [zh] chunk 数 = 向上取整(len / chunk_size)。空文件占个名字但没有 chunk。
		// Empty files occupy a namespace slot but own no chunks.
		let n_chunks = if len == 0 { 0 } else { len.div_ceil(chunk_size) };

		// [zh] 给每个 chunk 发一个全局唯一 ID，并用 round-robin 选 2 个副本。
		// [zh] 游标“每选一个副本就 +1”（不是每个 chunk +1），这样副本能均匀铺开；
		// [zh] 在线 server >= 2 时，连续取的两个下标必然不同 -> 主从落在不同机器上。
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

			// [zh] 分配时就把“期望的副本”记进元数据（spec 要求 sofort hinterlegt）。
			// [zh] 之后 chunk-server 心跳上报这个 chunk，就算作“已写入确认”。
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
		// [zh] lookup = 读文件前问 master 要副本地址。逐个 chunk 把离线副本过滤掉，
		// [zh] 保持写入顺序，所以幸存者里第一个当 primary：主副本挂了，从副本自动顶上
		// [zh] 成为 primary（客户端读写路径因此不用关心是谁挂了）。某个 chunk 一个活
		// [zh] 副本都不剩 -> 整个 lookup 报 NoLiveReplicas。
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
		// [zh] 删除是“惰性”的：master 这里只把路径从命名空间删掉、给 chunk 打上
		// [zh] deleted 标记；真正删字节要等下一次心跳，master 在心跳回复里让持有者删。
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
		// [zh] chunk-server 启动时调一次：master 发一个唯一 ServerId 并记下它的数据平面
		// [zh] 地址(后续 lookup/allocate 就把这个地址给客户端)。之后心跳只带 ServerId。
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
		// [zh] 心跳是 chunk-server -> master 的唯一通道：它带来这台机器“当前持有的全部
		// [zh] chunk 列表”(chunks_held)，master 拿它跟自己的元数据做差，推断出一切——
		// [zh] 谁写成功了、谁该删、谁掉线了。chunk-server 不会单独发“我写好了/删好了”。
		let held: HashSet<ChunkId> = chunks_held.iter().copied().collect();
		let mut state = self.0.lock().await;

		// [zh] 收到心跳同时刷新存活时间；之前被判离线但其实还活着的机器在这里“复活”。
		// A heartbeat is also the liveness signal; a server that was timed
		// out but is in fact alive rejoins here.
		if let Some(info) = state.servers.get_mut(&server_id) {
			info.last_seen = Instant::now();
			info.online = true;
		}

		// [zh] (a) 上报里凡是 master “不想要它在这台机器上”的，都塞进 drop_chunks
		// [zh] 让它本地删除：包括 master 根本不认识的 chunk、已被标记删除的 chunk、
		// [zh] 以及不该分配在这台机器上的 chunk。这是幂等的——只要它下次心跳还报，
		// [zh] master 就再发一次删除指令，直到它不再报为止。
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

		// [zh] (b) 反过来：一个已标记删除、且这台机器“本该持有但这次不再上报”的 chunk，
		// [zh] 说明它已经在本地删掉了 -> 把这台机器从该 chunk 的副本表里去掉；副本表空了
		// [zh] 就彻底忘掉这个 chunk。所以 delete 后要两次心跳：第一次下发删除指令，第二
		// [zh] 次观察到不再上报、才真正清元数据（对应那个 delete 测试 sleep 了 3 个周期）。
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

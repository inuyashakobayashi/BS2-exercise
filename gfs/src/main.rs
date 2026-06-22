//! Interactive REPL CLI for `gfs`.
//!
//! `cargo run` drops into a prompt that owns a single in-process master
//! server plus any number of chunk servers spawned on demand, and runs
//! `put` / `get` / `delete` against them. Master and chunks share the
//! REPL's tokio runtime and tracing subscriber, so tracing output
//! appears in the same terminal.
//!
//! The standalone `master_server` and `chunk_server` binaries remain
//! available for setups that need the components in separate processes.

use std::net::{Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use tarpc::{client, context, serde_transport::tcp::connect, tokio_serde::formats::Json};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

use gfs::chunk::{ChunkServer, ChunkServerHandles};
use gfs::data::{DataRequest, DataResponse, read_frame, write_frame};
use gfs::master::MasterServer;
use gfs::{CHUNK_SIZE, ClientMasterClient, MasterError, ReplicaSet};

// --------------------------------------------------------------------------
// REPL state
// --------------------------------------------------------------------------

struct Repl {
	master: Option<MasterCtx>,
	chunks: Vec<ChunkCtx>,
}

struct MasterCtx {
	client: ClientMasterClient,
	client_master_addr: SocketAddr,
	chunk_master_addr: SocketAddr,
	_client_task: JoinHandle<()>,
	_chunk_task: JoinHandle<()>,
	_timeout_task: JoinHandle<()>,
}

struct ChunkCtx {
	data_addr: SocketAddr,
	handles: Option<ChunkServerHandles>,
}

impl Repl {
	fn new() -> Self {
		Self {
			master: None,
			chunks: Vec::new(),
		}
	}

	fn shutdown(&mut self) {
		for c in self.chunks.drain(..) {
			if let Some(h) = c.handles {
				h.abort();
			}
		}
		if let Some(m) = self.master.take() {
			m._client_task.abort();
			m._chunk_task.abort();
			m._timeout_task.abort();
		}
	}
}

// --------------------------------------------------------------------------
// Command parsing
// --------------------------------------------------------------------------

enum Cmd {
	Master {
		client_port: u16,
		chunk_port: u16,
	},
	Chunk {
		data_port: u16,
	},
	Chunks,
	Kill {
		index: usize,
	},
	Status,
	Put {
		local: PathBuf,
		remote: String,
		chunk_size: u64,
	},
	Get {
		remote: String,
		local: PathBuf,
	},
	Delete {
		remote: String,
	},
	Help,
	Quit,
	Empty,
}

fn parse_line(line: &str) -> Result<Cmd, String> {
	let mut toks = line.split_whitespace();
	let head = match toks.next() {
		Some(h) => h,
		None => return Ok(Cmd::Empty),
	};
	let rest: Vec<&str> = toks.collect();
	match head {
		"master" => parse_master(&rest),
		"chunk" => parse_chunk(&rest),
		"chunks" => parse_no_args(&rest, "chunks", Cmd::Chunks),
		"kill" => parse_kill(&rest),
		"status" => parse_no_args(&rest, "status", Cmd::Status),
		"put" => parse_put(&rest),
		"get" => parse_get(&rest),
		"delete" => parse_delete(&rest),
		"help" | "?" => Ok(Cmd::Help),
		"quit" | "exit" | "q" => Ok(Cmd::Quit),
		other => Err(format!("unknown command `{other}`; type `help`")),
	}
}

fn parse_no_args(rest: &[&str], name: &str, cmd: Cmd) -> Result<Cmd, String> {
	if !rest.is_empty() {
		return Err(format!("{name}: unexpected argument `{}`", rest[0]));
	}
	Ok(cmd)
}

fn parse_master(rest: &[&str]) -> Result<Cmd, String> {
	let mut client_port: u16 = 0;
	let mut chunk_port: u16 = 0;
	let mut i = 0;
	while i < rest.len() {
		match rest[i] {
			"--client-port" => {
				let v = rest
					.get(i + 1)
					.ok_or_else(|| "master: --client-port requires <port>".to_string())?;
				client_port = v
					.parse()
					.map_err(|_| format!("master: invalid port `{v}`"))?;
				i += 2;
			}
			"--chunk-port" => {
				let v = rest
					.get(i + 1)
					.ok_or_else(|| "master: --chunk-port requires <port>".to_string())?;
				chunk_port = v
					.parse()
					.map_err(|_| format!("master: invalid port `{v}`"))?;
				i += 2;
			}
			other => return Err(format!("master: unexpected `{other}`")),
		}
	}
	Ok(Cmd::Master {
		client_port,
		chunk_port,
	})
}

fn parse_kill(rest: &[&str]) -> Result<Cmd, String> {
	if rest.len() != 1 {
		return Err("usage: kill <chunk-index>".to_string());
	}
	let index = rest[0]
		.parse()
		.map_err(|_| format!("kill: invalid index `{}`", rest[0]))?;
	Ok(Cmd::Kill { index })
}

fn parse_chunk(rest: &[&str]) -> Result<Cmd, String> {
	let mut data_port: u16 = 0;
	let mut i = 0;
	while i < rest.len() {
		match rest[i] {
			"--data-port" => {
				let v = rest
					.get(i + 1)
					.ok_or_else(|| "chunk: --data-port requires <port>".to_string())?;
				data_port = v
					.parse()
					.map_err(|_| format!("chunk: invalid port `{v}`"))?;
				i += 2;
			}
			other => return Err(format!("chunk: unexpected `{other}`")),
		}
	}
	Ok(Cmd::Chunk { data_port })
}

fn parse_put(rest: &[&str]) -> Result<Cmd, String> {
	let mut positional: Vec<&str> = Vec::new();
	let mut chunk_size: u64 = CHUNK_SIZE;
	let mut i = 0;
	while i < rest.len() {
		match rest[i] {
			"--chunk-size" => {
				let v = rest
					.get(i + 1)
					.ok_or_else(|| "put: --chunk-size requires <bytes>".to_string())?;
				chunk_size = v
					.parse()
					.map_err(|_| format!("put: invalid chunk size `{v}`"))?;
				if chunk_size == 0 {
					return Err("put: chunk size must be > 0".to_string());
				}
				i += 2;
			}
			other if other.starts_with("--") => {
				return Err(format!("put: unexpected flag `{other}`"));
			}
			other => {
				positional.push(other);
				i += 1;
			}
		}
	}
	if positional.len() != 2 {
		return Err("usage: put <local> <remote> [--chunk-size <bytes>]".to_string());
	}
	Ok(Cmd::Put {
		local: PathBuf::from(positional[0]),
		remote: positional[1].to_string(),
		chunk_size,
	})
}

fn parse_get(rest: &[&str]) -> Result<Cmd, String> {
	if rest.len() != 2 {
		return Err("usage: get <remote> <local>".to_string());
	}
	Ok(Cmd::Get {
		remote: rest[0].to_string(),
		local: PathBuf::from(rest[1]),
	})
}

fn parse_delete(rest: &[&str]) -> Result<Cmd, String> {
	if rest.len() != 1 {
		return Err("usage: delete <remote>".to_string());
	}
	Ok(Cmd::Delete {
		remote: rest[0].to_string(),
	})
}

// --------------------------------------------------------------------------
// Entry point
// --------------------------------------------------------------------------

#[tokio::main]
async fn main() -> ExitCode {
	tracing_subscriber::fmt()
		.with_env_filter(
			EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| EnvFilter::new("info,gfs=debug,tarpc=warn")),
		)
		.without_time()
		.init();

	println!("gfs interactive REPL — type `help` for commands");

	let mut repl = Repl::new();
	let mut lines = BufReader::new(tokio::io::stdin()).lines();
	let mut stdout = tokio::io::stdout();

	loop {
		let _ = stdout.write_all(b"gfs> ").await;
		let _ = stdout.flush().await;

		let line = tokio::select! {
			_ = tokio::signal::ctrl_c() => {
				println!();
				break;
			}
			res = lines.next_line() => match res {
				Ok(Some(s)) => s,
				Ok(None) => {
					println!();
					break;
				}
				Err(e) => {
					eprintln!("stdin: {e}");
					break;
				}
			}
		};

		match parse_line(&line) {
			Ok(Cmd::Empty) => {}
			Ok(Cmd::Quit) => break,
			Ok(Cmd::Help) => print_help(),
			Ok(cmd) => {
				if let Err(e) = dispatch(&mut repl, cmd).await {
					eprintln!("error: {e}");
				}
			}
			Err(e) => eprintln!("error: {e}"),
		}
	}

	repl.shutdown();
	ExitCode::SUCCESS
}

fn print_help() {
	println!(
		"commands:
  master [--client-port <n>] [--chunk-port <n>]   bring up the in-process master (once)
  chunk  [--data-port <n>]                        spawn one chunk server
  chunks                                          list spawned chunk servers
  kill <index>                                    abort a chunk server
  status                                          show master endpoints + chunk count
  put    <local> <remote> [--chunk-size <bytes>]  upload <local> file, save as <remote>
  get    <remote> <local>                         download <remote> file, save as <local>
  delete <remote>                                 remove <remote> file
  help | ?                                        this list
  quit | exit | q                                 leave the REPL (Ctrl-D works too)"
	);
}

// --------------------------------------------------------------------------
// Dispatch
// --------------------------------------------------------------------------

async fn dispatch(repl: &mut Repl, cmd: Cmd) -> Result<(), Box<dyn std::error::Error>> {
	match cmd {
		Cmd::Master {
			client_port,
			chunk_port,
		} => cmd_master(repl, client_port, chunk_port).await,
		Cmd::Chunk { data_port } => cmd_chunk(repl, data_port).await,
		Cmd::Chunks => {
			cmd_chunks(repl);
			Ok(())
		}
		Cmd::Kill { index } => cmd_kill(repl, index),
		Cmd::Status => {
			cmd_status(repl);
			Ok(())
		}
		Cmd::Put {
			local,
			remote,
			chunk_size,
		} => cmd_put(require_master(repl)?, &local, remote, chunk_size).await,
		Cmd::Get { remote, local } => cmd_get(require_master(repl)?, remote, &local).await,
		Cmd::Delete { remote } => cmd_delete(require_master(repl)?, remote).await,
		Cmd::Empty | Cmd::Help | Cmd::Quit => unreachable!(),
	}
}

fn require_master(repl: &Repl) -> Result<&ClientMasterClient, Box<dyn std::error::Error>> {
	repl.master
		.as_ref()
		.map(|m| &m.client)
		.ok_or_else(|| "master not running; run `master` first".into())
}

// --------------------------------------------------------------------------
// Cluster commands
// --------------------------------------------------------------------------

async fn cmd_master(
	repl: &mut Repl,
	client_port: u16,
	chunk_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
	if repl.master.is_some() {
		return Err("master already running".into());
	}
	let client_bind: SocketAddr = (Ipv6Addr::LOCALHOST, client_port).into();
	let chunk_bind: SocketAddr = (Ipv6Addr::LOCALHOST, chunk_port).into();

	let server = MasterServer::new();
	let timeout_task = tokio::spawn(server.clone().detect_timeouts());
	let (client_master_addr, chunk_master_addr, client_task, chunk_task) =
		server.spawn_listeners(client_bind, chunk_bind).await?;

	let transport = connect(client_master_addr, Json::default).await?;
	let client = ClientMasterClient::new(client::Config::default(), transport).spawn();

	println!("master up: client={client_master_addr} chunk={chunk_master_addr}");
	repl.master = Some(MasterCtx {
		client,
		client_master_addr,
		chunk_master_addr,
		_client_task: client_task,
		_chunk_task: chunk_task,
		_timeout_task: timeout_task,
	});
	Ok(())
}

async fn cmd_chunk(repl: &mut Repl, data_port: u16) -> Result<(), Box<dyn std::error::Error>> {
	let chunk_master_addr = repl
		.master
		.as_ref()
		.map(|m| m.chunk_master_addr)
		.ok_or("master not running; run `master` first")?;

	let bind: SocketAddr = (Ipv6Addr::LOCALHOST, data_port).into();
	let listener = TcpListener::bind(bind).await?;
	let local = listener.local_addr()?;
	let server = ChunkServer::register(chunk_master_addr, local, Path::new("wal.db"))
		.await
		.map_err(|e| e as Box<dyn std::error::Error>)?;
	let handles = server.spawn(listener);

	let idx = repl.chunks.len();
	println!("chunk[{idx}] up: data={local}");
	repl.chunks.push(ChunkCtx {
		data_addr: local,
		handles: Some(handles),
	});
	Ok(())
}

fn cmd_chunks(repl: &Repl) {
	if repl.chunks.is_empty() {
		println!("(no chunk servers)");
		return;
	}
	for (i, c) in repl.chunks.iter().enumerate() {
		let status = if c.handles.is_some() { "" } else { " (killed)" };
		println!("chunk[{i}] data={}{status}", c.data_addr);
	}
}

fn cmd_kill(repl: &mut Repl, index: usize) -> Result<(), Box<dyn std::error::Error>> {
	let ctx = repl
		.chunks
		.get_mut(index)
		.ok_or_else(|| format!("kill: no chunk server at index {index}"))?;
	match ctx.handles.take() {
		Some(h) => {
			h.abort();
			println!("chunk[{index}] killed (data={})", ctx.data_addr);
			Ok(())
		}
		None => Err(format!("kill: chunk[{index}] is already killed").into()),
	}
}

fn cmd_status(repl: &Repl) {
	match &repl.master {
		Some(m) => println!(
			"master: client={} chunk={}; chunk servers: {}",
			m.client_master_addr,
			m.chunk_master_addr,
			repl.chunks.len()
		),
		None => println!("master: not running; chunk servers: {}", repl.chunks.len()),
	}
}

// --------------------------------------------------------------------------
// File-plane commands
// --------------------------------------------------------------------------

async fn cmd_put(
	master: &ClientMasterClient,
	local: &Path,
	remote: String,
	chunk_size: u64,
) -> Result<(), Box<dyn std::error::Error>> {
	let bytes = fs::read(local).await?;
	let len = bytes.len() as u64;

	let chunks = master
		.allocate(context::current(), remote.clone(), len, chunk_size)
		.await?
		.map_err(display_master_error)?;

	let expected = if len == 0 {
		0
	} else {
		len.div_ceil(chunk_size) as usize
	};
	assert_eq!(
		chunks.len(),
		expected,
		"master returned {} chunks for len={len} chunk_size={chunk_size}; expected {expected}",
		chunks.len(),
	);

	for (i, (chunk_id, replicas)) in chunks.iter().enumerate() {
		let start = i as u64 * chunk_size;
		let end = ((i as u64 + 1) * chunk_size).min(len);
		let payload = bytes[start as usize..end as usize].to_vec();
		write_chunk(*chunk_id, replicas, payload).await?;
	}

	println!("wrote {} ({len} bytes, {} chunk(s))", remote, chunks.len());
	Ok(())
}

async fn cmd_get(
	master: &ClientMasterClient,
	remote: String,
	local: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
	let chunks = master
		.lookup(context::current(), remote.clone())
		.await?
		.map_err(display_master_error)?;

	let mut out: Vec<u8> = Vec::new();
	for (chunk_id, replicas) in &chunks {
		let bytes = read_chunk_full(*chunk_id, replicas).await?;
		out.extend_from_slice(&bytes);
	}

	fs::write(local, &out).await?;
	println!(
		"read {} ({} bytes, {} chunk(s))",
		remote,
		out.len(),
		chunks.len()
	);
	Ok(())
}

async fn cmd_delete(
	master: &ClientMasterClient,
	remote: String,
) -> Result<(), Box<dyn std::error::Error>> {
	master
		.delete(context::current(), remote.clone())
		.await?
		.map_err(display_master_error)?;
	println!("deleted {remote}");
	Ok(())
}

// --------------------------------------------------------------------------
// Data-plane helpers
// --------------------------------------------------------------------------

/// Send a chain-replicated write to `replicas.primary`. The primary is
/// responsible for forwarding to the secondary before acknowledging.
async fn write_chunk(
	chunk_id: u64,
	replicas: &ReplicaSet,
	payload: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
	let mut stream = TcpStream::connect(replicas.primary).await?;
	let req = DataRequest::Write {
		chunk_id,
		forward_to: replicas.secondary,
		payload,
	};
	write_frame(&mut stream, &req).await?;
	match read_frame::<_, DataResponse>(&mut stream).await? {
		DataResponse::Written => Ok(()),
		DataResponse::Error(msg) => Err(format!("primary {}: {msg}", replicas.primary).into()),
		DataResponse::NotFound => Err("primary returned NotFound for write".into()),
		DataResponse::Data(_) => Err("primary returned Data for write".into()),
	}
}

/// Read an entire chunk; falls back to the secondary on any
/// connect/read error.
async fn read_chunk_full(
	chunk_id: u64,
	replicas: &ReplicaSet,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
	match read_chunk_from(replicas.primary, chunk_id).await {
		Ok(bytes) => Ok(bytes),
		Err(primary_err) => match replicas.secondary {
			Some(sec) => read_chunk_from(sec, chunk_id)
				.await
				.map_err(|sec_err| format!("primary {primary_err}; secondary {sec_err}").into()),
			None => Err(primary_err.into()),
		},
	}
}

async fn read_chunk_from(addr: SocketAddr, chunk_id: u64) -> Result<Vec<u8>, String> {
	let mut stream = TcpStream::connect(addr)
		.await
		.map_err(|e| format!("connect to {addr}: {e}"))?;
	let req = DataRequest::Read { chunk_id };
	write_frame(&mut stream, &req)
		.await
		.map_err(|e| format!("write_frame to {addr}: {e}"))?;
	match read_frame::<_, DataResponse>(&mut stream)
		.await
		.map_err(|e| format!("read_frame from {addr}: {e}"))?
	{
		DataResponse::Data(bytes) => Ok(bytes),
		DataResponse::NotFound => Err(format!("{addr}: chunk {chunk_id} not found")),
		DataResponse::Error(msg) => Err(format!("{addr}: {msg}")),
		DataResponse::Written => Err(format!("{addr}: returned Written for read")),
	}
}

fn display_master_error(e: MasterError) -> Box<dyn std::error::Error> {
	Box::<dyn std::error::Error>::from(e.to_string())
}

//! Chunk-server binary.
//!
//! Binds a TCP data-plane listener, registers with the master, and
//! spawns the heartbeat + data-plane loops from [`ChunkServer::serve`].
//! Chunk data is held in memory; it is lost on process exit.

use std::net::{Ipv6Addr, SocketAddr};

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use gfs::chunk::ChunkServer;

fn usage_and_exit() -> ! {
	eprintln!(
		"usage: chunk_server <master-addr> [--data-port <port>]\n\
         \n\
         master-addr    tarpc endpoint of the master's chunk-master service (host:port)\n\
         --data-port    TCP port for the data plane (default: 0, OS-assigned)"
	);
	std::process::exit(2);
}

fn parse_args() -> (SocketAddr, u16) {
	let mut args = std::env::args().skip(1);
	let master_addr: SocketAddr = match args.next() {
		Some(s) => match s.parse() {
			Ok(a) => a,
			Err(_) => usage_and_exit(),
		},
		None => usage_and_exit(),
	};
	let mut data_port: u16 = 0;
	while let Some(flag) = args.next() {
		match flag.as_str() {
			"--data-port" => {
				data_port = match args.next().and_then(|s| s.parse().ok()) {
					Some(p) => p,
					None => usage_and_exit(),
				}
			}
			_ => usage_and_exit(),
		}
	}
	(master_addr, data_port)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	tracing_subscriber::fmt()
		.with_env_filter(
			EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| EnvFilter::new("info,gfs=debug,tarpc=warn")),
		)
		.init();

	let (master_addr, data_port) = parse_args();

	let bind: SocketAddr = (Ipv6Addr::LOCALHOST, data_port).into();
	let listener = TcpListener::bind(bind).await?;
	let local = listener.local_addr()?;
	tracing::info!(%local, "chunk server binding");

	let server = ChunkServer::register(master_addr, local, std::path::Path::new("")).await?;
	server.serve(listener).await;
	Ok(())
}

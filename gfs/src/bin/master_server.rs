//! Master server binary.
//!
//! Spawns the two tarpc listeners (client-facing and chunk-server-facing)
//! plus the heartbeat-timeout detector task. Ports are configurable via
//! the `GFS_CLIENT_PORT` and `GFS_CHUNK_PORT` environment variables;
//! defaults match the values used by the sibling binaries.

use std::net::{Ipv6Addr, SocketAddr};

use tracing_subscriber::EnvFilter;

use gfs::master::MasterServer;

const DEFAULT_CLIENT_PORT: u16 = 50000;
const DEFAULT_CHUNK_PORT: u16 = 50001;

fn port_from_env(var: &str, default: u16) -> u16 {
	std::env::var(var)
		.ok()
		.and_then(|s| s.parse().ok())
		.unwrap_or(default)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	tracing_subscriber::fmt()
		.with_env_filter(
			EnvFilter::try_from_default_env()
				.unwrap_or_else(|_| EnvFilter::new("info,gfs=debug,tarpc=warn")),
		)
		.init();

	let client_bind: SocketAddr = (
		Ipv6Addr::LOCALHOST,
		port_from_env("GFS_CLIENT_PORT", DEFAULT_CLIENT_PORT),
	)
		.into();
	let chunk_bind: SocketAddr = (
		Ipv6Addr::LOCALHOST,
		port_from_env("GFS_CHUNK_PORT", DEFAULT_CHUNK_PORT),
	)
		.into();

	let server = MasterServer::new();
	tokio::spawn(server.clone().detect_timeouts());

	let (client_addr, chunk_addr, client_task, chunk_task) =
		server.spawn_listeners(client_bind, chunk_bind).await?;
	tracing::info!(%client_addr, %chunk_addr, "master server listening");

	let _ = tokio::join!(client_task, chunk_task);
	Ok(())
}

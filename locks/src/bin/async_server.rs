//! Single-threaded async/await lock server.
//!
//! Your job: spawn one task per connection with
//! [`LocalSpawnExt::spawn_local`] onto a [`LocalPool`].


use std::{env, io, net::TcpListener};

// use async_std::net::TcpListener;
// use futures::executor::LocalPool;
// use futures::task::LocalSpawnExt;

fn main() -> io::Result<()> {
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let _listener = TcpListener::bind(&addr)?;
	eprintln!("async_server listening on {}", addr);

	todo!("accept loop: read every incoming connection and spawn a per-connection task")
}

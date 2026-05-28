//! Multi-threaded lock server (monitor pattern).
//!
//! Your job: one listener thread plus one worker thread per connection.
//!
//! Track per-client which locks are currently held (so disconnect can
//! release them and promote the next waiter) and which locks the client is
//! currently waiting on.

use std::{env, io, net::TcpListener};

fn main() -> io::Result<()> {
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let _listener = TcpListener::bind(&addr)?;
	eprintln!("thread_server listening on {}", addr);

	todo!(
		"accept connections, spawn a worker thread per client, share state"
	)
}

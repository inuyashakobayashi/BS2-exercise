//! Async/await lock server — not implemented in this submission.
//!
//! This project implements the thread-based and event-based variants.
//! The async variant is left as a stub to allow compilation without
//! async dependencies.

use std::{env, io, net::TcpListener};

fn main() -> io::Result<()> {
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let _listener = TcpListener::bind(&addr)?;
	eprintln!("async_server listening on {}", addr);

	todo!("accept loop: read every incoming connection and spawn a per-connection task")
}

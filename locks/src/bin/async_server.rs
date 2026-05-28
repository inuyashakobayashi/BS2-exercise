//! Single-threaded async/await lock server.
//!
//! Your job: spawn one task per connection with
//! [`LocalSpawnExt::spawn_local`] onto a [`LocalPool`].

use std::{env, io};

use async_std::net::TcpListener;
use futures::executor::LocalPool;
use futures::task::LocalSpawnExt;

fn main() -> io::Result<()> {
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let mut pool = LocalPool::new();
	let spawner = pool.spawner();

	spawner
		.spawn_local(async move {
			let _listener = match TcpListener::bind(&addr).await {
				Ok(l) => l,
				Err(e) => {
					eprintln!("bind failed: {}", e);
					return;
				}
			};
			eprintln!("async_server listening on {}", addr);

			todo!(
				"accept loop: read every incoming connection and spawn a per-connection task"
			)
		})
		.expect("failed to spawn accept loop");

	pool.run();
	Ok(())
}

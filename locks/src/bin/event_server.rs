//! Single-threaded `mio` event-loop lock server.
//!
//! Your job: all state lives in one thread. A [`Poll`]
//! multiplexes the listener and every connected client; the main loop
//! reacts to readiness events without ever blocking on a single socket.

use std::{env, io};

use mio::{Events, Interest, Poll, Token, net};

fn main() -> io::Result<()> {
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let mut listener = net::TcpListener::bind(addr.parse().expect("invalid bind address"))?;
	eprintln!("event_server listening on {}", addr);

	let mut poll = Poll::new()?;
	let mut events = Events::with_capacity(64);
	poll.registry()
		.register(&mut listener, Token(0), Interest::READABLE)?;

	loop {
		poll.poll(&mut events, None)?;
		for _event in events.iter() {
			todo!("dispatch on event.token(): listener -> accept; per-client -> read/write paths")
		}
	}
}

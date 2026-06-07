//! Single-threaded `mio` event-loop lock server.
//!
//! # Architecture
//!
//! Everything runs in a single OS thread driven by mio::Poll. The poll
//! object multiplexes the listener socket and every connected client socket
//! without blocking on any one of them.
//!
//! ```
//!   loop {
//!       poll.poll(&mut events, None);   // block until ≥1 socket is ready
//!       for event in events {
//!           match event.token() {
//!               LISTENER → accept new connections
//!               CLIENT   → read / write that client's socket
//!           }
//!       }
//!   }
//! ```
//!
//! # Non-blocking IO and message fragmentation
//!
//! Every socket is registered as non-blocking. mio guarantees that when
//! a socket is reported as readable, at least one byte is available without
//! blocking — but not that a full newline-terminated message is present.
//! TCP is a byte stream; a single send() by the peer may arrive split across
//! multiple recv() calls (fragmentation), and vice versa.
//!
//! The LineFramer (from lib.rs) handles this transparently:
//!
//! * refresh() reads all currently available bytes into an internal buffer.
//! * next_line() returns the next complete line (if one is buffered), or
//!   Ok(None) if a full line has not arrived yet.
//! * write() / flush_nonblocking() buffer outgoing bytes and drain the
//!   buffer to the socket when it reports writeable, tolerating WouldBlock.
//!
//! # Blocking on ACQUIRE
//!
//! The event loop never actually blocks on a single client. Instead, an
//! ACQUIRE request for a contended lock simply stores the requesting client's
//! ID in the lock's FIFO waiter queue — no reply is sent yet. When the holder
//! later sends RELEASE, promote_and_notify finds the next waiter, queues a
//! GRANTED response into that client's write buffer, and re-registers its
//! socket for WRITABLE interest so the event loop will flush it.
//!
//! # Disconnect handling
//!
//! When refresh() returns UnexpectedEof (or any other IO error),
//! disconnect_client is called. It:
//!   1. Removes the client from any waiter queue it may be in.
//!   2. Releases every lock the client held by calling promote_and_notify on
//!      each one, so the next waiter receives its GRANTED response.
//!

use std::{
	collections::{HashMap, VecDeque},
	env,
	io::{self, Write},
	net::SocketAddr,
};

use mio::{Events, Interest, Poll, Token, net};

use locklib::{
	ClientId, ERR_ALREADY_HELD, ERR_INVALID_LOCK_NAME, ERR_LINE_TOO_LONG, ERR_NOT_HELD,
	ERR_UNKNOWN_COMMAND_PREFIX, FrameError, LineFramer, Request, Response,
};

// ---------------------------------------------------------------------------
// Token layout
// ---------------------------------------------------------------------------

/// Token reserved for the server's listening socket.
const LISTENER_TOKEN: Token = Token(0);

/// Convert a slab slot index to the corresponding mio Token.
fn client_token(slot_idx: usize) -> Token {
	Token(slot_idx + 1)
}

/// Inverse of `client_token`: recover the slab slot index from a Token.
fn token_to_client_idx(t: Token) -> usize {
	t.0 - 1
}

// ---------------------------------------------------------------------------
// Lock store
// ---------------------------------------------------------------------------

/// State of a single named lock.
struct LockEntry {
	/// Some(id) while held by a client; None when free.
	holder: Option<ClientId>,
	/// FIFO queue of waiting client IDs. The front is the oldest waiter and
	/// will be granted the lock next (prevents starvation).
	waiters: VecDeque<ClientId>,
}

impl LockEntry {
	fn new() -> Self {
		LockEntry {
			holder: None,
			waiters: VecDeque::new(),
		}
	}
}

/// Central lock table for the entire server.
struct LockStore {
	/// Map from lock name to entry. Entries are created on first access.
	locks: HashMap<String, LockEntry>,
}

impl LockStore {
	fn new() -> Self {
		LockStore {
			locks: HashMap::new(),
		}
	}

	/// Return the entry for name, creating it (free, empty queue) if absent.
	fn entry(&mut self, name: &str) -> &mut LockEntry {
		self.locks
			.entry(name.to_owned())
			.or_insert_with(LockEntry::new)
	}

	/// Pop the next waiter from name's queue, set it as the new holder, and
	/// return its client ID. Returns None if the queue is empty (lock freed).
	fn promote_next(&mut self, name: &str) -> Option<ClientId> {
		let entry = self.locks.get_mut(name)?;
		if let Some(next_id) = entry.waiters.pop_front() {
			entry.holder = Some(next_id);
			Some(next_id)
		} else {
			entry.holder = None;
			None
		}
	}
}

// ---------------------------------------------------------------------------
// Per-client connection state
// ---------------------------------------------------------------------------

/// All state associated with one active TCP connection.
struct ClientState {
	/// Unique connection ID assigned at accept time.
	id: ClientId,
	/// Framed non-blocking IO over the raw mio TcpStream.
	/// LineFramer handles TCP fragmentation on the read side and
	/// WouldBlock-safe buffered writes on the write side.
	framer: LineFramer<net::TcpStream>,
	/// If Some(name), this client has sent ACQUIRE for name but has not
	/// yet received a GRANTED reply (the lock was contended). Used to update
	/// held when the waiter is promoted.
	waiting_for: Option<String>,
	/// Names of locks currently held by this client. Used to release them all
	/// when the client disconnects.
	held: Vec<String>,
	/// True when the write buffer is non-empty and we need a WRITABLE event
	/// to flush it. Tracks whether the socket is currently registered with
	/// `Interest::WRITABLE` so we can avoid redundant `reregister` calls.
	wants_write: bool,
}

impl ClientState {
	fn new(id: ClientId, stream: net::TcpStream) -> Self {
		ClientState {
			id,
			framer: LineFramer::new(stream),
			waiting_for: None,
			held: Vec::new(),
			wants_write: false,
		}
	}

	/// Append a formatted response to the write buffer.
	fn enqueue(&mut self, resp: Response) {
		// `Write for LineFramer` buffers into an internal `Vec<u8>` — always
		// succeeds (no IO involved at this point).
		let _ = writeln!(self.framer, "{}", resp);
		self.wants_write = true;
	}
}


/// Simple slab allocator for `ClientState` values.
///
/// Slot indices are stable across insertions so they can be embedded in mio
/// Tokens without invalidation.
struct ClientSlab {
	slots: Vec<Option<ClientState>>,
}

impl ClientSlab {
	fn new() -> Self {
		ClientSlab { slots: Vec::new() }
	}

	/// Insert a client and return the slot index that maps to it via `client_token`.
	fn insert(&mut self, client: ClientState) -> usize {
		// Reuse any previously freed slot before growing the backing vector.
		if let Some(idx) = self.slots.iter().position(|s| s.is_none()) {
			self.slots[idx] = Some(client);
			idx
		} else {
			let idx = self.slots.len();
			self.slots.push(Some(client));
			idx
		}
	}

	fn get(&self, idx: usize) -> Option<&ClientState> {
		self.slots.get(idx).and_then(|s| s.as_ref())
	}

	fn get_mut(&mut self, idx: usize) -> Option<&mut ClientState> {
		self.slots.get_mut(idx).and_then(|s| s.as_mut())
	}

	/// Remove and return the client at `idx`, freeing the slot for reuse.
	fn remove(&mut self, idx: usize) -> Option<ClientState> {
		self.slots.get_mut(idx).and_then(|s| s.take())
	}

	/// Iterate over all live (non-None) clients with their slot indices.
	fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut ClientState)> {
		self.slots
			.iter_mut()
			.enumerate()
			.filter_map(|(i, s)| s.as_mut().map(|c| (i, c)))
	}
}

// ---------------------------------------------------------------------------
// Main event loop
// ---------------------------------------------------------------------------

fn main() -> io::Result<()> {
	let addr: SocketAddr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into())
		.parse()
		.expect("invalid bind address");

	let mut listener = net::TcpListener::bind(addr)?;
	eprintln!("event_server listening on {}", addr);

	let mut poll = Poll::new()?;
	let mut events = Events::with_capacity(64);

	// Register the listener as the first source; it never needs WRITABLE.
	poll.registry()
		.register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

	let mut clients = ClientSlab::new();
	let mut locks = LockStore::new();
	let mut next_client_id: ClientId = 1;

	loop {
		// Block until at least one socket has an event. `None` timeout means
		// "wait forever" — there is no timer-based work in this server.
		poll.poll(&mut events, None)?;

		// Collect into a Vec first because processing events may mutate
		// `clients` (e.g. insert new clients on accept), which would conflict
		// with iterating over `events` and `clients` simultaneously.
		let event_list: Vec<(Token, bool, bool)> = events
			.iter()
			.map(|e| (e.token(), e.is_readable(), e.is_writable()))
			.collect();

		for (token, readable, writable) in event_list {
			// -----------------------------------------------------------------
			// Listener: accept all pending connections in a tight loop until
			// `WouldBlock`, which signals "no more connections right now".
			// -----------------------------------------------------------------
			if token == LISTENER_TOKEN {
				loop {
					match listener.accept() {
						Ok((stream, _peer_addr)) => {
							// Assign a unique ID and insert into the slab.
							let id = next_client_id;
							next_client_id += 1;
							let client = ClientState::new(id, stream);
							let slot_idx = clients.insert(client);

							// Register the new socket for readability only; we
							// will add WRITABLE interest on demand when there
							// is data to send.
							let tok = client_token(slot_idx);
							if let Some(c) = clients.get_mut(slot_idx) {
								poll.registry().register(
									c.framer.as_mut(),
									tok,
									Interest::READABLE,
								)?;
							}
						}
						Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
						Err(e) => eprintln!("accept error: {}", e),
					}
				}
				continue; // back to the outer event loop
			}

			// -----------------------------------------------------------------
			// Client socket event: resolve the slab index from the Token.
			// -----------------------------------------------------------------
			let slot_idx = token_to_client_idx(token);

			// Handle WRITABLE before READABLE: flush buffered output first so
			// that a response generated in this same iteration can be sent
			// without waiting for the next poll cycle.
			if writable {
				let disconnect = if let Some(client) = clients.get_mut(slot_idx) {
					match client.framer.flush_nonblocking() {
						Ok(true) => {
							// All bytes sent. Switch back to READABLE-only
							// interest to avoid spurious wakeups.
							client.wants_write = false;
							poll.registry().reregister(
								client.framer.as_mut(),
								token,
								Interest::READABLE,
							)?;
							false
						}
						Ok(false) => false, // still data in buffer, keep WRITABLE registered
						Err(_) => true,     // write error → disconnect
					}
				} else {
					false
				};
				if disconnect {
					disconnect_client(slot_idx, token, &mut clients, &mut locks, &mut poll)?;
					continue;
				}
			}

			if readable {
				// Pull all currently available bytes into the LineFramer buffer.
				// `WouldBlock` is swallowed by `refresh`; `UnexpectedEof` means
				// the peer closed the connection.
				let should_disconnect = if let Some(client) = clients.get_mut(slot_idx) {
					match client.framer.refresh() {
						Ok(()) => false,
						Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => true,
						Err(_) => true,
					}
				} else {
					false
				};

				if should_disconnect {
					disconnect_client(slot_idx, token, &mut clients, &mut locks, &mut poll)?;
					continue;
				}

				// Drain all complete lines buffered so far. A single `refresh`
				// may have pulled in multiple lines (e.g. a pipelined client),
				// so loop until `next_line` returns `Ok(None)`.
				loop {
					let line_result = if let Some(client) = clients.get_mut(slot_idx) {
						client.framer.next_line()
					} else {
						break; // client was removed inside process_line (shouldn't happen)
					};

					match line_result {
						Ok(None) => break, // no more complete lines in buffer
						Ok(Some(line)) => {
							let line = line.trim_end_matches('\r').to_owned();
							if line.is_empty() {
								continue;
							}
							process_line(slot_idx, token, line, &mut clients, &mut locks, &mut poll)?;
						}
						Err(FrameError::LineTooLong) => {
							// Protocol violation: send error, then drop connection.
							if let Some(client) = clients.get_mut(slot_idx) {
								client.enqueue(Response::Err(ERR_LINE_TOO_LONG[4..].to_owned()));
							}
							disconnect_client(slot_idx, token, &mut clients, &mut locks, &mut poll)?;
							break;
						}
						Err(_) => {
							// IO or UTF-8 error in the framer → disconnect.
							disconnect_client(slot_idx, token, &mut clients, &mut locks, &mut poll)?;
							break;
						}
					}
				}

				// If processing generated any responses, ensure the socket is
				// registered for WRITABLE so the next poll cycle flushes them.
				if let Some(client) = clients.get_mut(slot_idx) {
					if client.wants_write {
						poll.registry().reregister(
							client.framer.as_mut(),
							token,
							Interest::READABLE | Interest::WRITABLE,
						)?;
					}
				}
			}
		}


		let mut to_disconnect: Vec<(usize, Token)> = Vec::new();
		let mut to_reregister: Vec<(usize, Token)> = Vec::new();

		for (idx, client) in clients.iter_mut() {
			if !client.wants_write {
				continue;
			}
			let tok = client_token(idx);
			match client.framer.flush_nonblocking() {
				Ok(true) => {
					// Fully flushed: downgrade to READABLE only.
					client.wants_write = false;
					to_reregister.push((idx, tok));
				}
				Ok(false) => {
					// Partial flush (`WouldBlock`): keep WRITABLE registered.
					to_reregister.push((idx, tok));
				}
				Err(_) => {
					to_disconnect.push((idx, tok));
				}
			}
		}

		for (idx, tok) in to_reregister {
			if let Some(client) = clients.get_mut(idx) {
				let interest = if client.wants_write {
					Interest::READABLE | Interest::WRITABLE
				} else {
					Interest::READABLE
				};
				// Use `let _` to swallow errors from already-deregistered sockets.
				let _ = poll.registry().reregister(client.framer.as_mut(), tok, interest);
			}
		}

		for (idx, tok) in to_disconnect {
			disconnect_client(idx, tok, &mut clients, &mut locks, &mut poll)?;
		}
	}
}

// ---------------------------------------------------------------------------
// Process one complete request line from a client.
// ---------------------------------------------------------------------------


fn process_line(
	slot_idx: usize,
	_token: Token,
	line: String,
	clients: &mut ClientSlab,
	locks: &mut LockStore,
	poll: &mut Poll,
) -> io::Result<()> {
	// Retrieve the client ID before any mutable borrows.
	let client_id = clients.get(slot_idx).map(|c| c.id).unwrap_or(0);

	let req: Request = match line.parse() {
		Ok(r) => r,
		Err(_) => {
			// Classify the error: known verb with bad lock name vs. unknown verb.
			let verb = line.split_whitespace().next().unwrap_or(&line).to_owned();
			let known_verbs = ["ACQUIRE", "TRY_ACQUIRE", "RELEASE", "STATUS", "LIST"];
			let resp = if known_verbs.contains(&verb.as_str()) {
				Response::Err(ERR_INVALID_LOCK_NAME[4..].to_owned())
			} else {
				Response::Err(format!("{}{}", ERR_UNKNOWN_COMMAND_PREFIX, verb))
			};
			if let Some(client) = clients.get_mut(slot_idx) {
				client.enqueue(resp);
			}
			return Ok(());
		}
	};

	match req {
		Request::Acquire(ref name) => {
			let entry = locks.entry(name);

			// Reject if this client already holds the lock.
			if entry.holder == Some(client_id) {
				if let Some(client) = clients.get_mut(slot_idx) {
					client.enqueue(Response::Err(ERR_ALREADY_HELD[4..].to_owned()));
				}
				return Ok(());
			}
			// Reject if this client is already queued for the same lock.
			if entry.waiters.iter().any(|id| *id == client_id) {
				if let Some(client) = clients.get_mut(slot_idx) {
					client.enqueue(Response::Err(ERR_ALREADY_HELD[4..].to_owned()));
				}
				return Ok(());
			}

			if entry.holder.is_none() {
				// Lock is free → grant immediately. No waiting needed.
				entry.holder = Some(client_id);
				if let Some(client) = clients.get_mut(slot_idx) {
					client.held.push(name.clone());
					client.enqueue(Response::Granted(name.clone()));
				}
			} else {
				// Lock is contended → enqueue. No response is sent yet; the
				// GRANTED reply will be queued by `promote_and_notify` when
				// the current holder releases the lock.
				entry.waiters.push_back(client_id);
				if let Some(client) = clients.get_mut(slot_idx) {
					client.waiting_for = Some(name.clone());
				}
			}
		}

		Request::TryAcquire(ref name) => {
			// Non-blocking: grant only if the lock is currently free.
			let entry = locks.entry(name);
			if entry.holder.is_none() {
				entry.holder = Some(client_id);
				if let Some(client) = clients.get_mut(slot_idx) {
					client.held.push(name.clone());
					client.enqueue(Response::Granted(name.clone()));
				}
			} else {
				if let Some(client) = clients.get_mut(slot_idx) {
					client.enqueue(Response::Denied(name.clone()));
				}
			}
		}

		Request::Release(ref name) => {
			let entry = locks.entry(name);
			// Only the current holder may release.
			if entry.holder != Some(client_id) {
				if let Some(client) = clients.get_mut(slot_idx) {
					client.enqueue(Response::Err(ERR_NOT_HELD[4..].to_owned()));
				}
				return Ok(());
			}

			// Remove from the held set and send OK to the releaser.
			if let Some(client) = clients.get_mut(slot_idx) {
				client.held.retain(|n| n != name);
				client.enqueue(Response::Ok);
			}

			// Hand the lock to the next waiter and queue GRANTED into their
			// write buffer. Their socket will be re-registered for WRITABLE.
			promote_and_notify(name, locks, clients, poll)?;
		}

		Request::Status(ref name) => {
			let entry = locks.entry(name);
			let resp = match entry.holder {
				None => Response::StatusFree {
					name: name.clone(),
					waiters: entry.waiters.len(),
				},
				Some(holder) => Response::StatusHeld {
					name: name.clone(),
					holder,
					waiters: entry.waiters.len(),
				},
			};
			if let Some(client) = clients.get_mut(slot_idx) {
				client.enqueue(resp);
			}
		}

		Request::List => {
			// Collect the names of all held locks, then emit the multi-line
			// LIST response (LIST\n<name>\n...\nEND).
			let held_names: Vec<String> = locks
				.locks
				.iter()
				.filter(|(_, e)| e.holder.is_some())
				.map(|(n, _)| n.clone())
				.collect();

			if let Some(client) = clients.get_mut(slot_idx) {
				client.enqueue(Response::ListBegin);
				for n in &held_names {
					client.enqueue(Response::ListEntry(n.clone()));
				}
				client.enqueue(Response::ListEnd);
			}
		}
	}

	Ok(())
}

// ---------------------------------------------------------------------------
// Promote the next waiter for a lock and queue GRANTED to their connection.
// ---------------------------------------------------------------------------

fn promote_and_notify(
	name: &str,
	locks: &mut LockStore,
	clients: &mut ClientSlab,
	poll: &mut Poll,
) -> io::Result<()> {
	loop {
		match locks.promote_next(name) {
			None => break, // queue empty; lock is now free
			Some(next_client_id) => {
				// Find the slab slot for this client ID.
				let slot = clients
					.slots
					.iter()
					.enumerate()
					.find(|(_, s)| s.as_ref().map(|c| c.id) == Some(next_client_id))
					.map(|(i, _)| i);

				if let Some(slot_idx) = slot {
					if let Some(client) = clients.get_mut(slot_idx) {
						client.waiting_for = None;
						client.held.push(name.to_owned());
						client.enqueue(Response::Granted(name.to_owned()));
						// Re-register the socket with WRITABLE interest so the
						// event loop flushes the GRANTED response.
						let tok = client_token(slot_idx);
						poll.registry().reregister(
							client.framer.as_mut(),
							tok,
							Interest::READABLE | Interest::WRITABLE,
						)?;
					}
					break; // successfully promoted one waiter
				} else {
					// Client already removed from the slab (disconnected).
					continue;
				}
			}
		}
	}
	Ok(())
}

// ---------------------------------------------------------------------------
// Disconnect a client cleanly.
// ---------------------------------------------------------------------------

fn disconnect_client(
	slot_idx: usize,
	_token: Token,
	clients: &mut ClientSlab,
	locks: &mut LockStore,
	poll: &mut Poll,
) -> io::Result<()> {
	let client = match clients.remove(slot_idx) {
		Some(c) => c,
		None => return Ok(()), // already removed (guard against double-disconnect)
	};

	// Deregister the socket. The underlying TcpStream is dropped when `client`
	// goes out of scope, which also closes the fd.
	let mut stream_ref = client.framer;
	let _ = poll.registry().deregister(stream_ref.as_mut());
	// `stream_ref` (and the TcpStream inside it) is dropped here.

	let client_id = client.id;

	// Remove this client from any waiter queues it was in.
	for entry in locks.locks.values_mut() {
		entry.waiters.retain(|id| *id != client_id);
	}

	// Release all held locks and wake the next waiter for each.
	let held = client.held.clone();
	for name in &held {
		if let Some(entry) = locks.locks.get_mut(name) {
			if entry.holder == Some(client_id) {
				// Clear the holder so `promote_next` can set the new one.
				entry.holder = None;
			}
		}
		promote_and_notify(name, locks, clients, poll)?;
	}

	Ok(())
}

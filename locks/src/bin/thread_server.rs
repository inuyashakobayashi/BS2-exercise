//! Multi-threaded lock server (monitor pattern).
//!
//! # Architecture
//!
//! One OS thread is spawned per accepted TCP connection. All threads share a
//! single `Arc<Mutex<LockStore>>` which acts as the **monitor**: every access
//! to the lock table goes through the mutex, making cross-thread state
//! consistent.
//!
//! # Blocking on ACQUIRE
//!
//! When a lock is already held, the requesting thread must park itself until
//! the lock becomes available. We achieve this with a per-waiter
//! `Arc<(Mutex<WakeState>, Condvar)>` pair:
//!
//! 1. The requesting thread creates the pair and pushes it (with its client ID)
//!    onto the lock's FIFO waiter queue **while holding the store mutex**.
//! 2. It then releases the store mutex and blocks on `Condvar::wait`.
//! 3. When the holder releases the lock, `promote_next` pops the front waiter,
//!    updates `holder`, and calls `notify_one` — which wakes the sleeping thread.
//! 4. The thread checks `WakeState`: `Granted` means it now owns the lock;
//!    `Evicted` means it was removed from the queue (due to disconnect) without
//!    receiving the lock.
//!
//! # Disconnect cleanup
//!
//! When `BufReader::lines()` returns an error (peer closed the socket), the
//! thread falls through to the cleanup block at the end of `handle_client`:
//!   - Scans all lock queues and evicts this client if it is still waiting.
//!   - Calls `promote_next` on every lock this client held, so queued clients
//!     are woken in FIFO order.
//!
//! # Trade-offs vs. event server
//!
//! + Simple, linear control flow: every connection reads and writes sequentially.
//! + Easy to reason about: each thread owns its own stack and local state.
//! - One OS stack per connection: expensive at high connection counts.
//! - Shared state requires Mutex discipline (risk of deadlock if misused).
//! - Context-switch overhead when many threads block/unblock simultaneously.

use std::{
	collections::{HashMap, HashSet, VecDeque},
	env,
	io::{self, BufRead, BufReader, Write},
	net::{TcpListener, TcpStream},
	sync::{Arc, Condvar, Mutex},
};

use locklib::{
	ClientId, ERR_ALREADY_HELD, ERR_INVALID_LOCK_NAME, ERR_NOT_HELD, ERR_UNKNOWN_COMMAND_PREFIX,
	Request, Response,
};

// ---------------------------------------------------------------------------
// Shared lock store
// ---------------------------------------------------------------------------

/// State of a single named lock.
struct LockEntry {
	/// `Some(id)` while the lock is held by a client; `None` when free.
	holder: Option<ClientId>,
	/// FIFO waiter queue. Each slot pairs a client ID with the condvar handle
	/// that the waiting thread is sleeping on. Front = oldest waiter = next to
	/// be granted (prevents starvation).
	waiters: VecDeque<(ClientId, Arc<(Mutex<WakeState>, Condvar)>)>,
}

/// Outcome of a condvar wake-up for a thread waiting on ACQUIRE.
#[derive(PartialEq, Clone, Copy)]
enum WakeState {
	/// Still sleeping; loop back and wait again (spurious wakeup guard).
	Waiting,
	/// The lock was successfully transferred to this client. `holder` has
	/// already been updated to our `client_id` by `promote_next`.
	Granted,
	/// We were removed from the waiter queue without receiving the lock.
	/// This happens when the server cleans up a disconnecting client that
	/// was blocked in ACQUIRE. The thread should exit without sending a reply.
	Evicted,
}

impl LockEntry {
	fn new() -> Self {
		LockEntry {
			holder: None,
			waiters: VecDeque::new(),
		}
	}

	/// Transfer the lock to the oldest waiter (FIFO). If the queue is empty
	/// the lock becomes free (`holder = None`).
	///
	/// Sets `holder` before waking the thread so that the new owner can
	/// observe its own ID in STATUS queries immediately after waking.
	fn promote_next(&mut self) {
		if let Some((next_id, wake)) = self.waiters.pop_front() {
			self.holder = Some(next_id);
			let (lock, cvar) = &*wake;
			let mut guard = lock.lock().unwrap();
			*guard = WakeState::Granted;
			cvar.notify_one(); // wake the sleeping thread
		} else {
			self.holder = None;
		}
	}

	/// Remove `client_id` from the waiter queue without granting the lock.
	/// Wakes the thread with `WakeState::Evicted` so it can exit cleanly.
	/// Returns `true` if the client was found and removed.
	fn evict_waiter(&mut self, client_id: ClientId) -> bool {
		if let Some(pos) = self.waiters.iter().position(|(id, _)| *id == client_id) {
			let (_, wake) = self.waiters.remove(pos).unwrap();
			let (lock, cvar) = &*wake;
			let mut guard = lock.lock().unwrap();
			*guard = WakeState::Evicted;
			cvar.notify_one();
			true
		} else {
			false
		}
	}
}

/// Central store shared across all connection threads.
struct LockStore {
	/// Lock table — entries are created implicitly on first access.
	locks: HashMap<String, LockEntry>,
	/// Monotonically increasing counter; each accepted connection gets a unique ID.
	next_id: ClientId,
}

impl LockStore {
	fn new() -> Self {
		LockStore {
			locks: HashMap::new(),
			next_id: 1,
		}
	}

	/// Allocate and return the next connection ID.
	fn alloc_id(&mut self) -> ClientId {
		let id = self.next_id;
		self.next_id += 1;
		id
	}

	/// Return the entry for `name`, creating it (free, no waiters) if absent.
	fn entry(&mut self, name: &str) -> &mut LockEntry {
		self.locks.entry(name.to_owned()).or_insert_with(LockEntry::new)
	}
}

/// Alias for the shared, mutex-protected store passed to each worker thread.
type SharedStore = Arc<Mutex<LockStore>>;

// ---------------------------------------------------------------------------
// Per-client worker thread
// ---------------------------------------------------------------------------

/// Main loop for one connected client. Runs entirely in its own OS thread.
///
/// Reads newline-delimited requests from `stream`, dispatches them against the
/// shared `store`, and writes responses back. When the connection closes (the
/// iterator ends), releases all locks held by this client.
fn handle_client(stream: TcpStream, store: SharedStore) {
	// Allocate a unique ID for this connection.
	let client_id: ClientId = {
		let mut s = store.lock().unwrap();
		s.alloc_id()
	};

	// Arc<TcpStream> trick from the task description: `TcpStream` implements
	// both `Read for &TcpStream` and `Write for &TcpStream`, so we can wrap it
	// in an Arc and share references to it between the reader and writer halves
	// without needing a Mutex around the stream itself.
	let stream = Arc::new(stream);
	let reader_stream = Arc::clone(&stream);

	// Track which locks this client currently holds so we can release them all
	// on disconnect (the "fail-safe" behaviour Chubby is known for).
	let mut held: HashSet<String> = HashSet::new();

	let reader = BufReader::new(&*reader_stream);
	let mut writer = &*stream; // shared reference; no extra allocation

	// Convenience macro: send one response line, ignoring write errors.
	// If the connection is already half-closed, the next read will surface EOF.
	macro_rules! send {
		($resp:expr) => {
			let _ = writeln!(writer, "{}", $resp);
		};
	}

	// Request loop: each iteration processes exactly one command.
	for line in reader.lines() {
		let line = match line {
			Ok(l) => l,
			Err(_) => break, // EOF or IO error → proceed to disconnect cleanup
		};

		// Tolerate bare CR from telnet / Windows line endings.
		let line = line.trim_end_matches('\r').to_owned();
		if line.is_empty() {
			continue;
		}

		// Attempt to parse the line as a typed Request. Unknown verbs or
		// syntactically invalid lock names are caught here and replied to
		// gracefully — the connection is *not* dropped.
		let req: Request = match line.parse() {
			Ok(r) => r,
			Err(_) => {
				let verb = line.split_whitespace().next().unwrap_or(&line);
				let known_verbs = ["ACQUIRE", "TRY_ACQUIRE", "RELEASE", "STATUS", "LIST"];
				if known_verbs.contains(&verb) {
					// Known verb with a bad argument: the only argument type is
					// a lock name, so the name must be invalid.
					send!(Response::Err(ERR_INVALID_LOCK_NAME[4..].to_owned()));
				} else {
					send!(Response::Err(format!(
						"{}{}",
						ERR_UNKNOWN_COMMAND_PREFIX, verb
					)));
				}
				continue; // keep the connection open
			}
		};

		match req {
			Request::Acquire(ref name) => {
				// ---- Phase 1: decide whether to grant immediately or park ----
				//
				// Hold the store mutex only long enough to inspect state and
				// either grant the lock or enqueue ourselves. We release it
				// *before* blocking on the condvar to avoid holding the global
				// lock while an entire thread sleeps.
				let wake = {
					let mut s = store.lock().unwrap();
					let entry = s.entry(name);

					// Re-acquiring a lock already held by this client is an error
					// (would cause a deadlock because RELEASE only decrements once).
					if entry.holder == Some(client_id) {
						send!(Response::Err(ERR_ALREADY_HELD[4..].to_owned()));
						continue;
					}
					// Also reject if this client is already in the waiter queue.
					if entry.waiters.iter().any(|(id, _)| *id == client_id) {
						send!(Response::Err(ERR_ALREADY_HELD[4..].to_owned()));
						continue;
					}

					if entry.holder.is_none() {
						// Lock is free → immediate grant. No need to sleep.
						entry.holder = Some(client_id);
						held.insert(name.clone());
						drop(s); // release mutex before writing to socket
						send!(Response::Granted(name.clone()));
						continue;
					}

					// Lock is contended → enqueue ourselves and prepare to sleep.
					// The Arc is shared between this thread (sleeper) and the
					// store entry (waker via promote_next / evict_waiter).
					let wake = Arc::new((Mutex::new(WakeState::Waiting), Condvar::new()));
					entry.waiters.push_back((client_id, Arc::clone(&wake)));
					wake
					// Store mutex released here (end of block).
				};

				// ---- Phase 2: sleep until woken ----
				//
				// Condvar::wait loop guards against spurious wakeups as required
				// by the POSIX spec (Rust's condvar can have them too).
				let state = {
					let (lock, cvar) = &*wake;
					let mut s = lock.lock().unwrap();
					while *s == WakeState::Waiting {
						s = cvar.wait(s).unwrap();
					}
					*s
				};

				// ---- Phase 3: inspect the wakeup reason ----
				match state {
					WakeState::Granted => {
						// promote_next already set holder = Some(client_id).
						held.insert(name.clone());
						send!(Response::Granted(name.clone()));
					}
					WakeState::Evicted | WakeState::Waiting => {
						// Evicted: the cleanup path removed us from the queue
						// (this client's own connection is being torn down).
						// Do not send a reply; the loop will exit shortly.
					}
				}
			}

			Request::TryAcquire(ref name) => {
				// Non-blocking: inspect the lock and reply immediately.
				let mut s = store.lock().unwrap();
				let entry = s.entry(name);
				if entry.holder.is_none() {
					entry.holder = Some(client_id);
					drop(s);
					held.insert(name.clone());
					send!(Response::Granted(name.clone()));
				} else {
					drop(s);
					send!(Response::Denied(name.clone()));
				}
			}

			Request::Release(ref name) => {
				let mut s = store.lock().unwrap();
				let entry = s.entry(name);
				// Only the current holder may release.
				if entry.holder != Some(client_id) {
					drop(s);
					send!(Response::Err(ERR_NOT_HELD[4..].to_owned()));
					continue;
				}
				// Transfer the lock to the next waiter (or free it).
				entry.promote_next();
				drop(s); // release mutex before writing
				held.remove(name);
				send!(Response::Ok);
			}

			Request::Status(ref name) => {
				let mut s = store.lock().unwrap();
				let entry = s.entry(name);
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
				drop(s);
				send!(resp);
			}

			Request::List => {
				// Snapshot the set of held locks under the mutex; emit the
				// multi-line LIST response after releasing it.
				let s = store.lock().unwrap();
				let held_names: Vec<String> = s
					.locks
					.iter()
					.filter(|(_, e)| e.holder.is_some())
					.map(|(n, _)| n.clone())
					.collect();
				drop(s);
				send!(Response::ListBegin);
				for n in &held_names {
					send!(Response::ListEntry(n.clone()));
				}
				send!(Response::ListEnd);
			}
		}
	}

	// ---------------------------------------------------------------------------
	// Disconnect cleanup
	//
	// The for-loop exited: the peer closed the connection (EOF) or an IO error
	// occurred. We must:
	//   1. Evict this client from any waiter queue (it should not receive the
	//      lock — the connection is gone).
	//   2. Release every lock this client held and promote the next waiter for
	//      each, so queued clients can proceed in FIFO order.
	//
	// Both steps happen under a single mutex acquisition for consistency.
	// ---------------------------------------------------------------------------
	let mut s = store.lock().unwrap();

	// Step 1: evict from any waiter queue.
	// A client is normally waiting on at most one lock, but we scan all entries
	// defensively.
	for entry in s.locks.values_mut() {
		entry.evict_waiter(client_id);
	}

	// Step 2: release all held locks and wake the next waiter for each.
	for name in &held {
		if let Some(entry) = s.locks.get_mut(name) {
			if entry.holder == Some(client_id) {
				entry.promote_next();
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn main() -> io::Result<()> {
	// Accept an optional bind address as the first CLI argument (used by the
	// integration test harness to pick an ephemeral port).
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let listener = TcpListener::bind(&addr)?;
	eprintln!("thread_server listening on {}", addr);

	// The store is shared across all threads via a reference-counted pointer.
	let store: SharedStore = Arc::new(Mutex::new(LockStore::new()));

	// Accept loop: block until a client connects, then hand off to a new thread.
	for stream in listener.incoming() {
		match stream {
			Ok(s) => {
				let store = Arc::clone(&store);
				// Each worker thread gets an Arc clone of the store and the
				// fresh TcpStream. The thread exits when handle_client returns.
				std::thread::spawn(move || handle_client(s, store));
			}
			Err(e) => eprintln!("accept error: {}", e),
		}
	}
	Ok(())
}

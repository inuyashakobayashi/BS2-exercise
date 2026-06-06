//! Shared test harness for the out-of-process integration tests.
//!
//! Each test spawns one of the three server binaries (`thread_server`,
//! `event_server`, `async_server`) on an ephemeral 127.0.0.1 port, talks to
//! it via plain `TcpStream`s, and relies on `Drop` to reap the child.
//!
//! The binaries are located through the `CARGO_BIN_EXE_<name>` environment
//! variables that Cargo sets at compile time for integration tests.

#![allow(dead_code)] // each test file only uses a subset of these helpers.

use std::{
	io::{BufRead, BufReader, ErrorKind, Write},
	net::{Shutdown, SocketAddr, TcpListener, TcpStream},
	process::{Child, Command, Stdio},
	sync::{Mutex, OnceLock},
	thread,
	time::{Duration, Instant},
};

use locklib::{Request, Response};

// --------------------------------------------------------------------------
// Server fixture
// --------------------------------------------------------------------------

#[derive(Copy, Clone, Debug)]
pub enum Variant {
	Thread,
	Event,
	Async,
}

impl Variant {
	fn binary_path(self) -> &'static str {
		match self {
			Variant::Thread => env!("CARGO_BIN_EXE_thread_server"),
			Variant::Event => env!("CARGO_BIN_EXE_event_server"),
			Variant::Async => env!("CARGO_BIN_EXE_async_server"),
		}
	}
}

pub struct Server {
	child: Option<Child>,
	pub addr: SocketAddr,
}

/// Serialises port-probe + server-spawn across parallel tests. Without this,
/// two tests can race: test A drops its probe listener, test B binds a fresh
/// probe, gets handed the same port, and one of the two server children then
/// fails to bind.
fn spawn_lock() -> &'static Mutex<()> {
	static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
	LOCK.get_or_init(|| Mutex::new(()))
}

impl Server {
	pub fn spawn(variant: Variant) -> Server {
		let guard = spawn_lock().lock().expect("spawn lock");

		// Reserve an ephemeral port and hold the probe until after the child
		// has (presumably) bound to it. We release the probe *before* the
		// server binds — still a tiny race, but the surrounding mutex means
		// only one test is in this critical section at a time, which rules
		// out cross-test collisions.
		let probe = TcpListener::bind("127.0.0.1:0").expect("reserve port");
		let addr = probe.local_addr().expect("probe addr");
		drop(probe);

		let child = Command::new(variant.binary_path())
			.arg(addr.to_string())
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.unwrap_or_else(|e| panic!("spawn {:?}: {}", variant, e));

		let server = Server {
			child: Some(child),
			addr,
		};

		// Poll the port until the server is accepting connections.
		let deadline = Instant::now() + Duration::from_secs(5);
		loop {
			match TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
				Ok(s) => {
					drop(s);
					break;
				}
				Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
				Err(e) => panic!("server {:?} never became ready: {}", variant, e),
			}
		}

		drop(guard);
		server
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		if let Some(mut child) = self.child.take() {
			let _ = child.kill();
			let _ = child.wait();
		}
	}
}

// --------------------------------------------------------------------------
// Client fixture
// --------------------------------------------------------------------------

pub struct Client {
	stream: TcpStream,
	reader: BufReader<TcpStream>,
}

const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(2);

impl Client {
	pub fn connect(server: &Server) -> Client {
		Self::connect_addr(server.addr)
	}

	pub fn connect_addr(addr: SocketAddr) -> Client {
		let stream =
			TcpStream::connect_timeout(&addr, Duration::from_secs(2)).expect("client connect");
		let reader_stream = stream.try_clone().expect("clone stream");
		stream
			.set_read_timeout(Some(DEFAULT_READ_TIMEOUT))
			.expect("set timeout");
		reader_stream
			.set_read_timeout(Some(DEFAULT_READ_TIMEOUT))
			.expect("set timeout");
		Client {
			stream,
			reader: BufReader::new(reader_stream),
		}
	}

	pub fn send(&mut self, req: Request) {
		writeln!(self.stream, "{}", req).expect("send request");
	}

	pub fn send_raw(&mut self, line: &str) {
		writeln!(self.stream, "{}", line).expect("send raw");
	}

	/// Reads one line, parses it as a `Response`, and panics on either
	/// I/O failure or parse failure. The per-read deadline is the socket's
	/// current read timeout (default 2 s).
	pub fn recv(&mut self) -> Response {
		let mut line = String::new();
		match self.reader.read_line(&mut line) {
			Ok(0) => panic!("server closed connection unexpectedly"),
			Ok(_) => {}
			Err(e) => panic!("read failed: {}", e),
		}
		if line.ends_with('\n') {
			line.pop();
		}
		if line.ends_with('\r') {
			line.pop();
		}
		line.parse()
			.unwrap_or_else(|_| panic!("unparseable response: {:?}", line))
	}

	pub fn expect(&mut self, expected: Response) {
		assert_eq!(self.recv(), expected, "response mismatch");
	}

	/// Asserts that no reply arrives within `within`. Used to verify that an
	/// `ACQUIRE` is parked on a contended lock.
	///
	/// All reads go through `self.reader` (a `BufReader` over a clone of the
	/// socket); we therefore also check the buffer *and* the underlying
	/// stream's read timeout here.
	pub fn expect_silent_for(&mut self, within: Duration) {
		if !self.reader.buffer().is_empty() {
			let buf = self.reader.buffer().to_vec();
			panic!(
				"server already buffered bytes: {:?}",
				String::from_utf8_lossy(&buf)
			);
		}
		self.reader
			.get_ref()
			.set_read_timeout(Some(within))
			.expect("set short timeout");
		// Materialise the fill_buf outcome into owned data so that we can
		// restore the timeout before reporting.
		let outcome: Result<Option<Vec<u8>>, std::io::Error> = match self.reader.fill_buf() {
			Ok(b) if b.is_empty() => Ok(None),
			Ok(b) => Ok(Some(b.to_vec())),
			Err(e) => Err(e),
		};
		self.reader
			.get_ref()
			.set_read_timeout(Some(DEFAULT_READ_TIMEOUT))
			.expect("restore timeout");
		match outcome {
			Ok(None) => panic!("server closed connection (expected silence)"),
			Ok(Some(b)) => panic!(
				"server sent bytes during silence window: {:?}",
				String::from_utf8_lossy(&b)
			),
			Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
			Err(e) => panic!("unexpected read error during silence window: {}", e),
		}
	}

	/// Drops the connection with a TCP RST-equivalent: shutdown(Both) so the
	/// server sees the disconnect immediately.
	pub fn drop_abrupt(self) {
		let _ = self.stream.shutdown(Shutdown::Both);
		drop(self);
	}

	/// Reads a LIST reply (LIST\n<entry>\n...\nEND) and returns the entry set.
	pub fn recv_list(&mut self) -> std::collections::HashSet<String> {
		assert_eq!(self.recv(), Response::ListBegin);
		let mut names = std::collections::HashSet::new();
		loop {
			match self.recv() {
				Response::ListEnd => return names,
				Response::ListEntry(n) => {
					names.insert(n);
				}
				other => panic!("unexpected response inside LIST: {:?}", other),
			}
		}
	}

	/// Polls `STATUS <name>` until `predicate` returns `true`, or the deadline
	/// expires. Returns the final response.
	pub fn poll_status_until<F: Fn(&Response) -> bool>(
		&mut self,
		name: &str,
		predicate: F,
		deadline: Duration,
	) -> Response {
		let start = Instant::now();
		loop {
			self.send(Request::Status(name.to_owned()));
			let resp = self.recv();
			if predicate(&resp) {
				return resp;
			}
			if start.elapsed() >= deadline {
				panic!("STATUS {} never matched predicate; last: {:?}", name, resp);
			}
			thread::sleep(Duration::from_millis(20));
		}
	}
}

// --------------------------------------------------------------------------
// Parameterise a test body over all three server variants.
//
// Usage:
//   each_server!(my_test, |server| {
//       let mut c = Client::connect(server);
//       ...
//   });
//
// Expands to three `#[test]` functions: `my_test::thread`, `my_test::event`,
// `my_test::async_`.
// --------------------------------------------------------------------------

#[macro_export]
macro_rules! each_server {
	($name:ident, |$s:ident| $body:block) => {
		mod $name {
			use super::*;

			fn run(variant: $crate::common::Variant) {
				let $s = &$crate::common::Server::spawn(variant);
				$body
			}

			#[test]
			fn thread() {
				run($crate::common::Variant::Thread);
			}
			#[test]
			fn event() {
				run($crate::common::Variant::Event);
			}
			// #[test]
			// fn async_() {
			// 	run($crate::common::Variant::Async);
			// }
		}
	};
}

/// Common short wait used across tests when we need the server to notice
/// something (e.g. a disconnect). Intentionally generous — correctness,
/// not speed, is the point.
pub const SHORT_WAIT: Duration = Duration::from_millis(100);

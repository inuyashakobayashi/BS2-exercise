//! Shared protocol and framing helpers for the lock service.
//!
//! # What's already done for you
//!
//! This entire file is provided so that you can focus on the learning
//! objectives of this exercise instead of on text parsing and line framing.
//! In particular:
//!
//! * The wire-protocol message types [`Request`] and [`Response`] are fully
//!   declared, parsed (`FromStr`) and formatted (`fmt::Display`). Their
//!   round-trip tests are green out-of-the-box.
//! * The error-message catalogue (`ERR_*` constants) is fixed. Use these
//!   constants from every server backend so that all three implementations
//!   emit byte-identical error replies.
//! * [`LineFramer`] is a complete, tested non-blocking line reader / buffered
//!   writer. You will need it for the mio-based event server. Read the doc
//!   comments if you are curious about the double-buffered write path.
//!
//! # What you need to implement
//!
//! Any two of the three server backends in `src/bin/`:
//! [`thread_server.rs`](bin/thread_server.rs),
//! [`event_server.rs`](bin/event_server.rs),
//! [`async_server.rs`](bin/async_server.rs).
//!
//! A working `client.rs` is also provided.
//!
//! # Protocol in a nutshell
//!
//! Every message is one `\n`-terminated line. A trailing `\r` is silently
//! trimmed on ingest so that telnet / `nc` (CR-LF) clients work out of the
//! box. Lock names are ASCII and match `[A-Za-z0-9_.:-]{1,64}`. Lines longer
//! than [`MAX_LINE`] bytes surface as a typed [`FrameError::LineTooLong`]
//! error so that the server can emit [`ERR_LINE_TOO_LONG`] and drop the
//! connection.
//!
//! ## Requests (client -> server)
//!
//! ```text
//! ACQUIRE <name>       # wait until held, then reply GRANTED
//! TRY_ACQUIRE <name>   # immediate GRANTED or DENIED
//! RELEASE <name>       # give up a held lock
//! STATUS <name>        # report holder / queue length
//! LIST                 # list all known locks
//! ```
//!
//! ## Responses (server -> client)
//!
//! ```text
//! OK                                         # generic success
//! GRANTED <name>                             # ACQUIRE / TRY_ACQUIRE succeeded
//! DENIED <name>                              # TRY_ACQUIRE failed
//! STATUS <name> FREE <waiters>               # unheld lock
//! STATUS <name> HELD <holder-id> <waiters>   # held lock
//! LIST\n<name1>\n...\nEND                    # multi-line list response
//! ERR <message>                              # see error catalogue
//! ```
//!
//! # Forward compatibility
//!
//! Clients silently ignore unknown trailing tokens on recognised response
//! verbs, and servers answer unknown requests with
//! `ERR unknown command <verb>` without closing the connection. Together
//! these rules let newer servers add optional arguments (or new verbs)
//! without breaking older clients.

use std::{
	fmt,
	io::{self, ErrorKind, Read, Write},
	str::FromStr,
};

// --------------------------------------------------------------------------
// Constants
// --------------------------------------------------------------------------

/// Maximum size in bytes of a single protocol line, excluding the
/// terminating `\n`. Anything longer surfaces as [`FrameError::LineTooLong`].
pub const MAX_LINE: usize = 4096;

/// Maximum length in bytes of a lock name.
pub const MAX_LOCK_NAME: usize = 64;

// ----- Error catalogue ----------------------------------------------------
//
// Defined as `pub const` strings so that every backend emits byte-identical
// error replies. Servers format unknown-command errors by concatenating
// `ERR_UNKNOWN_COMMAND_PREFIX` with the offending verb.

pub const ERR_INVALID_LOCK_NAME: &str = "ERR invalid lock name";
pub const ERR_LINE_TOO_LONG: &str = "ERR line too long";
pub const ERR_NOT_HELD: &str = "ERR not held";
pub const ERR_ALREADY_HELD: &str = "ERR already held";
pub const ERR_UNKNOWN_COMMAND_PREFIX: &str = "ERR unknown command ";

// --------------------------------------------------------------------------
// Core types
// --------------------------------------------------------------------------

/// Connection-scoped identity assigned by the server. Not a user concept,
/// each accepted connection gets a fresh id.
pub type ClientId = u64;

/// Returns `true` iff `name` is a syntactically valid lock name.
pub fn is_valid_lock_name(name: &str) -> bool {
	!name.is_empty()
		&& name.len() <= MAX_LOCK_NAME
		&& name
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

/// Client -> server message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
	Acquire(String),
	TryAcquire(String),
	Release(String),
	Status(String),
	List,
}

/// Server -> client message.
///
/// `LIST` replies are emitted as three separate variants ([`ListBegin`],
/// zero or more [`ListEntry`]s, [`ListEnd`]) so that every response
/// serialises to exactly one line. The lexer is therefore line-symmetric:
/// one `Response` per line.
///
/// [`ListBegin`]: Response::ListBegin
/// [`ListEntry`]: Response::ListEntry
/// [`ListEnd`]:   Response::ListEnd
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
	Ok,
	Granted(String),
	Denied(String),
	StatusFree {
		name: String,
		waiters: usize,
	},
	StatusHeld {
		name: String,
		holder: ClientId,
		waiters: usize,
	},
	ListBegin,
	ListEntry(String),
	ListEnd,
	Err(String),
}

/// Returned when a protocol line cannot be parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError;

impl fmt::Display for ParseError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("malformed protocol message")
	}
}

impl std::error::Error for ParseError {}

// --------------------------------------------------------------------------
// Parsing helpers
// --------------------------------------------------------------------------

/// Splits `s` at the first space. The head is always a `&str`; the tail is
/// `None` if `s` contains no space.
fn split_first(s: &str) -> (&str, Option<&str>) {
	match s.split_once(' ') {
		Some((head, tail)) => (head, Some(tail)),
		None => (s, None),
	}
}

/// Consumes exactly one lock-name token from `rest` and rejects any trailing
/// input. Used by the *request* parser (strict, the server must not guess
/// what an unknown trailing word means).
fn take_lock_name_strict(rest: Option<&str>) -> Result<String, ParseError> {
	let name = rest.ok_or(ParseError)?;
	if name.contains(' ') || !is_valid_lock_name(name) {
		return Err(ParseError);
	}
	Ok(name.to_owned())
}

/// Consumes the first space-separated token from `rest` as a lock name and
/// silently discards anything after it. Used by the *response* parser so that
/// newer servers can append optional trailing arguments without breaking
/// older clients.
fn take_lock_name_lenient(rest: Option<&str>) -> Result<String, ParseError> {
	let rest = rest.ok_or(ParseError)?;
	let (name, _ignored) = split_first(rest);
	if !is_valid_lock_name(name) {
		return Err(ParseError);
	}
	Ok(name.to_owned())
}

// --------------------------------------------------------------------------
// Parsing: Request (strict)
// --------------------------------------------------------------------------

impl FromStr for Request {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, ParseError> {
		let (verb, rest) = split_first(s);
		match verb {
			"ACQUIRE" => Ok(Request::Acquire(take_lock_name_strict(rest)?)),
			"TRY_ACQUIRE" => Ok(Request::TryAcquire(take_lock_name_strict(rest)?)),
			"RELEASE" => Ok(Request::Release(take_lock_name_strict(rest)?)),
			"STATUS" => Ok(Request::Status(take_lock_name_strict(rest)?)),
			"LIST" if rest.is_none() => Ok(Request::List),
			_ => Err(ParseError),
		}
	}
}

impl fmt::Display for Request {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Request::Acquire(n) => write!(f, "ACQUIRE {}", n),
			Request::TryAcquire(n) => write!(f, "TRY_ACQUIRE {}", n),
			Request::Release(n) => write!(f, "RELEASE {}", n),
			Request::Status(n) => write!(f, "STATUS {}", n),
			Request::List => f.write_str("LIST"),
		}
	}
}

// --------------------------------------------------------------------------
// Parsing: Response (lenient)
// --------------------------------------------------------------------------

impl FromStr for Response {
	type Err = ParseError;

	fn from_str(s: &str) -> Result<Self, ParseError> {
		let (verb, rest) = split_first(s);

		match verb {
			"OK" => Ok(Response::Ok),
			"LIST" => Ok(Response::ListBegin),
			"END" => Ok(Response::ListEnd),
			"ERR" => Ok(Response::Err(rest.unwrap_or("").to_owned())),

			"GRANTED" => Ok(Response::Granted(take_lock_name_lenient(rest)?)),
			"DENIED" => Ok(Response::Denied(take_lock_name_lenient(rest)?)),

			"STATUS" => parse_status(rest.ok_or(ParseError)?),

			// Fall-through: a bare lock name. The server is mid-LIST reply.
			_ if rest.is_none() && is_valid_lock_name(verb) => {
				Ok(Response::ListEntry(verb.to_owned()))
			}

			_ => Err(ParseError),
		}
	}
}

fn parse_status(rest: &str) -> Result<Response, ParseError> {
	let (name, rest) = split_first(rest);
	if !is_valid_lock_name(name) {
		return Err(ParseError);
	}
	let rest = rest.ok_or(ParseError)?;
	let (kind, rest) = split_first(rest);
	match kind {
		"FREE" => {
			let rest = rest.ok_or(ParseError)?;
			let (waiters_s, _tail) = split_first(rest);
			let waiters: usize = waiters_s.parse().map_err(|_| ParseError)?;
			Ok(Response::StatusFree {
				name: name.to_owned(),
				waiters,
			})
		}
		"HELD" => {
			let rest = rest.ok_or(ParseError)?;
			let (holder_s, rest) = split_first(rest);
			let rest = rest.ok_or(ParseError)?;
			let (waiters_s, _tail) = split_first(rest);
			let holder: ClientId = holder_s.parse().map_err(|_| ParseError)?;
			let waiters: usize = waiters_s.parse().map_err(|_| ParseError)?;
			Ok(Response::StatusHeld {
				name: name.to_owned(),
				holder,
				waiters,
			})
		}
		_ => Err(ParseError),
	}
}

impl fmt::Display for Response {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Response::Ok => f.write_str("OK"),
			Response::Granted(n) => write!(f, "GRANTED {}", n),
			Response::Denied(n) => write!(f, "DENIED {}", n),
			Response::StatusFree { name, waiters } => {
				write!(f, "STATUS {} FREE {}", name, waiters)
			}
			Response::StatusHeld {
				name,
				holder,
				waiters,
			} => write!(f, "STATUS {} HELD {} {}", name, holder, waiters),
			Response::ListBegin => f.write_str("LIST"),
			Response::ListEntry(n) => f.write_str(n),
			Response::ListEnd => f.write_str("END"),
			Response::Err(msg) => {
				if msg.is_empty() {
					f.write_str("ERR")
				} else {
					write!(f, "ERR {}", msg)
				}
			}
		}
	}
}

// --------------------------------------------------------------------------
// LineFramer: non-blocking line reader + buffered writer
//
// This is a complete, working implementation. You do not need to change
// anything below this line. The type is used by the mio-based event server
// to frame `\n`-terminated lines without blocking, and to buffer writes so
// that a short write (the socket reporting `WouldBlock`) does not drop
// bytes.
//
// Read the doc comments if you are interested in the trickier bits
// (compaction of the read buffer, the double-buffered writer).
// --------------------------------------------------------------------------

/// Errors surfaced by [`LineFramer::next_line`].
#[derive(Debug)]
pub enum FrameError {
	/// Underlying stream returned an error (typically `WouldBlock` is
	/// swallowed by [`LineFramer::refresh`], so anything we surface here is
	/// genuine).
	Io(io::Error),
	/// A pending line has grown past [`MAX_LINE`] bytes without seeing a
	/// `\n`. The caller is expected to emit [`ERR_LINE_TOO_LONG`] and drop
	/// the connection.
	LineTooLong,
	/// A completed line contained bytes that are not valid UTF-8.
	InvalidUtf8,
}

impl fmt::Display for FrameError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			FrameError::Io(e) => write!(f, "{}", e),
			FrameError::LineTooLong => f.write_str("protocol line exceeds maximum length"),
			FrameError::InvalidUtf8 => f.write_str("protocol line is not valid UTF-8"),
		}
	}
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
	fn from(e: io::Error) -> Self {
		FrameError::Io(e)
	}
}

/// Non-blocking line framer over a byte stream.
///
/// The reader keeps a growing ring-style buffer and compacts the front half
/// whenever more than half of it has been consumed. The writer is
/// double-buffered: new writes always go into the *back* buffer so that a
/// partial, `WouldBlock`-interrupted flush of the *front* buffer can resume
/// on the next call to [`flush_nonblocking`](LineFramer::flush_nonblocking)
/// without losing or reordering any bytes.
pub struct LineFramer<S> {
	stream: S,
	reader: Reader,
	writer: Writer,
}

struct Reader {
	buf: Vec<u8>,
	/// Index of the next unconsumed byte. Invariant: `tail <= buf.len()`.
	tail: usize,
}

struct Writer {
	bufs: [Vec<u8>; 2],
	/// Index of the buffer currently accepting `write` calls.
	active: usize,
	/// Position inside `bufs[active ^ 1]` (the drain buffer) that the next
	/// `flush_nonblocking` should resume at.
	cursor: usize,
}

impl<S> LineFramer<S> {
	pub fn new(stream: S) -> Self {
		Self {
			stream,
			reader: Reader {
				buf: Vec::new(),
				tail: 0,
			},
			writer: Writer {
				bufs: [Vec::new(), Vec::new()],
				active: 0,
				cursor: 0,
			},
		}
	}

	/// Returns `true` iff every byte previously accepted by `write` has
	/// been handed to the underlying stream.
	pub fn write_buffer_empty(&self) -> bool {
		self.writer.bufs[self.writer.active].is_empty()
			&& self.writer.cursor >= self.writer.bufs[self.writer.active ^ 1].len()
	}
}

impl<S> AsRef<S> for LineFramer<S> {
	fn as_ref(&self) -> &S {
		&self.stream
	}
}

impl<S> AsMut<S> for LineFramer<S> {
	fn as_mut(&mut self) -> &mut S {
		&mut self.stream
	}
}

impl<S: Read> LineFramer<S> {
	/// Pulls whatever bytes are currently available on the underlying
	/// stream into the read buffer. `WouldBlock` is swallowed silently.
	/// Call again once `poll` reports readiness.
	///
	/// Returns [`ErrorKind::UnexpectedEof`] once the peer has closed the
	/// connection so that the caller can treat it as a disconnect.
	pub fn refresh(&mut self) -> io::Result<()> {
		match self.stream.read_to_end(&mut self.reader.buf) {
			Ok(0) => Err(ErrorKind::UnexpectedEof.into()),
			Ok(_) => Ok(()),
			Err(e) if e.kind() == ErrorKind::WouldBlock => Ok(()),
			Err(e) => Err(e),
		}
	}
}

impl<S> LineFramer<S> {
	/// Returns the next complete line in the buffer, without its terminating
	/// `\n` (and without a trailing `\r`, if any). Returns `Ok(None)` if the
	/// buffer does not yet contain a full line.
	///
	/// Once the buffer contains [`MAX_LINE`] or more unconsumed bytes
	/// without a `\n`, this returns [`FrameError::LineTooLong`] *once* and
	/// resets internal state. The caller is expected to emit
	/// [`ERR_LINE_TOO_LONG`] and drop the connection.
	pub fn next_line(&mut self) -> Result<Option<String>, FrameError> {
		let slice = &self.reader.buf[self.reader.tail..];
		match slice.iter().position(|b| *b == b'\n') {
			Some(idx) => {
				let end = self.reader.tail + idx;
				// `end` points at the '\n'. Strip an optional preceding '\r'.
				let mut line_end = end;
				if line_end > self.reader.tail && self.reader.buf[line_end - 1] == b'\r' {
					line_end -= 1;
				}
				let bytes = self.reader.buf[self.reader.tail..line_end].to_vec();
				self.reader.tail = end + 1;
				self.reader.compact();
				match String::from_utf8(bytes) {
					Ok(line) => Ok(Some(line)),
					Err(_) => Err(FrameError::InvalidUtf8),
				}
			}
			None => {
				if slice.len() >= MAX_LINE {
					// Purge the buffer: the caller is expected to drop the
					// connection, so keeping the runaway bytes around only
					// wastes memory.
					self.reader.buf.clear();
					self.reader.tail = 0;
					Err(FrameError::LineTooLong)
				} else {
					Ok(None)
				}
			}
		}
	}
}

impl<S: Write> LineFramer<S> {
	/// Drains as much of the outbound buffer as the underlying stream will
	/// accept without blocking. Returns `Ok(true)` if the buffer is now
	/// empty, `Ok(false)` if bytes remain (e.g. the stream reported
	/// `WouldBlock`).
	pub fn flush_nonblocking(&mut self) -> io::Result<bool> {
		loop {
			let drain_idx = self.writer.active ^ 1;
			if self.writer.cursor >= self.writer.bufs[drain_idx].len() {
				// Drain buffer exhausted: swap roles. New writes will start
				// landing in what was the drain buffer; the freshly filled
				// buffer becomes the next drain target.
				if self.writer.bufs[self.writer.active].is_empty() {
					// Nothing left anywhere.
					return Ok(true);
				}
				self.writer.bufs[drain_idx].clear();
				self.writer.cursor = 0;
				self.writer.active = drain_idx;
				continue;
			}
			let buf = &self.writer.bufs[drain_idx][self.writer.cursor..];
			match self.stream.write(buf) {
				Ok(0) => return Err(ErrorKind::WriteZero.into()),
				Ok(n) => self.writer.cursor += n,
				Err(e) if e.kind() == ErrorKind::Interrupted => continue,
				Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(false),
				Err(e) => return Err(e),
			}
		}
	}
}

impl<S> Write for LineFramer<S> {
	/// Appends `buf` to the outbound queue without touching the underlying
	/// stream. Bytes are actually written on a subsequent call to
	/// [`flush_nonblocking`](LineFramer::flush_nonblocking).
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.writer.bufs[self.writer.active].extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()>
	where
		Self: Sized,
	{
		// Intentionally a no-op at this level: the caller controls flushing
		// through `flush_nonblocking`. A blanket impl of `flush` would have
		// to decide whether to spin on `WouldBlock`, which is exactly what
		// the caller of this type is trying to avoid.
		Ok(())
	}
}

impl Reader {
	fn compact(&mut self) {
		if self.buf.len() == self.tail {
			self.buf.clear();
			self.tail = 0;
		} else if self.buf.len() / 2 <= self.tail {
			self.buf.drain(0..self.tail);
			self.tail = 0;
		}
	}
}

// --------------------------------------------------------------------------
// Tests (sanity checks for the LineFramer)
// --------------------------------------------------------------------------

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::Cursor;

	#[test]
	fn line_framer_reads_split_lines() {
		let input: &[u8] = b"ACQUIRE foo\nRELEASE foo\r\nLIST\n";
		let mut fr = LineFramer::new(Cursor::new(input));
		fr.refresh().unwrap();
		assert_eq!(fr.next_line().unwrap().as_deref(), Some("ACQUIRE foo"));
		assert_eq!(fr.next_line().unwrap().as_deref(), Some("RELEASE foo"));
		assert_eq!(fr.next_line().unwrap().as_deref(), Some("LIST"));
		assert!(fr.next_line().unwrap().is_none());
	}

	#[test]
	fn line_framer_partial_line_returns_none() {
		let mut fr = LineFramer::new(Cursor::new(b"ACQUIRE fo".as_slice()));
		fr.refresh().unwrap();
		assert!(fr.next_line().unwrap().is_none());
	}

	#[test]
	fn line_framer_reports_oversized_line() {
		let huge: Vec<u8> = std::iter::repeat(b'A').take(MAX_LINE + 10).collect();
		let mut fr = LineFramer::new(Cursor::new(huge));
		fr.refresh().unwrap();
		match fr.next_line() {
			Err(FrameError::LineTooLong) => {}
			other => panic!("expected LineTooLong, got {:?}", other),
		}
		// Second call should behave as empty (buffer was purged).
		assert!(fr.next_line().unwrap().is_none());
	}

	#[test]
	fn line_framer_writes_go_through_double_buffer() {
		let mut fr: LineFramer<Vec<u8>> = LineFramer::new(Vec::new());
		fr.write_all(b"HELLO\n").unwrap();
		fr.write_all(b"WORLD\n").unwrap();
		assert!(!fr.write_buffer_empty());
		assert!(fr.flush_nonblocking().unwrap());
		assert!(fr.write_buffer_empty());
		assert_eq!(fr.as_ref().as_slice(), b"HELLO\nWORLD\n");
	}

	#[test]
	fn line_framer_preserves_bytes_across_flush_cycles() {
		// Simulates a scenario where a second write arrives *between* two
		// flush attempts. The double-buffer must preserve ordering.
		let mut fr: LineFramer<Vec<u8>> = LineFramer::new(Vec::new());
		fr.write_all(b"A").unwrap();
		assert!(fr.flush_nonblocking().unwrap());
		fr.write_all(b"B").unwrap();
		assert!(fr.flush_nonblocking().unwrap());
		fr.write_all(b"C").unwrap();
		assert!(fr.flush_nonblocking().unwrap());
		assert_eq!(fr.as_ref().as_slice(), b"ABC");
	}
}

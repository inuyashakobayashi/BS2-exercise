//! Interactive text client for the distributed lock service.
//!
//! Two threads share one socket via the `Read for &TcpStream` /
//! `Write for &TcpStream` trick, so no extra `Mutex` is needed:
//!
//! * A reader thread copies every line the server sends to stdout, prefixed
//!   with `< ` for readability.
//! * The main thread reads stdin line-by-line and forwards each line to the
//!   server.
//!
//! Shutdown is entirely side-effect-driven: when stdin hits EOF (Ctrl-D) the
//! main thread drops its handle to the socket, which closes the write half;
//! the server then drops the connection, the reader's `lines()` iterator
//! yields `None`, and its thread exits. Symmetrically, if the server closes
//! first, the reader sees EOF and exits, stdin reads will produce writes
//! that fail, terminating the main loop.
//!
//! Intentionally no SIGINT handler: hitting Ctrl-C aborts the process, the
//! kernel sends RST/FIN, and the server's disconnect path runs as designed.

use std::{
	env,
	io::{self, BufRead, BufReader, Write},
	net::{Shutdown, TcpStream},
	sync::Arc,
	thread,
};

fn main() -> io::Result<()> {
	let addr = env::args()
		.nth(1)
		.unwrap_or_else(|| "127.0.0.1:8000".into());
	let stream = Arc::new(TcpStream::connect(&addr)?);
	eprintln!("connected to {}", addr);

	// Reader thread: server -> stdout.
	let reader_stream = stream.clone();
	let reader = thread::spawn(move || {
		let reader = BufReader::new(&*reader_stream);
		for line in reader.lines() {
			match line {
				Ok(l) => println!("< {}", l),
				Err(_) => break,
			}
		}
	});

	// Main thread: stdin -> server.
	let stdin = io::stdin();
	let mut out = &*stream;
	for line in stdin.lock().lines() {
		let line = match line {
			Ok(l) => l,
			Err(_) => break,
		};
		if writeln!(out, "{}", line).is_err() {
			break;
		}
	}

	// Shut down only the write half so the server sees EOF and disconnects;
	// the kernel-level FIN then causes the reader thread's `lines()` to
	// return None, which joins it cleanly.
	let _ = stream.shutdown(Shutdown::Write);
	let _ = reader.join();
	Ok(())
}

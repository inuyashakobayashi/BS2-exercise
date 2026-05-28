//! End-to-end integration tests for the three lock-server variants.
//!
//! Each test spawns the server binary under test, connects one or more
//! clients over TCP, and asserts on the line-based wire protocol directly.
//! The `each_server!` macro runs every scenario against `thread_server`,
//! `event_server`, and `async_server` in turn, so a failure localises to
//! exactly one (variant, scenario) pair.
//!
//! Run with: `cargo test --test integration`.

mod common;

use std::{thread, time::Duration};

use locklib::{Request, Response};

use crate::common::{Client, SHORT_WAIT};

// --------------------------------------------------------------------------
// 1. Protocol smoke test: a single client walks through every verb.
// --------------------------------------------------------------------------

each_server!(protocol_smoke, |server| {
	let mut c = Client::connect(server);

	c.send(Request::Acquire("foo".into()));
	c.expect(Response::Granted("foo".into()));

	c.send(Request::Status("foo".into()));
	match c.recv() {
		Response::StatusHeld {
			name,
			holder: _,
			waiters,
		} => {
			assert_eq!(name, "foo");
			assert_eq!(waiters, 0);
		}
		other => panic!("expected HELD status, got {:?}", other),
	}

	c.send(Request::List);
	let names = c.recv_list();
	assert!(names.contains("foo"), "LIST missing held lock: {:?}", names);

	c.send(Request::Release("foo".into()));
	c.expect(Response::Ok);

	c.send(Request::Status("foo".into()));
	c.expect(Response::StatusFree {
		name: "foo".into(),
		waiters: 0,
	});
});

// --------------------------------------------------------------------------
// 2. TRY_ACQUIRE on a contended lock is denied immediately; a subsequent
//    blocking ACQUIRE completes only after the holder releases.
// --------------------------------------------------------------------------

each_server!(try_acquire_contention, |server| {
	let mut a = Client::connect(server);
	a.send(Request::Acquire("x".into()));
	a.expect(Response::Granted("x".into()));

	let mut b = Client::connect(server);
	b.send(Request::TryAcquire("x".into()));
	b.expect(Response::Denied("x".into()));

	b.send(Request::Acquire("x".into()));
	b.expect_silent_for(SHORT_WAIT);

	a.send(Request::Release("x".into()));
	a.expect(Response::Ok);

	b.expect(Response::Granted("x".into()));
});

// --------------------------------------------------------------------------
// 3. FIFO ordering: the client that queued first is granted first.
// --------------------------------------------------------------------------

each_server!(fifo_two_waiters, |server| {
	let mut a = Client::connect(server);
	let mut b = Client::connect(server);
	let mut c = Client::connect(server);
	let mut obs = Client::connect(server);

	a.send(Request::Acquire("x".into()));
	a.expect(Response::Granted("x".into()));

	// Enqueue B, then wait for the server to record one waiter. Polling is
	// the only way to pin the order: otherwise C's request could race B's
	// into the queue.
	b.send(Request::Acquire("x".into()));
	obs.poll_status_until(
		"x",
		|r| matches!(r, Response::StatusHeld { waiters: 1, .. }),
		Duration::from_secs(2),
	);

	c.send(Request::Acquire("x".into()));
	obs.poll_status_until(
		"x",
		|r| matches!(r, Response::StatusHeld { waiters: 2, .. }),
		Duration::from_secs(2),
	);

	a.send(Request::Release("x".into()));
	a.expect(Response::Ok);
	b.expect(Response::Granted("x".into()));

	// C must still be parked while B holds.
	c.expect_silent_for(SHORT_WAIT);

	b.send(Request::Release("x".into()));
	b.expect(Response::Ok);
	c.expect(Response::Granted("x".into()));
});

// --------------------------------------------------------------------------
// 4. A disconnecting client releases every lock it was holding; a new
//    client can then acquire them immediately.
// --------------------------------------------------------------------------

each_server!(disconnect_releases_held, |server| {
	let mut a = Client::connect(server);
	a.send(Request::Acquire("x".into()));
	a.expect(Response::Granted("x".into()));
	a.send(Request::Acquire("y".into()));
	a.expect(Response::Granted("y".into()));

	a.drop_abrupt();

	let mut b = Client::connect(server);
	b.poll_status_until(
		"x",
		|r| matches!(r, Response::StatusFree { .. }),
		Duration::from_secs(2),
	);
	b.poll_status_until(
		"y",
		|r| matches!(r, Response::StatusFree { .. }),
		Duration::from_secs(2),
	);

	b.send(Request::Acquire("x".into()));
	b.expect(Response::Granted("x".into()));
	b.send(Request::Acquire("y".into()));
	b.expect(Response::Granted("y".into()));
});

// --------------------------------------------------------------------------
// 5. A client that disconnects *while waiting* must not be handed the lock.
//    Instead, the next real waiter (C) gets it.
//
// Note the asymmetry across variants: the event and async servers notice
// B's disconnect immediately (they were reading from the socket). The
// thread server only notices it *indirectly*. A's RELEASE wakes B's
// worker, which then fails to write GRANTED, which finally triggers the
// cleanup that promotes C. Either way, only C may receive GRANTED x.
// --------------------------------------------------------------------------

each_server!(disconnect_removes_waiter, |server| {
	let mut a = Client::connect(server);
	let mut b = Client::connect(server);
	let mut c = Client::connect(server);
	let mut obs = Client::connect(server);

	a.send(Request::Acquire("x".into()));
	a.expect(Response::Granted("x".into()));

	b.send(Request::Acquire("x".into()));
	obs.poll_status_until(
		"x",
		|r| matches!(r, Response::StatusHeld { waiters, .. } if *waiters >= 1),
		Duration::from_secs(2),
	);

	b.drop_abrupt();

	c.send(Request::Acquire("x".into()));
	// Give the server a moment to register C in the queue. We deliberately do
	// not assert on STATUS waiters here: thread_server may carry a phantom B
	// until A releases.
	thread::sleep(SHORT_WAIT);

	a.send(Request::Release("x".into()));
	a.expect(Response::Ok);

	c.expect(Response::Granted("x".into()));
});

// --------------------------------------------------------------------------
// 6. Releasing a lock that the client does not hold is an error, not a
//    silent no-op.
// --------------------------------------------------------------------------

each_server!(release_not_held_errors, |server| {
	let mut c = Client::connect(server);
	c.send(Request::Release("foo".into()));
	match c.recv() {
		Response::Err(msg) => assert!(
			msg.to_lowercase().contains("not held"),
			"unexpected error: {:?}",
			msg
		),
		other => panic!("expected ERR not held, got {:?}", other),
	}
});

// --------------------------------------------------------------------------
// 7. A client re-acquiring a lock it already holds is an error. Pins the
//    current server behaviour so we notice if it ever drifts.
// --------------------------------------------------------------------------

each_server!(reacquire_same_client_errors, |server| {
	let mut c = Client::connect(server);
	c.send(Request::Acquire("foo".into()));
	c.expect(Response::Granted("foo".into()));
	c.send(Request::Acquire("foo".into()));
	match c.recv() {
		Response::Err(msg) => assert!(
			msg.to_lowercase().contains("already held"),
			"unexpected error: {:?}",
			msg
		),
		other => panic!("expected ERR already held, got {:?}", other),
	}
});

// --------------------------------------------------------------------------
// 8. Lock-name validation rejects illegal characters, oversized names, and
//    empty names at the wire-protocol level. We bypass `Request::Display`
//    via `send_raw` because the typed request constructor does no
//    validation on its own.
// --------------------------------------------------------------------------

each_server!(invalid_lock_name, |server| {
	let mut c = Client::connect(server);

	// Path traversal-shaped input contains '/' which is not in the allow-set.
	c.send_raw("ACQUIRE ../etc/passwd");
	assert_err_contains(&mut c, "invalid lock name");

	// Trailing space -> empty name token.
	c.send_raw("ACQUIRE ");
	assert_err_contains(&mut c, "invalid lock name");

	// Over-sized name.
	let long: String = std::iter::repeat('a').take(65).collect();
	c.send_raw(&format!("ACQUIRE {}", long));
	assert_err_contains(&mut c, "invalid lock name");
});

// --------------------------------------------------------------------------
// 9. The canonical requirement from the task description: three clients
//    hammering the same lock must all complete without deadlock.
// --------------------------------------------------------------------------

each_server!(three_client_contention_smoke, |server| {
	let addr = server.addr;
	let rounds_per_client = 5;

	let handles: Vec<_> = (0..3)
		.map(|_| {
			thread::spawn(move || {
				let mut c = Client::connect_addr(addr);
				for _ in 0..rounds_per_client {
					c.send(Request::Acquire("shared".into()));
					c.expect(Response::Granted("shared".into()));
					// Hold briefly so the other threads actually contend.
					thread::sleep(Duration::from_millis(5));
					c.send(Request::Release("shared".into()));
					c.expect(Response::Ok);
				}
			})
		})
		.collect();

	for h in handles {
		h.join().expect("client thread completed");
	}

	// The lock must end up free with no residual waiters.
	let mut probe = Client::connect(server);
	probe.send(Request::Status("shared".into()));
	probe.expect(Response::StatusFree {
		name: "shared".into(),
		waiters: 0,
	});
});

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

fn assert_err_contains(c: &mut Client, needle: &str) {
	match c.recv() {
		Response::Err(msg) => assert!(
			msg.to_lowercase().contains(needle),
			"expected error containing {:?}, got {:?}",
			needle,
			msg
		),
		other => panic!("expected ERR, got {:?}", other),
	}
}

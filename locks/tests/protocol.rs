//! Protocol-level round-trip tests.
//!
//! These only exercise the `FromStr` + `Display` pair on `Request` and
//! `Response`; no network, no binaries.
//!
//! Run with: `cargo test --test protocol`.

use locklib::{MAX_LOCK_NAME, Request, Response};

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

fn roundtrip_req(req: Request) {
	let wire = req.to_string();
	let parsed: Request = wire.parse().expect("request round-trip");
	assert_eq!(parsed, req);
}

fn roundtrip_resp(resp: Response) {
	let wire = resp.to_string();
	let parsed: Response = wire.parse().expect("response round-trip");
	assert_eq!(parsed, resp);
}

// --------------------------------------------------------------------------
// Request
// --------------------------------------------------------------------------

#[test]
fn request_roundtrip_all_variants() {
	roundtrip_req(Request::Acquire("foo".into()));
	roundtrip_req(Request::TryAcquire("bar.baz-qux_1".into()));
	roundtrip_req(Request::Release("ns:lock".into()));
	roundtrip_req(Request::Status("A".into()));
	roundtrip_req(Request::List);
}

#[test]
fn request_rejects_malformed() {
	assert!("".parse::<Request>().is_err());
	assert!("BOGUS".parse::<Request>().is_err());
	assert!("ACQUIRE".parse::<Request>().is_err());
	assert!("ACQUIRE foo bar".parse::<Request>().is_err());
	assert!("LIST extra".parse::<Request>().is_err());
	assert!("ACQUIRE foo!".parse::<Request>().is_err());
	assert!("ACQUIRE ".parse::<Request>().is_err());
}

#[test]
fn request_rejects_oversized_name() {
	let name: String = std::iter::repeat('a').take(MAX_LOCK_NAME + 1).collect();
	let wire = format!("ACQUIRE {}", name);
	assert!(wire.parse::<Request>().is_err());
}

// --------------------------------------------------------------------------
// Response
// --------------------------------------------------------------------------

#[test]
fn response_roundtrip_all_variants() {
	roundtrip_resp(Response::Ok);
	roundtrip_resp(Response::Granted("foo".into()));
	roundtrip_resp(Response::Denied("foo".into()));
	roundtrip_resp(Response::StatusFree {
		name: "x".into(),
		waiters: 0,
	});
	roundtrip_resp(Response::StatusHeld {
		name: "x".into(),
		holder: 7,
		waiters: 3,
	});
	roundtrip_resp(Response::ListBegin);
	roundtrip_resp(Response::ListEntry("foo".into()));
	roundtrip_resp(Response::ListEnd);
	roundtrip_resp(Response::Err("invalid lock name".into()));
}

#[test]
fn response_ignores_unknown_trailing_tokens() {
	// Forward-compat contract: client silently tolerates extra tokens on a
	// recognised verb, so a newer server can extend the wire format.
	let parsed: Response = "GRANTED foo surprise arg".parse().unwrap();
	assert_eq!(parsed, Response::Granted("foo".into()));

	let parsed: Response = "STATUS foo HELD 7 3 extra".parse().unwrap();
	assert_eq!(
		parsed,
		Response::StatusHeld {
			name: "foo".into(),
			holder: 7,
			waiters: 3,
		}
	);
}

#[test]
fn response_rejects_malformed() {
	assert!("STATUS foo HELD".parse::<Response>().is_err());
	assert!("STATUS foo HELD notanumber 0".parse::<Response>().is_err());
	assert!("STATUS".parse::<Response>().is_err());
	assert!("GRANTED".parse::<Response>().is_err());
	assert!("GRANTED bad!name".parse::<Response>().is_err());
}

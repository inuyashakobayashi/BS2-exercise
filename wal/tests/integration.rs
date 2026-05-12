use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tempfile::NamedTempFile;

use wal::Store;

// 1. Round-trip ------------------------------------------------------------

#[test]
fn round_trip_reopen() {
	let db_path = NamedTempFile::new().expect("tempfile");
	{
		let mut db: Store<String, String> = Store::open(&db_path).unwrap();
		for i in 0..100 {
			db.set(format!("k{i:03}"), format!("v{i}")).unwrap();
		}
	}
	let db: Store<String, String> = Store::open(&db_path).unwrap();
	assert_eq!(db.len(), 100);
	for i in 0..100 {
		assert_eq!(db.get(&format!("k{i:03}")), Some(&format!("v{i}")));
	}
}

// 2. Scan ordering ---------------------------------------------------------

#[test]
fn scan_returns_sorted_entries() {
	let db_path = NamedTempFile::new().expect("tempfile");
	let mut db: Store<String, u32> = Store::open(&db_path).unwrap();
	for k in ["mango", "apple", "cherry", "noodle", "banana"] {
		db.set(k.to_string(), k.len() as u32).unwrap();
	}

	let all: Vec<&str> = db.scan(..).map(|(k, _)| k.as_str()).collect();
	assert_eq!(all, vec!["apple", "banana", "cherry", "mango", "noodle"]);

	let range: Vec<&str> = db
		.scan("m".to_string()..="n".to_string())
		.map(|(k, _)| k.as_str())
		.collect();
	assert_eq!(range, vec!["mango"]);
}

// 3. Delete persistence ----------------------------------------------------

#[test]
fn delete_persists_across_reopen() {
	let db_path = NamedTempFile::new().expect("tempfile");
	{
		let mut db: Store<String, String> = Store::open(&db_path).unwrap();
		db.set("a".into(), "1".into()).unwrap();
		db.set("b".into(), "2".into()).unwrap();
	}
	{
		let mut db: Store<String, String> = Store::open(&db_path).unwrap();
		db.delete(&"a".to_string()).unwrap();
	}
	let db: Store<String, String> = Store::open(&db_path).unwrap();
	assert_eq!(db.get(&"a".to_string()), None);
	assert_eq!(db.get(&"b".to_string()), Some(&"2".into()));
}

// 4. An empty store reopens as empty. --------------------------------------

#[test]
fn empty_store_reopens() {
	let db_path = NamedTempFile::new().expect("tempfile");
	{
		let _db: Store<String, String> = Store::open(&db_path).unwrap();
	}
	let db: Store<String, String> = Store::open(&db_path).unwrap();
	assert_eq!(db.len(), 0);
	assert!(db.is_empty());
	assert!(db.scan(..).next().is_none());
}

// 5. Overwriting the same key: the latest value wins after replay. ---------

#[test]
fn overwrite_same_key_latest_wins_after_replay() {
	let db_path = NamedTempFile::new().expect("tempfile");
	{
		let mut db: Store<String, u32> = Store::open(&db_path).unwrap();
		db.set("k".into(), 1).unwrap();
		db.set("k".into(), 2).unwrap();
		db.set("k".into(), 3).unwrap();
	}
	let db: Store<String, u32> = Store::open(&db_path).unwrap();
	assert_eq!(db.get(&"k".to_string()), Some(&3));
	assert_eq!(db.len(), 1);
}

// 6. Torn writes — sweep-based recovery property ---------------------------
//
// Property: for every byte position `cut` in the WAL, truncating the file
// to that length must result in the store reopening to *some* state that
// was committed at some point during the original op sequence. Equivalently:
// the recovered state is always a valid prefix of the committed history.
//
// This test is **format-agnostic** — it never inspects the on-disk byte
// layout. Whether you choose postcard, JSON, fixed-length records, or any
// other framing, the property must hold.
//
// One sweep replaces three pinpoint torn-write / CRC tests because it
// covers every cut position (mid-length-prefix, mid-checksum, mid-body,
// frame boundary, tail padding…) without needing implementation knowledge.

#[test]
fn any_truncation_recovers_to_a_committed_prefix() {
	let db_path = NamedTempFile::new().expect("tempfile");

	// A mix of set / overwrite / delete so successive states genuinely diverge.
	let ops: Vec<(String, Option<String>)> = vec![
		("alpha".into(),   Some("1".into())),
		("bravo".into(),   Some("two".into())),
		("charlie".into(), Some("longer-value-here".into())),
		("alpha".into(),   None),                       // delete
		("delta".into(),   Some("4".into())),
		("bravo".into(),   Some("two-revised".into())), // overwrite
	];

	// Snapshot the expected map after each commit, plus the empty pre-state.
	let mut expected: Vec<BTreeMap<String, String>> = vec![BTreeMap::new()];
	{
		let mut db: Store<String, String> = Store::open(&db_path).unwrap();
		let mut state = BTreeMap::new();
		for (k, v) in &ops {
			match v {
				Some(v) => {
					db.set(k.clone(), v.clone()).unwrap();
					state.insert(k.clone(), v.clone());
				}
				None => {
					db.delete(k).unwrap();
					state.remove(k);
				}
			}
			expected.push(state.clone());
		}
	}

	let full = fs::read(&db_path).unwrap();

	// Sanity check: the full file must reopen to the final committed state.
	assert_eq!(
		recover(db_path.path(), &full),
		Some(expected.last().cloned().unwrap()),
		"full file failed to reopen to the final state",
	);

	// Sweep every truncation position. Each one must either Err on open
	// (the implementation may reject byte ranges it cannot make sense of)
	// or recover to one of the snapshotted prefixes.
	for cut in 0..full.len() {
		match recover(db_path.path(), &full[..cut]) {
			None => continue, // implementation rejected — allowed
			Some(observed) => assert!(
				expected.contains(&observed),
				"cut={cut} of {}: recovered state {:?} is not any committed prefix",
				full.len(),
				observed,
			),
		}
	}
}

fn recover(path: &Path, bytes: &[u8]) -> Option<BTreeMap<String, String>> {
	fs::write(path, bytes).unwrap();
	let db: Store<String, String> = Store::open(path).ok()?;
	Some(db.scan(..).map(|(k, v)| (k.clone(), v.clone())).collect())
}

// 7. Compaction ------------------------------------------------------------

#[test]
fn compaction_shrinks_log_and_preserves_state() {
	let db_path = NamedTempFile::new().expect("tempfile");
	let mut db: Store<String, String> = Store::open(&db_path).unwrap();
	for i in 0..50 {
		db.set(format!("k{i:02}"), format!("v{i}")).unwrap();
	}
	db.delete(&"k07".to_string()).unwrap();

	let size_before = fs::metadata(&db_path).unwrap().len();
	db.compact().unwrap();
	let size_after = fs::metadata(&db_path).unwrap().len();

	// Compaction must physically shrink the log: 50 sets + 1 delete versus
	// 49 fresh entries means strictly fewer bytes on disk.
	assert!(
		size_after < size_before,
		"compact() did not shrink the log: {size_before} → {size_after}",
	);

	// Mutations after compaction still go through the log.
	db.set("k99".into(), "v99".into()).unwrap();
	drop(db);

	let db: Store<String, String> = Store::open(&db_path).unwrap();
	assert_eq!(db.get(&"k07".to_string()), None);
	assert_eq!(db.get(&"k99".to_string()), Some(&"v99".into()));
	assert_eq!(db.len(), 50); // 50 - 1 deleted + 1 added after compact
	for i in 0..50 {
		if i == 7 {
			continue;
		}
		assert_eq!(db.get(&format!("k{i:02}")).cloned(), Some(format!("v{i}")));
	}
}

// 8. Crash during compaction -----------------------------------------------

#[test]
fn orphan_tmp_is_ignored_on_open() {
	let db_path = NamedTempFile::new().expect("tempfile");
	{
		let mut db: Store<String, String> = Store::open(&db_path).unwrap();
		db.set("k".into(), "v".into()).unwrap();
		db.compact().unwrap();
		db.set("k2".into(), "v2".into()).unwrap();
	}
	// Simulate a half-finished next compaction.
	fs::write(db_path.path().with_extension("tmp"), b"garbage not a store").unwrap();

	let db: Store<String, String> = Store::open(&db_path).unwrap();
	assert_eq!(db.get(&"k".into()), Some(&"v".into()));
	assert_eq!(db.get(&"k2".into()), Some(&"v2".into()));
	assert!(!db_path.path().with_extension("tmp").exists());
}

// 9. gfs-lite shape: u64 keys, byte-vector values --------------------------

#[test]
fn stores_u64_keyed_chunks() {
	let db_path = NamedTempFile::new().expect("tempfile");
	let payload_a = vec![0xAAu8; 4 * 1024 * 1024];
	let payload_b = vec![0xBBu8; 4 * 1024 * 1024];
	{
		let mut db: Store<u64, Vec<u8>> = Store::open(&db_path).unwrap();
		db.set(1, payload_a.clone()).unwrap();
		db.set(2, payload_b.clone()).unwrap();
		db.set(3, vec![]).unwrap();
	}
	let db: Store<u64, Vec<u8>> = Store::open(&db_path).unwrap();
	assert_eq!(db.get(&1).map(|v| v.len()), Some(payload_a.len()));
	assert_eq!(db.get(&1).unwrap()[0], 0xAA);
	assert_eq!(db.get(&2).unwrap()[0], 0xBB);
	assert_eq!(db.get(&3).map(|v| v.len()), Some(0));

	let ids: Vec<u64> = db.scan(..).map(|(k, _)| *k).collect();
	assert_eq!(ids, vec![1, 2, 3]);
}

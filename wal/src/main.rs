use std::io::{self, BufRead, IsTerminal, Write};
use std::{env, process};

use wal::Store;

fn main() {
	let dir = env::args().nth(1).unwrap_or_else(|| "wal.db".into());
	let mut store: Store<String, String> = Store::open(&dir).unwrap_or_else(|e| {
		eprintln!("error: cannot open store at {dir:?}: {e}");
		process::exit(1);
	});

	let stdin = io::stdin();
	let stdout = io::stdout();
	let interactive = stdin.is_terminal();

	if interactive {
		println!("WAL store at {dir:?}  (type 'help' for commands)");
	}

	loop {
		if interactive {
			print!("> ");
			stdout.lock().flush().ok();
		}

		let mut line = String::new();
		if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
			break;
		}

		let line = line.trim();
		if line.is_empty() {
			continue;
		}

		// Split into at most three tokens: command, first arg, rest (so that
		// values in `set` may contain spaces).
		let mut parts = line.splitn(3, ' ');
		let cmd = parts.next().unwrap_or("");
		let arg1 = parts.next().unwrap_or("").to_string();
		let arg2 = parts.next().unwrap_or("").to_string();

		let mut out = stdout.lock();

		match cmd {
			"get" => {
				if arg1.is_empty() {
					writeln!(out, "usage: get <key>").ok();
				} else {
					match store.get(&arg1) {
						Some(v) => writeln!(out, "{v}").ok(),
						None => writeln!(out, "(nil)").ok(),
					};
				}
			}
			"set" => {
				if arg1.is_empty() || arg2.is_empty() {
					writeln!(out, "usage: set <key> <value>").ok();
				} else {
					match store.set(arg1, arg2) {
						Ok(()) => writeln!(out, "ok").ok(),
						Err(e) => writeln!(out, "error: {e}").ok(),
					};
				}
			}
			"delete" | "del" => {
				if arg1.is_empty() {
					writeln!(out, "usage: delete <key>").ok();
				} else {
					match store.delete(&arg1) {
						Ok(()) => writeln!(out, "ok").ok(),
						Err(e) => writeln!(out, "error: {e}").ok(),
					};
				}
			}
			"scan" => {
				let mut count = 0usize;
				if arg1.is_empty() {
					for (k, v) in store.scan(..) {
						writeln!(out, "{k} = {v}").ok();
						count += 1;
					}
				} else if arg2.is_empty() {
					for (k, v) in store.scan(arg1..) {
						writeln!(out, "{k} = {v}").ok();
						count += 1;
					}
				} else {
					for (k, v) in store.scan(arg1..=arg2) {
						writeln!(out, "{k} = {v}").ok();
						count += 1;
					}
				}
				writeln!(out, "({count} entries)").ok();
			}
			"compact" => {
				match store.compact() {
					Ok(()) => writeln!(out, "compacted").ok(),
					Err(e) => writeln!(out, "error: {e}").ok(),
				};
			}
			"len" => {
				writeln!(out, "{}", store.len()).ok();
			}
			"help" => {
				write!(
					out,
					r#"
  get <key>              look up a key
  set <key> <value>      insert or overwrite (value may contain spaces)
  delete <key>           remove a key
  scan [from [to]]       ascending range scan (inclusive bounds)
  compact                replace the log with a fresh snapshot
  len                    number of entries
  quit                   exit
"#
				)
				.ok();
			}
			"quit" | "exit" | "q" => break,
			other => {
				writeln!(out, "unknown command {other:?}; type 'help'").ok();
			}
		}
	}
}

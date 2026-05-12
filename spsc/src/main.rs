use spsc::*;
use std::thread;

fn main() {
	let (px, cx) = channel(1);

	thread::spawn(move || {
		px.send("Ping").unwrap();
	});

	println!("recv: {}", cx.recv().unwrap());
}

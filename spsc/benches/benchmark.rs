//! Throughput comparison between our SPSC and `std::sync::mpsc`.
//!
//! The Criterion HTML report under `target/criterion/report/index.html`
//! visualises the margins across capacity × batch-size combinations.

use std::{sync::mpsc, thread};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn run_spsc(capacity: usize, count: usize) -> usize {
	let (px, cx) = spsc::channel(capacity);

	let producer = thread::spawn(move || {
		for i in 0..count {
			px.send(i).unwrap();
		}
	});

	let consumer = thread::spawn(move || {
		let mut sum = 0usize;
		while let Ok(i) = cx.recv() {
			sum += i;
		}
		sum
	});

	producer.join().unwrap();
	consumer.join().unwrap()
}

fn run_mpsc(count: usize) -> usize {
	let (sx, rx) = mpsc::channel();

	let producer = thread::spawn(move || {
		for i in 0..count {
			sx.send(i).unwrap();
		}
	});

	let consumer = thread::spawn(move || {
		let mut sum = 0usize;
		while let Ok(i) = rx.recv() {
			sum += i;
		}
		sum
	});

	producer.join().unwrap();
	consumer.join().unwrap()
}

fn spsc_vs_mpsc(c: &mut Criterion) {
	let mut group = c.benchmark_group("spsc_vs_mpsc");

	for &count in &[1 << 8, 1 << 10, 1 << 12, 1 << 14] {
		for &capacity in &[16usize, 256, 4096] {
			group.bench_with_input(
				BenchmarkId::new(format!("spsc/cap={capacity}"), count),
				&count,
				|b, &n| b.iter(|| run_spsc(capacity, n)),
			);
		}
		group.bench_with_input(BenchmarkId::new("mpsc", count), &count, |b, &n| {
			b.iter(|| run_mpsc(n))
		});
	}

	group.finish();
}

criterion_group!(benches, spsc_vs_mpsc);
criterion_main!(benches);

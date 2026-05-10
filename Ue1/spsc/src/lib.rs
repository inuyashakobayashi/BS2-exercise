//! Lock-free SPSC channel (Single Producer, Single Consumer).
//!
//! This crate is the skeleton for the first exercise. The public API below is
//! fixed; the choice of internal data structure (ring buffer, linked list, ...)
//! is yours. Refer to the task description for the exact semantics, especially
//! regarding full/empty channel behaviour and drop/panic safety.

#![allow(unused_variables)]
use std::cell::UnsafeCell; // Provides interior mutability.
use std::mem::MaybeUninit; // Allocates a block of memory without immediately initializing it.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
}; // Atomic Reference Counted pointer and Primitives for thread-safe lock-free programming.
use std::{fmt, marker::PhantomData};

// ---------------------------------------------------------------------------
// RingBuffer for Data Storage
// ---------------------------------------------------------------------------
struct RingBuffer<T> {
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    cap: usize,
    head: AtomicUsize,
    tail: AtomicUsize,
    producer_alive: AtomicBool,
    consumer_alive: AtomicBool,
}

impl<T> RingBuffer<T> {
    fn new(cap: usize) -> Self {
        // Create uninitialized slots with the number of cap.
        let slots = (0..cap)
            .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        RingBuffer {
            slots,
            cap,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            producer_alive: AtomicBool::new(true),
            consumer_alive: AtomicBool::new(true),
        }
    }

    /// The caller must ensure that the slot at `idx` is currently empty.
    unsafe fn write(&self, idx: usize, value: T) {
        let slot = &self.slots[idx % self.cap];
        unsafe {
            (*slot.get()).write(value);
        }
    }

    /// The caller must ensure that the slot at `idx` has been written to.
    unsafe fn read(&self, idx: usize) -> T {
        let slot = &self.slots[idx % self.cap];
        unsafe { (*slot.get()).assume_init_read() }
    }
}

// 1. the Producer only modifies `head` and writes to empty slots,
// 2. the Consumer only modifies `tail` and reads filled slots.
// 3. We use atomic counters (`head` and `tail`) with `Acquire`/`Release` memory
//    ordering, preventing any data races on memory slots.
// 4. As long as the data `T` is safe to transfer between threads (`T: Send`),
//    sharing the buffer itself across threads is safe.
unsafe impl<T: Send> Sync for RingBuffer<T> {}

// If a panic occurs, Rust calls the `drop` method to clean up, otherwise normal drop.
impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        struct PanicGuard<'a, T> {
            buf: &'a mut RingBuffer<T>,
        }

        impl<'a, T> Drop for PanicGuard<'a, T> {
            fn drop(&mut self) {
                // Called only if a panic occurred during the main drop loop.
                let head = self.buf.head.load(Ordering::Relaxed);
                let mut tail = self.buf.tail.load(Ordering::Relaxed);

                // Take over and drop all remaining elements.
                while tail != head {
                    self.buf.tail.store(tail + 1, Ordering::Relaxed);
                    unsafe {
                        let slot = &self.buf.slots[tail % self.buf.cap];
                        std::ptr::drop_in_place((*slot.get()).as_mut_ptr());
                    }
                    tail += 1;
                }
            }
        }

        // normal drop
        let head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail.load(Ordering::Relaxed);

        while tail != head {
            // 1. tail++ to prepare for cleanup
            self.tail.store(tail + 1, Ordering::Relaxed);

            // 2. Check whether the guard is needed
            let guard = PanicGuard { buf: self };

            // 3. Execute the real cleanup
            // If this element panics during cleanup,
            // the guard will be dropped, and the rest will be cleaned up.
            unsafe {
                let slot = &guard.buf.slots[tail % guard.buf.cap];
                std::ptr::drop_in_place((*slot.get()).as_mut_ptr());
            }

            // 4. If it didn't panic, no guard needed, so forget it.
            std::mem::forget(guard);

            tail += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Sending half of the channel.
pub struct Producer<T: Send> {
    buf: Arc<RingBuffer<T>>, // An atomically reference-counted pointer to the shared RingBuffer.
    _marker: PhantomData<T>,
}

/// Receiving half of the channel.
pub struct Consumer<T: Send> {
    buf: Arc<RingBuffer<T>>, // An atomically reference-counted pointer to the shared RingBuffer.
    _marker: PhantomData<T>,
}

impl<T: Send> Producer<T> {
    /// Send `value`. May block while the channel is full until space becomes
    /// available. Returns `Err(SendError(value))` if the consumer has been
    /// dropped and the value cannot be delivered.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let buf = &self.buf;

        // 1. Read the current write pointer (head).
        // Since only the Producer modifies `head`, `Relaxed` is enough.
        let head = buf.head.load(Ordering::Relaxed);

        // 2. Spin wait for an empty slot.
        loop {
            // Check if the Consumer is still alive at the start of each iteration.
            if !buf.consumer_alive.load(Ordering::Acquire) {
                return Err(SendError(value));
            }

            let tail = buf.tail.load(Ordering::Acquire);

            // If head - tail < cap, there is space to write, break the spin loop.
            if head - tail < buf.cap {
                break;
            }

            // If full, continue to spin.
            std::hint::spin_loop();
        }

        // 3. Write the data to the slot.
        // We know there is space because of the check above, so using unsafe write is safe.
        unsafe {
            buf.write(head, value);
        }

        // 4. head++.
        // Use `Release` to ensure that the data is already properly written to memory before `head` is updated.
        // When the Consumer sees the new `head`, it will read the correct value from the slot.
        buf.head.store(head + 1, Ordering::Release);

        Ok(())
    }
}

impl<T: Send> Consumer<T> {
    /// Receive the next message. May block while the channel is empty until
    /// a message becomes available. Returns `Err(RecvError)` if the producer
    /// has been dropped and no messages remain in the channel.
    pub fn recv(&self) -> Result<T, RecvError> {
        let buf = &self.buf;

        // 1. Read the current read pointer (tail).
        // Since only the Consumer modifies `tail`, `Relaxed` is enough.
        let tail = buf.tail.load(Ordering::Relaxed);

        // 2. Spin wait for data.
        loop {
            // Read the current write pointer `head`.
            let head = buf.head.load(Ordering::Acquire);

            // If head != tail, it means the Producer has written new data, break the spin loop.
            if head != tail {
                break;
            }

            // If the queue is empty, check if the Producer is still alive.
            if !buf.producer_alive.load(Ordering::Acquire) {
                // If Producer is dead, we must check `head` one last time to get the last piece of data before lost forever.
                if buf.head.load(Ordering::Acquire) != tail {
                    break;
                }
                // If Producer is dead and there is no data left, return an error.
                return Err(RecvError);
            }

            // No data and Producer is still alive, continue to spin.
            std::hint::spin_loop();
        }

        // 3. Read the data from the slot `tail`
        // The slot `tail` must contain valid data, so it is safe to use the unsafe read method.
        let value = unsafe { buf.read(tail) };

        // 4. tail++.
        // Use `Release` to ensure that the data has been safely "taken" before this time point.
        // When the Producer sees the new `tail`, it will know that the reading is properly completed.
        buf.tail.store(tail + 1, Ordering::Release);

        Ok(value)
    }
}

impl<T: Send> Iterator for Consumer<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.recv().ok()
    }
}

impl<T: Send> Drop for Producer<T> {
    fn drop(&mut self) {
        // When Producer is dropped, set its alive flag to false to notify Consumer.
        self.buf.producer_alive.store(false, Ordering::Release);
    }
}

impl<T: Send> Drop for Consumer<T> {
    fn drop(&mut self) {
        // When Consumer is dropped, set its alive flag to false to notify Producer.
        self.buf.consumer_alive.store(false, Ordering::Release);
    }
}

// SAFETY: justify here why your implementation correctly satisfies `Send`
// for `Producer` / `Consumer`.

// SAFETY:
// 1. Both `Producer` and `Consumer` only hold an `Arc<RingBuffer<T>>` (and a `PhantomData<T>`), but no thread-local state or raw pointers.
// 2. We implemented `Sync` for `RingBuffer<T>` with `T: Send`.
// 3. An `Arc<U>` implements `Send` if its inner type `U` (`RingBuffer<T>`) is both `Send` and `Sync`.
// 4. Therefore, as long as `T` is `Send`, `RingBuffer<T>` satisfies the requirement, making it safe to move `Producer` or `Consumer` to another thread.

unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Send for Consumer<T> {}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Returned when `send` cannot deliver because the consumer has been dropped.
/// Contains the value so the caller can recover it.
#[derive(Debug)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("send on closed channel")
    }
}

/// Returned when `recv` cannot produce a value because the channel is empty
/// and the producer has been dropped.
#[derive(Debug)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("receive on closed and empty channel")
    }
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

/// Create a new SPSC channel.
///
/// `capacity` is a hint to the implementation: for a ring buffer it is
/// typically the fixed size; for a linked list it might be a pre-allocation
/// hint. You decide how to interpret it. The behaviour for `capacity == 0`
/// is implementation-defined.
pub fn channel<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    // 1. create a RingBuffer
    // The 0 capacity is undefined behavior, for safety give it a minimum capacity of 1 to prevent division by 0 error
    let cap = if capacity == 0 { 1 } else { capacity };

    // 2. wrap the RingBuffer with Arc
    let buf = Arc::new(RingBuffer::new(cap));

    // 3. clone the Arc for Producer
    let px = Producer {
        buf: Arc::clone(&buf),
        _marker: PhantomData,
    };

    let cx = Consumer {
        buf,
        _marker: PhantomData,
    };

    // 4. return the pair of producer and consumer
    (px, cx)
}

// ---------------------------------------------------------------------------
// Tests — extend as needed.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        panic,
        sync::{LazyLock, Mutex},
        thread,
        time::Duration,
    };

    use super::*;

    const ELEMS: std::ops::Range<i32> = if cfg!(miri) { 0..100 } else { 0..1000 };
    const ITERS: usize = if cfg!(miri) { 5 } else { 50 };

    // ---- basic correctness ----

    #[test]
    fn elements_arrive_correctly_ordered() {
        let (px, cx) = channel(64);
        thread::spawn(move || {
            for i in ELEMS {
                px.send(i).unwrap();
            }
        });
        for i in ELEMS {
            assert_eq!(i, cx.recv().unwrap());
        }
        assert!(cx.recv().is_err());
    }

    #[test]
    fn no_elements_lost() {
        for _ in 0..ITERS {
            let (px, cx) = channel(32);
            let handle = thread::spawn(move || {
                let mut count = 0;
                while cx.recv().is_ok() {
                    count += 1;
                }
                count
            });
            thread::spawn(move || {
                for i in ELEMS {
                    px.send(i).unwrap();
                }
            });
            assert_eq!(handle.join().unwrap(), ELEMS.len());
        }
    }

    #[test]
    fn unused_elements_are_dropped() {
        // Shared tracker for Foo: ensures every element is created exactly once
        // and dropped exactly once.
        static ELEM_SET: LazyLock<Mutex<HashSet<i32>>> =
            LazyLock::new(|| Mutex::new(HashSet::new()));

        #[derive(Debug)]
        struct Elem(i32);
        impl Elem {
            fn new(key: i32) -> Self {
                assert!(
                    ELEM_SET.lock().unwrap().insert(key),
                    "double initialisation of element {}",
                    key
                );
                Elem(key)
            }
        }

        impl Drop for Elem {
            fn drop(&mut self) {
                assert!(
                    ELEM_SET.lock().unwrap().remove(&self.0),
                    "double free of element {}",
                    self.0
                );
            }
        }

        for i in 0..ITERS {
            let (px, cx) = channel(8);
            let handle = thread::spawn(move || {
                for i in 0.. {
                    if px.send(Elem::new(i)).is_err() {
                        return;
                    }
                }
            });
            for _ in 0..i {
                cx.recv().unwrap();
            }
            drop(cx);
            handle.join().ok();
            let map = ELEM_SET.lock().unwrap();
            assert!(map.is_empty(), "ELEM_SET not empty: {:?}", *map);
        }
    }

    // ---- edge cases ----

    #[test]
    fn capacity_one() {
        let (px, cx) = channel(1);
        let handle = thread::spawn(move || {
            for i in 0..500 {
                px.send(i).unwrap();
            }
        });
        for i in 0..500 {
            assert_eq!(i, cx.recv().unwrap());
        }
        handle.join().unwrap();
    }

    // ---- panic in drop: one element panics during drop; all others must
    //      still be dropped (including those that come *after* the
    //      panicking element in the buffer). ----
    // Passing this test with a complete implementation confers a bonus point.

    #[test]
    fn panic_in_drop_preserves_others() {
        static BOMB_SET: LazyLock<Mutex<HashSet<i32>>> =
            LazyLock::new(|| Mutex::new(HashSet::new()));

        #[derive(Debug)]
        struct Bomb(i32);
        impl Bomb {
            fn new(key: i32) -> Self {
                BOMB_SET.lock().unwrap().insert(key);
                Bomb(key)
            }
        }
        impl Drop for Bomb {
            fn drop(&mut self) {
                BOMB_SET.lock().unwrap().remove(&self.0);
                if self.0 == 3 {
                    // Intentional panic during drop on element 3.
                    panic!("boom in drop for element {}", self.0);
                }
            }
        }

        // Suppress the panic backtrace from the drop below.
        let prev_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));

        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let (px, cx) = channel(8);
            for i in 0..5 {
                px.send(Bomb::new(i)).unwrap();
            }
            // Drop both ends; the RingBuffer drop loop will drop elements
            // 0..5. Element 3 panics; the surrounding drop glue should still
            // free the backing allocation and the other Bombs must already
            // have been removed from BOMB_SET before the panic.
            drop(cx);
            drop(px);
        }));
        panic::set_hook(prev_hook);

        // The panic from element 3's drop must propagate out, but all other
        // elements — including element 4, which comes *after* the panicking
        // one in the buffer — must have been dropped.
        assert!(result.is_err(), "expected panic during drop");
        let remaining = BOMB_SET.lock().unwrap().clone();
        assert!(
            remaining.is_empty(),
            "not all elements were dropped: {:?}",
            remaining
        );
    }

    // ---- close: dropping one end unblocks the spinning other end ----

    #[test]
    fn close_unblocks_spinning_consumer() {
        let (px, cx) = channel::<i32>(4);
        let handle = thread::spawn(move || cx.recv());

        thread::sleep(Duration::from_millis(50));
        drop(px);
        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn close_unblocks_spinning_producer() {
        let (px, cx) = channel(2);
        let handle = thread::spawn(move || {
            for i in 0.. {
                if px.send(i).is_err() {
                    return i;
                }
            }
            unreachable!()
        });

        thread::sleep(Duration::from_millis(50));
        drop(cx);
        let last = handle.join().unwrap();
        assert!(last > 0);
    }

    // ---- additional correctness tests ----

    #[test]
    fn recv_drains_before_err() {
        let n = ELEMS.len();
        let (px, cx) = channel(32);
        thread::spawn(move || {
            for i in ELEMS {
                px.send(i).unwrap();
            }
            // producer drops here — channel closes
        });
        let mut received = Vec::with_capacity(n);
        loop {
            match cx.recv() {
                Ok(v) => received.push(v),
                Err(_) => break,
            }
        }
        assert_eq!(received.len(), n, "some elements lost on close");
        for (i, &v) in received.iter().enumerate() {
            assert_eq!(v, i as i32, "wrong element at position {}", i);
        }
    }

    #[test]
    fn send_recv_alternating() {
        // Single-threaded: send one, recv one, cycling through the buffer
        // many more times than its capacity. Catches modular-arithmetic
        // bugs at the buffer-position wrap boundary.
        let (px, cx) = channel(4);
        let cycles = if cfg!(miri) { 100 } else { 10_000 };
        for i in 0..cycles {
            px.send(i).unwrap();
            assert_eq!(cx.recv().unwrap(), i);
        }
    }

    #[test]
    fn zero_sized_type() {
        let (px, cx) = channel::<()>(8);
        let n = if cfg!(miri) { 50 } else { 500 };
        let handle = thread::spawn(move || {
            for _ in 0..n {
                px.send(()).unwrap();
            }
        });
        for _ in 0..n {
            cx.recv().unwrap();
        }
        handle.join().unwrap();
        assert!(cx.recv().is_err());
    }

    #[test]
    fn iterator_fuses_after_close() {
        let (px, cx) = channel(4);
        px.send(1).unwrap();
        px.send(2).unwrap();
        drop(px);
        let mut iter = cx.into_iter();
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn wide_type_across_threads() {
        // A multi-field struct makes partial/torn reads from incorrect
        // orderings more likely to surface (especially under Miri).
        #[derive(Debug, PartialEq)]
        struct Wide(u64, u64, u64);
        let (px, cx) = channel(16);
        let handle = thread::spawn(move || {
            for i in ELEMS.map(|i| i as u64) {
                px.send(Wide(i, i + 1, i + 2)).unwrap();
            }
        });
        for i in ELEMS.map(|i| i as u64) {
            assert_eq!(cx.recv().unwrap(), Wide(i, i + 1, i + 2));
        }
        handle.join().unwrap();
    }
}

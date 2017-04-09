extern crate coco;
extern crate rand;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::thread;

use coco::epoch::{self, Atomic};
use rand::Rng;

fn worker(a: Arc<Atomic<AtomicUsize>>) {
    let mut rng = rand::thread_rng();
    let mut sum = 0;

    for _ in 0..1000 {
        epoch::pin(|pin| {
            let val = if rng.gen_range(0, 100) < 10 {
                a.swap_box(Box::new(AtomicUsize::new(sum)), 0, pin)
                 .unwrap()
                 .load(Relaxed)
            } else {
                a.load(pin)
                 .unwrap()
                 .fetch_add(sum, Relaxed)
            };

            sum = sum.wrapping_add(val);
        });

        if rng.gen_range(0, 1010) == 0 {
            let a = a.clone();
            thread::spawn(move || worker(a)).join().unwrap();
        }
    }
}

fn main() {
    let a = Arc::new(Atomic::new(AtomicUsize::new(777), 0));

    let threads = (0..10)
        .map(|_| {
            let a = a.clone();
            thread::spawn(move || worker(a))
        })
        .collect::<Vec<_>>();

    for t in threads {
        t.join().unwrap();
    }
}

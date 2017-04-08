extern crate coco;
extern crate rand;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;

use coco::Stack;
use rand::Rng;

fn stress_test() {
}

fn leak_test() {
    let s = Arc::new(Stack::new());
    s.push(10);
    std::mem::forget(s);
    return;

    let threads = (0..8).map(|t| {
        let s = s.clone();

        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for i in 0..100_000 {
                if rng.gen_range(0, t + 1) == 0 {
                    s.pop();
                } else {
                    s.push(i);
                }
            }
        })
    }).collect::<Vec<_>>();

    for t in threads {
        t.join().unwrap();
    }
}

fn main() {
    stress_test();
    leak_test();
}

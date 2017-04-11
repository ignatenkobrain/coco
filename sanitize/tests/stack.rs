extern crate coco;
extern crate rand;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::thread;

use coco::Stack;
use rand::Rng;

#[test]
fn sanitize_stack() {
    const THREADS: usize = 8;

    let s = Arc::new(Stack::new());
    let len = Arc::new(AtomicUsize::new(0));

    let threads = (0..THREADS).map(|t| {
        let s = s.clone();
        let len = len.clone();

        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            for i in 0..100_000 {
                if rng.gen_range(0, t + 1) == 0 {
                    if s.pop().is_some() {
                        len.fetch_sub(1, SeqCst);
                    }
                } else {
                    s.push(t + THREADS * i);
                    len.fetch_add(1, SeqCst);
                }
            }
        })
    }).collect::<Vec<_>>();

    for t in threads {
        t.join().unwrap();
    }

    let mut last = [std::usize::MAX; THREADS];

    while !s.is_empty() {
        let x = s.pop().unwrap();
        let t = x % THREADS;

        assert!(last[t] > x);
        last[t] = x;

        len.fetch_sub(1, SeqCst);
    }
    assert_eq!(len.load(SeqCst), 0);
}

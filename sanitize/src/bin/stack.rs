extern crate coco;
extern crate rand;

use std::sync::Arc;
use std::thread;

use rand::Rng;
use coco::{epoch, Stack};

fn stress_test() {
    // TODO
}

fn leak_test() {
    let s = Arc::new(Stack::new());

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

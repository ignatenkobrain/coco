extern crate coco;

use std::thread;

use coco::deque;

#[test]
fn sanitize_deque() {
    let (w, s) = deque::new();

    for i in 0..1_000_000 {
        w.push(Box::new(i));
    }

    let threads = (0..8).map(|_| {
        let s = s.clone();

        thread::spawn(move || {
            for _ in 0..500_000 {
                s.steal();
            }
        })
    }).collect::<Vec<_>>();

    for _ in 0..500_000 {
        w.pop();
    }

    for t in threads {
        t.join().unwrap();
    }
}

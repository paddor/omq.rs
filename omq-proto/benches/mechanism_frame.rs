//! Per-frame CURVE overhead microbench.

use std::hint::black_box;
use std::time::Instant;

use crypto_box::Nonce;
use crypto_box::aead::Aead;

fn main() {
    let secret = crypto_box::SecretKey::from([0x42; 32]);
    let peer = crypto_box::PublicKey::from([0x43; 32]);
    let cipher = crypto_box::SalsaBox::new(&peer, &secret);
    let nonce = Nonce::from([0x11; 24]);
    for size in [128, 2_048, 8_192] {
        let payload = vec![0xAC; size];
        let start = Instant::now();
        for _ in 0..1000 {
            black_box(cipher.encrypt(&nonce, black_box(&payload[..])).unwrap());
        }
        let ns = start.elapsed().as_nanos() / 1000;
        println!("{size:>6} bytes: {ns:>8} ns/op");
    }
}

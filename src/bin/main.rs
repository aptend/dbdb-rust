use cluFlock::ExclusiveFlock;
use std::env::args;
use std::fs::File;
use std::io::prelude::*;
use std::thread;

fn main() {
    let f = File::create("m.loc").unwrap();
    let mut df = File::open("m.loc").unwrap();
    println!("Lock!");
    let mut guard = ExclusiveFlock::wait_lock(&f).unwrap();
    guard.write_all(args().nth(1).unwrap().as_bytes()).unwrap();
    println!("Write!");
    let mut s = String::new();
    df.read_to_string(&mut s).unwrap();
    println!("{:?}", s);
    thread::sleep_ms(5000);
    guard.unlock().unwrap();
    println!("Drop!");
    thread::sleep_ms(5000);
}

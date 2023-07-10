mod exec;
mod serialize;
mod metric;

mod local;
mod common;
mod os_statistics;
mod bench;
mod stp;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    let is_local = std::env::var("LOCAL")
        .map(|x| x == "1")
        .unwrap_or(false);

    println!("Starting local? {}", is_local);

    if is_local {
        local::main()
    }
}

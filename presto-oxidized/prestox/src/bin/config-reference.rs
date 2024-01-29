fn main() {
    let reg = prestox::config::PROPERTY_REGISTRY.lock().unwrap();
    println!("{:?}", *reg);
}

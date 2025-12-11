use performant::entrypoint;

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap(); // Ignore first arg
    let input = args.next().expect("Expected input file");

    let file = match input.as_str() {
        "tiny" => "./data/small-small-measurements.txt",
        "small" => "./data/small-measurements.txt",
        "full" => "./data/measurements.txt",
        _ => panic!("Unknown mode: {input}"),
    }
    .to_string();

    let stdout = std::io::stdout();

    entrypoint(file, 0x4000000, stdout);
}

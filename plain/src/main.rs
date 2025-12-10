use plain::calculate_averages;

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap(); // Ignore first arg
    let file_name = args.next().expect("Expected input file");

    let results = calculate_averages(file_name);

    println!("{results}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_averages() {
        let result = calculate_averages("../data/small-measurements.txt".to_string());
        let expected = std::fs::read_to_string("../data/small-ref.txt").unwrap();
        assert_eq!(expected, result);
    }
}

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap(); // Ignore first arg
    let file_name = args.next().expect("Expected input file");

    let results = calculate_averages(file_name);

    println!("{results}");
}

pub fn calculate_averages(file_name: String) -> String {
    let file = File::open(file_name).expect("Unable to open file");
    let reader = BufReader::new(file);

    let mut map = HashMap::<String, Record>::with_capacity(10000);

    for line in reader.lines() {
        let line = line.expect("Unable to read line");
        let (name, temp) = line.split_once(';').expect("Line with no ';'");

        let temp: f64 = temp.parse().expect("Unable to parse temperature value");

        let entry = map
            .entry(name.to_string())
            .or_insert_with(|| Record::default());

        entry.count += 1;
        entry.min = entry.min.min(temp);
        entry.max = entry.max.max(temp);
        entry.acc += temp;
    }

    let mut sorted_stations: Vec<_> = map.into_iter().collect();

    sorted_stations.sort_unstable_by_key(|f| f.0.clone());

    format_results(sorted_stations)
}

struct Record {
    min: f64,
    max: f64,
    acc: f64,
    count: usize,
}

impl Default for Record {
    fn default() -> Self {
        Self {
            min: f64::MAX,
            max: f64::MIN,
            acc: 0.,
            count: 0,
        }
    }
}

fn format_results(stations: Vec<(String, Record)>) -> String {
    let mut string = String::with_capacity(stations.len() * 10);

    string.push('{');

    string.push_str(
        &stations
            .into_iter()
            .map(|(name, records)| {
                format!(
                    "{}={:.1}/{:.1}/{:.1}, ",
                    name,
                    records.min,
                    records.acc / (records.count as f64),
                    records.max
                )
            })
            .reduce(|mut acc, c| {
                acc.push_str(&c);
                acc
            })
            .unwrap(),
    );
    string.pop(); // Remove ','
    string.pop(); // Remove ' '
    string.push('}');
    string.push('\n');

    string
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

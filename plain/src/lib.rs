use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

#[derive(Debug)]
pub struct Record {
    min: i16,
    max: i16,
    acc: isize,
    count: usize,
}

impl Default for Record {
    fn default() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            acc: 0,
            count: 0,
        }
    }
}

fn parse_temp(input: &str) -> i16 {
    let (left, right) = input.split_once('.').unwrap();

    let (left, negative) = if left.starts_with('-') {
        (&left[1..], true)
    } else {
        (left, false)
    };

    let left = left.parse::<i16>().unwrap() * 10;
    let right: i16 = right.parse().unwrap();

    let val = if negative {
        -left - right
    } else {
        left + right
    };

    val
}

pub fn calculate_averages(file_name: String) -> String {
    let file = File::open(file_name).expect("Unable to open file");
    let reader = BufReader::new(file);

    let mut map = HashMap::<String, Record>::with_capacity(10000);

    for line in reader.lines() {
        let line = line.expect("Unable to read line");
        let (name, temp) = line.split_once(';').expect("Line with no ';'");

        let temp = parse_temp(temp);

        let entry = map
            .entry(name.to_string())
            .or_insert_with(|| Record::default());

        entry.count += 1;
        entry.min = entry.min.min(temp);
        entry.max = entry.max.max(temp);
        entry.acc += temp as isize;
    }

    let mut sorted_stations: Vec<_> = map.into_iter().collect();
    sorted_stations.sort_unstable_by_key(|f| f.0.clone());

    format_results(sorted_stations)
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
                    (records.min as f64) / 10.,
                    (records.acc as f64) / (10. * (records.count as f64)),
                    (records.max as f64) / 10.
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

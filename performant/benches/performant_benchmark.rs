use criterion::{Criterion, criterion_group, criterion_main};
use performant::entrypoint;
use std::path::Path;

fn get_dataset_path(dataset_name: &str) -> &'static str {
    match dataset_name {
        "tiny" => "../data/small-small-measurements.txt",
        "small" => "../data/small-measurements.txt",
        "full" => "../data/measurements.txt",
        _ => panic!("Unknown dataset: {}", dataset_name),
    }
}

fn benchmark_performant_implementation(c: &mut Criterion) {
    let mut small_group = c.benchmark_group("performant");

    if Path::new(get_dataset_path("tiny")).exists() {
        small_group.bench_function("tiny_dataset", |b| {
            b.iter(|| entrypoint(get_dataset_path("tiny").to_string(), 0x4000000))
        });
    }

    if Path::new(get_dataset_path("small")).exists() {
        small_group.bench_function("small_dataset", |b| {
            b.iter(|| entrypoint(get_dataset_path("small").to_string(), 0x4000000))
        });
    }

    small_group.finish();

    let mut full_group = c.benchmark_group("performant_full");
    full_group.sample_size(10);

    if Path::new(get_dataset_path("full")).exists() {
        full_group.bench_function("full_dataset", |b| {
            b.iter(|| entrypoint(get_dataset_path("full").to_string(), 0x4000000))
        });
    }

    full_group.finish();
}

criterion_group!(benches, benchmark_performant_implementation);
criterion_main!(benches);

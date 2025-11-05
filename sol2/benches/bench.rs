use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;

fn criterion_benchmark(c: &mut Criterion) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/measurements.txt");
    let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    let mut group = c.benchmark_group("sol1");
    group.sample_size(100);
    group.throughput(Throughput::Bytes(size));

    group.bench_function(BenchmarkId::from_parameter("measurements.txt"), |b| {
        b.iter_batched(
            || path.to_string_lossy().to_string(),
            |filename| {
                let out = sol2::solve(filename).unwrap();
                black_box(out);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default();
    targets = criterion_benchmark,
);

criterion_main!(benches);

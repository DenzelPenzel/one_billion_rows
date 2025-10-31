use criterion::{Criterion, criterion_group, criterion_main};

fn criterion_benchmark(c: &mut Criterion) {
    let filename = "../../data/measurements.txt".to_string();
    c.bench_function("read_it", |b| b.iter(|| sol1::solve(filename.clone())));
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark,
);

criterion_main!(benches);

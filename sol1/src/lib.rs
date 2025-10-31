use ahash::AHashMap;
use memmap2::MmapOptions;
use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::ops::Range;

pub use findlib::find;

pub const NEWLINE: u8 = 10;
pub const SEMICOLON: u8 = 59;
pub const NUM_STATIONS: usize = 413;
pub const MINUS: u8 = 45;
pub const PERIOD: u8 = 46;

#[derive(Debug)]
struct Aggregator {
    name: String,
    min: i32,
    max: i32,
    sum: i64,
    count: u64,
}

impl Default for Aggregator {
    fn default() -> Self {
        Self {
            name: String::new(),
            min: i32::MAX,
            max: i32::MIN,
            sum: 0,
            count: 0,
        }
    }
}

// removed unused find_next_new_line

#[inline]
fn memchr_newline(slice: &[u8]) -> Option<usize> {
    slice.iter().position(|&b| b == b'\n')
}

fn chunk_by_newlines(data: &[u8], workers: usize) -> Vec<Range<usize>> {
    if workers == 0 {
        return vec![0..data.len()];
    }
    let mut ranges = Vec::new();
    let mut s = 0usize;
    let base = data.len() / workers.max(1);
    let chunk_size = if base == 0 { data.len() } else { base };

    while s < data.len() {
        let mut e = s + chunk_size;
        if e >= data.len() {
            ranges.push(s..data.len());
            break;
        }
        if let Some(nl_off) = memchr_newline(&data[e..]) {
            e += nl_off + 1;
            ranges.push(s..e);
            s = e;
        } else {
            ranges.push(s..data.len());
            break;
        }
    }

    ranges
}

fn parse_digits(buffer: &[u8]) -> i32 {
    let size = buffer.len();
    let mut neg = 1;
    let mut acc = 0;
    let mut pos_mul = 10_i32.pow(size as u32 - 2);
    for i in 0..size {
        match buffer[i] {
            MINUS => {
                neg = -1;
                pos_mul /= 10;
            }
            PERIOD => {
                // Do nothing
            }
            48..=57 => {
                // Digits
                let d = buffer[i] as i32 - 48;
                acc += d * pos_mul;
                pos_mul /= 10;
            }
            _ => {
                panic!("Unhandled ASCII numerical symbol: {}", buffer[i]);
            }
        }
    }
    acc *= neg;
    acc
}

#[inline]
fn mean_tenths(sum_scaled: i64, count: u64) -> i64 {
    let denom = count as i64;
    if sum_scaled >= 0 {
        (sum_scaled + (denom / 2)) / denom
    } else {
        -((-sum_scaled + (denom / 2)) / denom)
    }
}

fn scan_chunk(start: usize, end: usize, buffer: &[u8]) -> Vec<Aggregator> {
    let mut res: AHashMap<&[u8], Aggregator> = AHashMap::with_capacity(NUM_STATIONS);
    let mut pos = start;
    let mut field_start = start; // start of the current token (station or value)
    let mut current_station: &[u8] = &[]; // station slice captured at ';'
    let mut has_station = false; // whether we saw ';' on the current line

    while pos < end {
        match buffer[pos] {
            SEMICOLON => {
                current_station = &buffer[field_start..pos];
                field_start = pos + 1;
                has_station = true;
            }
            NEWLINE => {
                if has_station {
                    let value_slice = &buffer[field_start..pos];
                    if !value_slice.is_empty() {
                        let val = parse_digits(value_slice);
                        let entry = res
                            .entry(current_station)
                            .or_insert_with(Aggregator::default);
                        if entry.name.is_empty() {
                            entry.name = String::from_utf8_lossy(current_station).to_string();
                        }
                        entry.max = i32::max(val, entry.max);
                        entry.min = i32::min(val, entry.min);
                        entry.sum += val as i64;
                        entry.count += 1;
                    }
                }

                field_start = pos + 1; // start of next line
                has_station = false; // reset for the new line
            }
            _ => {}
        }

        pos += 1;
    }

    res.into_iter().map(|(_, v)| v).collect()
}

pub fn solve(filename: String) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open(&filename)?;
    let mapped_file = unsafe { MmapOptions::new().map(&file)? };
    let workers = rayon::current_num_threads().max(1);

    let chunks = chunk_by_newlines(&mapped_file, workers);

    let mut res: Vec<Aggregator> = Vec::with_capacity(NUM_STATIONS);

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());

        for r in chunks.iter().cloned() {
            let buffer = &mapped_file;
            let handle = scope.spawn(move || scan_chunk(r.start, r.end, &buffer));
            handles.push(handle);
        }

        for handle in handles {
            let part = handle.join().unwrap();
            if part.is_empty() {
                res.extend(part);
            } else {
                part.into_iter().for_each(|v| {
                    if let Some(agg) = res.iter_mut().find(|a| a.name == v.name) {
                        agg.sum += v.sum;
                        agg.count += v.count;
                        agg.max = i32::max(agg.max, v.max);
                        agg.min = i32::min(agg.min, v.min);
                    } else {
                        res.push(v);
                    }
                })
            }
        }
    });

    res.sort_unstable_by(|a, b| a.name.cmp(&b.name));

    let mut out = String::with_capacity(res.len().saturating_mul(32) + 3);
    out.push('{');

    for (idx, v) in res.iter().enumerate() {
        let mean_t = mean_tenths(v.sum, v.count);
        let _ = FmtWrite::write_fmt(
            &mut out,
            format_args!(
                "{}={:.1}/{:.1}/{:.1}",
                v.name,
                v.min as f32 / 10.0,
                mean_t as f32 / 10.0,
                v.max as f32 / 10.0
            ),
        );
        if idx + 1 != res.len() {
            out.push_str(", ");
        }
    }
    out.push('}');
    out.push('\n');

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use findlib::read_file;
    use std::path::Path;

    #[test]
    fn test_solve() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../test_cases");
        let files = find(&root, ".txt").unwrap_or_else(|e| panic!("walking test_cases: {e}"));
        for name in files {
            let txt_path = format!("{}.txt", name.display());
            let out_path = format!("{}.out", name.display());
            let got = solve(txt_path).unwrap_or_else(|e| panic!("solve failed: {e}"));
            let want = read_file(out_path);

            assert_eq!(want, got, "mismatch for {}", name.display())
        }
    }
}

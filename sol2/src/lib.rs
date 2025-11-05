use memmap2::MmapOptions;
use rayon::prelude::*;
use std::fs::File;
use std::ops::Range;

const OFFSET64: u64 = 14695981039346656037;
const PRIME64: u64 = 1099511628211;
const BUCKET_SIZE: usize = 1 << 25; // must be power of two

// Shifts/masks for number parsing
const SHIFT1: u64 = 8 * 1;
const SHIFT2: u64 = 8 * 2;
const SHIFT3: u64 = 8 * 3;
const SHIFT4: u64 = 8 * 4;

const CHAR_MASK0: u64 = 255;
const CHAR_MASK1: u64 = (255u64) << SHIFT1;
const CHAR_MASK2: u64 = (255u64) << SHIFT2;
const CHAR_MASK3: u64 = (255u64) << SHIFT3;
const CHAR_MASK4: u64 = (255u64) << SHIFT4;

const DOT1: u64 = (b'.' as u64) << 8;
const DOT2: u64 = (b'.' as u64) << 16;

fn round1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Hash(u64);

impl Hash {
    #[inline]
    fn index(self) -> usize {
        // FNV-like step then mask by table size
        let mut h = OFFSET64;
        h ^= self.0;
        h = h.wrapping_mul(PRIME64);
        (h & ((BUCKET_SIZE as u64) - 1)) as usize
    }
}

#[inline]
fn create_hash(u: u64, p: usize) -> Hash {
    if p >= 8 {
        Hash(u)
    } else {
        let m = (1u64 << (p << 3)) - 1;
        Hash(u & m)
    }
}

#[derive(Clone, Debug)]
struct Node {
    key: String,
    hash: Hash,
    next: Option<Box<Node>>,
    sum: i64,
    count: i64,
    min: i16,
    max: i16,
}

impl Node {
    fn new(key: String, hash: Hash) -> Self {
        Self {
            key,
            hash,
            next: None,
            sum: 0,
            count: 0,
            min: i16::MAX,
            max: i16::MIN,
        }
    }
}

struct Bucket {
    keys: Vec<String>,
    bucket: Vec<Option<Box<Node>>>,
}

impl Bucket {
    fn new() -> Self {
        let mut bucket = Vec::with_capacity(BUCKET_SIZE);
        bucket.resize_with(BUCKET_SIZE, || None);
        Bucket {
            keys: Vec::new(),
            bucket,
        }
    }

    fn keys(&self) -> &[String] {
        &self.keys
    }

    fn find(&self, h: Hash, key: &str) -> Option<&Node> {
        let mut curr = self.bucket[h.index()].as_deref();
        while let Some(node) = curr {
            if node.hash == h && (key.len() <= 8 || key == node.key) {
                return Some(node);
            }
            curr = node.next.as_deref();
        }
        None
    }

    fn find_mut(&mut self, h: Hash, key: &[u8]) -> Option<&mut Node> {
        let mut link = &mut self.bucket[h.index()];
        while let Some(node) = link {
            if node.hash == h && (key.len() <= 8 || key == node.key.as_bytes()) {
                return Some(node.as_mut());
            }
            link = &mut node.next;
        }
        None
    }

    fn insert(&mut self, h: Hash, key: &[u8]) -> &mut Node {
        let idx = h.index();

        // Try to find existing node without holding a mutable borrow across this function
        let found_ptr: *mut Node = {
            let mut link = &mut self.bucket[idx];
            loop {
                match link {
                    Some(node) => {
                        if node.hash == h && (key.len() <= 8 || key == node.key.as_bytes()) {
                            break node.as_mut() as *mut Node;
                        }
                        link = &mut node.next;
                    }
                    None => break std::ptr::null_mut(),
                }
            }
        };
        if !found_ptr.is_null() {
            // SAFETY: `found_ptr` points into `self.bucket[idx]` and we have exclusive access via `&mut self`.
            return unsafe { &mut *found_ptr };
        }

        // Not found: insert new node and record the key once
        let key_string = String::from_utf8(key.to_vec()).unwrap();
        self.keys.push(key_string.clone());

        let new_node = Box::new(Node::new(String::from_utf8(key.to_vec()).unwrap(), h));

        let head = &mut self.bucket[idx];
        match head {
            None => {
                *head = Some(new_node);
                return head.as_deref_mut().unwrap();
            }
            Some(head_node) => {
                let mut tail = head_node.as_mut();
                while tail.next.is_some() {
                    tail = tail.next.as_mut().unwrap().as_mut();
                }
                tail.next = Some(new_node);
                return tail.next.as_deref_mut().unwrap();
            }
        }
    }
}

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

fn process_partition(data: &[u8], range: Range<usize>) -> Bucket {
    let mut b = Bucket::new();
    let mut start = range.start;
    let end = range.end;

    while start < end {
        if start + 8 > end {
            let (city_bytes, after_city) = scan_city_slow(&data[start..end]);
            start += after_city;

            let uhash = city_hash8_prefix(city_bytes);
            let h = create_hash(uhash, city_bytes.len());

            let mut tmp = [0u8; 8];
            let avail = end - start;
            tmp[..avail].copy_from_slice(&data[start..end]);
            let u = u64::from_le_bytes(tmp);
            let (temp, adv) = parse_number(u);
            let node = b.insert(h, city_bytes);
            node.min = node.min.min(temp);
            node.max = node.max.max(temp);
            node.sum += temp as i64;
            node.count += 1;
            start += adv.min(avail);
        } else {
            let w = load_u64_le(&data[start..start + 8]);

            // Try find semicolon in first 8 bytes
            let mut idx = find_semicolon(w);
            let city_bytes: &[u8] = if idx >= 0 {
                let uidx = idx as usize;
                let slice = &data[start..start + uidx];
                start += uidx + 1; // skip ';'
                slice
            } else {
                let mut i = start + 8;
                let mut maybe: Option<&[u8]> = None;

                while i + 8 <= end {
                    let u = load_u64_le(&data[i..i + 8]);
                    idx = find_semicolon(u);
                    if idx >= 0 {
                        let uidx = idx as usize;
                        let slice = &data[start..i + uidx];
                        start = i + uidx + 1;
                        maybe = Some(slice);
                        break;
                    }
                    i += 8;
                }

                if let Some(bytes) = maybe {
                    bytes
                } else {
                    let (c, consumed) = scan_city_slow(&data[start..end]);
                    start += consumed;
                    c
                }
            };

            let uhash = city_hash8_prefix(city_bytes);
            let h = create_hash(uhash, city_bytes.len());

            if start + 8 > end {
                let mut tmp = [0u8; 8];
                let avail = end - start;
                tmp[..avail].copy_from_slice(&data[start..end]);
                let u = u64::from_le_bytes(tmp);
                let (temp, adv) = parse_number(u);
                let node = b.insert(h, city_bytes);
                node.min = node.min.min(temp);
                node.max = node.max.max(temp);
                node.sum += temp as i64;
                node.count += 1;
                start += adv.min(avail);
            } else {
                let u = load_u64_le(&data[start..start + 8]);
                let (temp, adv) = parse_number(u);
                let node = b.insert(h, city_bytes);
                node.min = node.min.min(temp);
                node.max = node.max.max(temp);
                node.sum += temp as i64;
                node.count += 1;
                start += adv;
            }
        }
    }

    b
}

#[inline]
fn parse_number(u: u64) -> (i16, usize) {
    // Formats:
    //  0.0      -> 4 bytes
    //  00.0     or -0.0 -> 5 bytes
    // -00.0     -> 6 bytes

    if (u & CHAR_MASK1) == DOT1 {
        // 0.0
        let ones = ((u & CHAR_MASK0) - b'0' as u64) * 10;
        let tenths = ((u & CHAR_MASK2) >> SHIFT2) - b'0' as u64;
        return (i16::try_from(ones + tenths).unwrap(), 4);
    } else if (u & CHAR_MASK2) == DOT2 {
        // 00.0 or -0.0
        let v0 = u & CHAR_MASK0;
        // If leading byte is '-', do not compute tens to avoid overflow on multiply
        let neg = v0 == b'-' as u64;
        let tens = if neg { 0 } else { (v0 - b'0' as u64) * 100 };
        let ones = (((u & CHAR_MASK1) >> SHIFT1) - b'0' as u64) * 10;
        let tenths = ((u & CHAR_MASK3) >> SHIFT3) - b'0' as u64;

        let temp_u = ones + tenths + tens;
        let val = i16::try_from(temp_u).unwrap();
        let val = if neg { -val } else { val };
        return (val, 5);
    } else {
        // -00.0
        let tens = (((u & CHAR_MASK1) >> SHIFT1) - b'0' as u64) * 100;
        let ones = (((u & CHAR_MASK2) >> SHIFT2) - b'0' as u64) * 10;
        let tenths = ((u & CHAR_MASK4) >> SHIFT4) - b'0' as u64;

        let t = i16::try_from(tens + ones + tenths).unwrap();
        return (t.saturating_neg(), 6);
    }
}

#[inline]
fn city_hash8_prefix(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    u64::from_le_bytes(buf)
}

// Find semicolon within the next 8 bytes.
// Returns byte index [0..7] if found, else -1.
// Implements hasvalue(x, ';') via haszero((x) ^ repeat_byte(';')) trick and trailing_zeros.
#[inline]
fn find_semicolon(word: u64) -> i32 {
    // maskedInput = (word ^ 0x3B*8) => bytes equal to ';' become 0x00
    let mut masked = word ^ 0x3B3B3B3B3B3B3B3B;
    // haszero(v) = ((v - 0x0101..) & ~v & 0x8080..)
    masked = (masked.wrapping_sub(0x0101010101010101)) & (!masked) & 0x8080_8080_8080_8080u64;
    if masked == 0 {
        return -1;
    }
    // Trailing zeros / 8 gives byte index
    (masked.trailing_zeros() >> 3) as i32
}

#[inline]
fn load_u64_le(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(arr)
}

#[inline]
fn scan_city_slow(data: &[u8]) -> (&[u8], usize) {
    if let Some(pos) = data.iter().position(|&b| b == b';') {
        (&data[..pos], pos + 1)
    } else {
        (data, data.len())
    }
}

pub fn solve(filename: String) -> Result<String, Box<dyn std::error::Error>> {
    let file = File::open(&filename)?;
    let mapped_file = unsafe { MmapOptions::new().map(&file)? };

    let workers = rayon::current_num_threads().max(1);
    let chunks = chunk_by_newlines(&mapped_file, workers);

    let groups: Vec<Bucket> = (0..chunks.len())
        .into_par_iter()
        .map(|i| process_partition(&mapped_file, chunks[i].clone()))
        .collect();

    let total_keys = groups.iter().map(|b| b.keys.len()).sum();
    let mut cities = Vec::with_capacity(total_keys);
    for b in groups.iter() {
        cities.extend_from_slice(b.keys());
    }
    cities.sort();
    cities.dedup();

    // Build output: {city=min/avg/max, ...}
    let mut out = String::with_capacity(cities.len() * 32);
    out.push('{');

    for (i, city) in cities.iter().enumerate() {
        let h = create_hash(city_hash8_prefix(city.as_bytes()), city.len());

        let mut minv: i16 = i16::MAX;
        let mut maxv: i16 = i16::MIN;
        let mut sum: i64 = 0;
        let mut cnt: i64 = 0;

        for g in groups.iter() {
            if let Some(node) = g.find(h, city) {
                minv = minv.min(node.min);
                maxv = maxv.max(node.max);
                sum += node.sum;
                cnt += node.count;
            }
        }

        if i > 0 {
            out.push_str(", ");
        }

        let min_f = round1(minv as f64 / 10.0);
        let avg_f = round1((sum as f64) / 10.0 / (cnt as f64));
        let max_f = round1(maxv as f64 / 10.0);

        // city=%.1f/%.1f/%.1f
        use std::fmt::Write as _;
        write!(out, "{}={:.1}/{:.1}/{:.1}", city, min_f, avg_f, max_f).unwrap();
    }

    out.push_str("}\n");

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use findlib::{find, read_file};
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

use dashmap::mapref::entry::Entry;
use std::io::Write;

use dashmap::DashMap;
use memchr::memchr;
use memmap::Mmap;
use rayon::{iter::ParallelIterator, slice::ParallelSlice};

// baseline: 150.46
// no string copy: 124.91
// ahash (no aes target): 105.43
// with_capacity(10_00): 108.73 -> SLOWER
// custom_parse: 90.66
// no utf8 validation: 53.63
// memchr: 52.27
// mmap: 52.28 -> NO CHANGE

const NUM_THREADS: usize = 8;

fn main() {
    // this is needed to control the number of threads
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()
        .unwrap();

    let file = std::fs::File::open("measurements.txt").unwrap();
    let data = unsafe { Mmap::map(&file).unwrap() };

    // remove trailing whitespace
    assert!(*data.last().unwrap() == b'\n');
    let data_trimmed = &data[..(data.len() - 1)];

    let ahasher = ahash::RandomState::new();
    // not mut because of internal mutability for threading
    let cities = DashMap::with_hasher_and_shard_amount(ahasher, NUM_THREADS * 4);

    data_trimmed
        .par_split(|byte| *byte == b'\n')
        .for_each(|line| {
            let separator_index = memchr(b';', line).unwrap();
            let (city_name, value_with_separator) = line.split_at(separator_index);
            let (_, value) = value_with_separator.split_first().unwrap();
            let parsed_value: f32 = custom_parse_temperature_value(value);

            match cities.entry(city_name) {
                Entry::Occupied(mut occupied_entry) => {
                    let stats: &mut Statistics = occupied_entry.get_mut();
                    stats.add_value(parsed_value);
                }
                Entry::Vacant(vacant_entry) => {
                    vacant_entry.insert(Statistics::new(parsed_value));
                }
            };
        });

    let mut cities: Vec<_> = cities.into_iter().collect();
    cities.sort_unstable_by(|(name1, _), (name2, _)| name1.cmp(name2));

    let mut out = std::io::stdout();
    for (city_name, stats) in cities {
        out.write_all(city_name).unwrap();
        println!(
            "={:.1}/{:.1}/{:.1}",
            round_to_one_digit(stats.min),
            round_to_one_digit(stats.total / stats.num_values as f32),
            round_to_one_digit(stats.max)
        );
    }
}

fn round_to_one_digit(value: f32) -> f32 {
    // this still leaves some -0.0, but I am unsure whether this is wanted
    (value * 10.0).round() / 10.0
}

fn custom_parse_temperature_value(mut bytes: &[u8]) -> f32 {
    let sign = if bytes[0] == b'-' {
        bytes = &bytes[1..];
        -1f32
    } else {
        1f32
    };

    let offset = bytes.len() - 3;

    let first_digit = (bytes[offset] - b'0') as f32;
    let after_comma = (bytes[offset + 2] - b'0') as f32 * 0.1;
    let small_value = first_digit + after_comma;

    let unsigned_value = if offset == 0 {
        small_value
    } else {
        small_value + ((bytes[0] - b'0') * 10) as f32
    };

    unsigned_value.copysign(sign)
}

struct Statistics {
    min: f32,
    max: f32,
    total: f32,
    num_values: usize,
}

impl Statistics {
    fn new(value: f32) -> Self {
        Self {
            min: value,
            max: value,
            total: value,
            num_values: 1,
        }
    }
    fn add_value(&mut self, value: f32) {
        self.min = value.min(self.min);
        self.max = value.max(self.max);
        self.total += value;
        self.num_values += 1;
    }
}

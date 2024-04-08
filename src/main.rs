use ahash::AHashMap;
use std::io::Write;

use memchr::memchr;
use memmap::Mmap;
use rayon::{iter::ParallelIterator, slice::ParallelSlice};

// implementation: user time, wall time
// baseline: 150.46, 2:35.80
// no string copy: 124.91, 2:10.28
// ahash (no aes target): 105.43, 1:50.77
// with_capacity(10_00): 108.73, 1:54.06 -> SLOWER
// custom_parse: 90.66, 1:36.31
// no utf8 validation: 53.63, 0:59.38
// memchr: 52.27, 1:00.20
// mmap: 52.28, 1:00.22 -> NO CHANGE (after parallelism it had an impact)
// parallel dashmap: 335.80, 0:46.64
// parallel fold: 64.44, 0:08.60

const NUM_THREADS: usize = 8;

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()
        .expect("threadpool should be able to build");

    let file = std::fs::File::open("measurements.txt").expect("file should exist and be readable");
    let data = unsafe { Mmap::map(&file).unwrap() };

    assert!(*data.last().expect("file should not be empty") == b'\n');
    let data_trimmed = &data[..(data.len() - 1)];

    let stats_per_city = data_trimmed
        .par_split(|character| *character == b'\n')
        .fold(AHashMap::new, |mut stats_per_city, line| {
            let (city_name, temperature) = parse_line(line);

            stats_per_city
                .entry(city_name)
                .and_modify(|stats: &mut Statistics| stats.add_value(temperature))
                .or_insert_with(|| Statistics::new(temperature));

            stats_per_city
        })
        .reduce_with(merge_city_hashmaps_from_parallel_tasks)
        .unwrap();

    sort_and_print(stats_per_city);
}

fn parse_line(line: &[u8]) -> (&[u8], f32) {
    let separator_index = memchr(b';', line).unwrap();
    let (city_name, value_with_separator) = line.split_at(separator_index);
    let (_, value) = value_with_separator.split_first().unwrap();
    let parsed_value: f32 = parse_temperature_value(value);

    (city_name, parsed_value)
}

fn parse_temperature_value(mut bytes: &[u8]) -> f32 {
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

    fn merge_with(&mut self, other: &Self) {
        self.min = other.min.min(self.min);
        self.max = other.max.max(self.max);
        self.total += other.total;
        self.num_values += other.num_values;
    }
}

type CityHashMap<'a> = AHashMap<&'a [u8], Statistics>;

fn merge_city_hashmaps_from_parallel_tasks<'a>(
    stats_per_city1: CityHashMap<'a>,
    stats_per_city2: CityHashMap<'a>,
) -> CityHashMap<'a> {
    let (mut larger_stats_per_city, smaller_stats_per_city) =
        if stats_per_city1.len() > stats_per_city2.len() {
            (stats_per_city1, stats_per_city2)
        } else {
            (stats_per_city2, stats_per_city1)
        };

    for (city_name, new_stats) in smaller_stats_per_city.into_iter() {
        larger_stats_per_city
            .entry(city_name)
            .and_modify(|existing_stats| existing_stats.merge_with(&new_stats))
            .or_insert(new_stats);
    }

    larger_stats_per_city
}

fn sort_and_print(stats_per_city: CityHashMap) {
    let mut city_stats: Vec<_> = stats_per_city.into_iter().collect();
    city_stats.sort_unstable_by(|(name1, _), (name2, _)| name1.cmp(name2));

    let mut out = std::io::stdout();
    for (city_name, stats) in city_stats {
        out.write_all(city_name)
            .expect("should be able to write to stdout");
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

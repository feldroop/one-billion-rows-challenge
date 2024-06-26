use ahash::AHashMap;
use memmap::Mmap;
use rayon::{iter::ParallelIterator, slice::ParallelSlice};

const NUM_THREADS: usize = 8;

fn main() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()
        .expect("threadpool should be able to build");

    let file = std::fs::File::open("measurements.txt").expect("file should exist and be readable");
    let input_data = unsafe { Mmap::map(&file).unwrap() };

    assert!(*input_data.last().expect("file should not be empty") == b'\n');
    let (_, input_data_trimmed) = &input_data.split_last().unwrap();

    let stats_per_city = input_data_trimmed
        .par_split(|character| *character == b'\n')
        .fold(AHashMap::new, |mut stats_per_city, line| {
            let (city_name, temperature) = parse_line(line);

            stats_per_city
                .entry(city_name)
                .and_modify(|stats: &mut Statistics| stats.add_value(temperature))
                .or_insert_with(|| Statistics::new(temperature));

            stats_per_city
        })
        .reduce_with(merge_hashmaps_from_parallel_tasks)
        .unwrap();

    sort_and_print(stats_per_city);
}

fn parse_line(line: &[u8]) -> (&[u8], f32) {
    let separator_index = memchr::memchr(b';', line).unwrap();
    let (city_name, value_with_separator) = line.split_at(separator_index);
    let parsed_value: f32 = parse_temperature_value(value_with_separator);

    (city_name, parsed_value)
}

fn parse_temperature_value(bytes: &[u8]) -> f32 {
    // four variants: [;1.1] [;-1.1] [;11.1] [;-11.1]
    let last4 = bytes.last_chunk::<4>().unwrap();

    let mut parts = [
        (last4[0] - b'0') as f32 * 10.0,
        (last4[1] - b'0') as f32 * 1.0,
        (last4[2] - b'0') as f32 * 0.0,
        (last4[3] - b'0') as f32 * 0.1,
    ];

    if last4[0] == b';' || last4[0] == b'-' {
        parts[0] = 0.0;
    }

    let unsigned_value: f32 = parts.into_iter().sum();

    // this works because b'-' is smaller than b'/',
    // which is in turn smaller than the ascii byte of any number
    let sign = bytes[1] as f32 - (b'/' as f32);

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

fn merge_hashmaps_from_parallel_tasks<'a>(
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

    for (city_name, stats) in city_stats {
        println!(
            "{}={:.1}/{:.1}/{:.1}",
            std::str::from_utf8(city_name).expect("input should be utf8"),
            round_to_one_digit(stats.min),
            round_to_one_digit(stats.total / stats.num_values as f32),
            round_to_one_digit(stats.max)
        );
    }
}

fn round_to_one_digit(value: f32) -> f32 {
    (value * 10.0).round() / 10.0
}

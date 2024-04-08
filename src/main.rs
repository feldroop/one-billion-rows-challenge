use std::collections::hash_map::Entry;
use std::io::Write;

use ahash::AHashMap;

// baseline: 150.46
// no string copy: 124.91
// ahash (no aes target): 105.43
// with_capacity(10_00): 108.73 -> SLOWER
// custom_parse: 90.66
// no utf8 validation: 53.63

fn main() {
    let data = std::fs::read("measurements.txt").expect("file should be readable");

    // remove trailing whitespace
    assert!(*data.last().unwrap() == b'\n');
    let data_trimmed = &data[..(data.len() - 1)];

    let mut cities: AHashMap<_, Statistics> = AHashMap::new();

    for line in data_trimmed.split(|byte| *byte == b'\n') {
        let seperator_index = line.iter().position(|&b| b == b';').unwrap();

        let (city_name, value_with_separator) = line.split_at(seperator_index);
        let (_, value) = value_with_separator.split_first().unwrap();
        let parsed_value: f32 = custom_parse_temperature_value(value);

        match cities.entry(city_name) {
            Entry::Occupied(mut entry) => {
                let stats = entry.get_mut();
                stats.min = parsed_value.min(stats.min);
                stats.max = parsed_value.max(stats.max);
                stats.max += parsed_value;
                stats.num_values += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(Statistics {
                    min: parsed_value,
                    max: parsed_value,
                    total: parsed_value,
                    num_values: 1,
                });
            }
        };
    }

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
    // this still leaves some -0.0, but I am unsure wther this is wanted
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

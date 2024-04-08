use std::collections::hash_map::Entry;

use ahash::AHashMap;

// baseline: 150.46
// no string copy: 124.91
// ahash (no aes target): 105.43

fn main() {
    let data = std::fs::read_to_string("measurements.txt").unwrap();

    let mut cities: AHashMap<_, Statistics> = AHashMap::new();

    for line in data.lines() {
        let (city_name, value) = line.split_once(';').unwrap();
        let parsed_value: f32 = value.parse().unwrap();

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

    for (city_name, stats) in cities {
        println!(
            "{}={:.1}/{:.1}/{:.1}",
            city_name,
            stats.min,
            stats.total / stats.num_values as f32,
            stats.max
        );
    }
}

struct Statistics {
    min: f32,
    max: f32,
    total: f32,
    num_values: usize,
}

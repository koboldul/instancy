//! Adapted from upstream simple.rs:1-8, hello.rs:1-32,
//! exchange.rs:1-45, and distinct.rs:1-58.
//!
//! `simple` keeps the single-worker source 0..10 at time zero. `hello` keeps
//! ten rounds, worker-zero-only input, and exchange by the integer itself.
//! `exchange` fixes batch=8, rounds=4 and two workers, including the original
//! advance_to(round) convention: the first TWO batches have timestamp zero.
//! `distinct` keeps both original input partitions, not a substitute dataset.

use std::collections::{HashMap, HashSet};

use instancy::{DataflowBuilder, Result, SpawnOptions};

use super::helpers::{self, WORKERS};

pub fn simple() -> Result<()> {
    let builder = DataflowBuilder::<u64>::new("timely-simple");
    builder
        .source("0..10", vec![(0, (0..10u64).collect())])
        .output("output")?;
    let runtime = helpers::runtime()?;
    let mut spawned = runtime.spawn(builder.build()?, SpawnOptions::default())?;
    let output = spawned.take_output::<u64>("output")?;
    spawned.join_blocking()?;
    let mut seen: Vec<_> = output
        .collect_data()
        .into_iter()
        .flat_map(|(time, batch)| batch.into_iter().map(move |value| (time, value)))
        .collect();
    seen.sort();
    assert_eq!(seen, (0..10).map(|value| (0, value)).collect::<Vec<_>>());
    Ok(())
}

pub fn hello() -> Result<()> {
    let input = vec![
        (0..10u64).map(|round| (round, vec![round])).collect(),
        Vec::new(),
    ];
    let results = helpers::unary("timely-hello", input, |stream| {
        Ok(stream.exchange_by_hash("by-value", |value| *value))
    })?;
    for (worker, records) in results.iter().enumerate() {
        for &(time, value) in records {
            assert_eq!(time, value);
            assert_eq!(value as usize % WORKERS, worker);
        }
    }
    assert_eq!(
        helpers::sorted(results),
        (0..10).map(|round| (round, round)).collect::<Vec<_>>()
    );
    Ok(())
}

pub fn exchange() -> Result<()> {
    let partition: Vec<_> = (0..4u64)
        .map(|round| (round.saturating_sub(1), (0..8u64).collect()))
        .collect();
    let results = helpers::unary(
        "timely-exchange",
        vec![partition.clone(); WORKERS],
        |stream| Ok(stream.exchange_by_hash("by-value", |value| *value)),
    )?;
    for (worker, records) in results.into_iter().enumerate() {
        let mut expected = Vec::new();
        for (time, values) in &partition {
            for &value in values {
                if value as usize % WORKERS == worker {
                    expected.extend(std::iter::repeat_n((*time, value), WORKERS));
                }
            }
        }
        let mut records = records;
        records.sort();
        expected.sort();
        assert_eq!(records, expected);
    }
    Ok(())
}

pub fn distinct() -> Result<()> {
    let results = helpers::unary(
        "timely-distinct",
        vec![
            vec![(0, vec![0u64, 1, 2, 2, 2, 3, 3, 4])],
            vec![(0, vec![0u64, 0, 3, 4, 4, 5, 7, 7])],
        ],
        |stream| {
            let mut seen: HashMap<u64, HashSet<u64>> = HashMap::new();
            Ok(stream.exchange_by_hash("by-value", |value| *value).unary(
                "Distinct",
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        let counts = seen.entry(time).or_default();
                        let unique = data
                            .into_iter()
                            .filter(|value| counts.insert(*value))
                            .collect();
                        output.push_vec(time, unique);
                    }
                    Ok(())
                },
            ))
        },
    )?;
    for (worker, records) in results.iter().enumerate() {
        for &(time, value) in records {
            assert_eq!(time, 0);
            assert_eq!(value as usize % WORKERS, worker);
        }
    }
    assert_eq!(
        helpers::sorted(results),
        [0, 1, 2, 3, 4, 5, 7].map(|value| (0, value)).to_vec()
    );
    Ok(())
}

//! Adapted from upstream hashjoin.rs:1-106. Fixes keys=4, vals=16,
//! batch=2 and workers=2. Keeps DefaultHasher's exact tuple inputs (including
//! the upstream default i32 generator index), two independently exchanged
//! inputs, persistent duplicate-preserving maps, and epoch probes.

use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

use instancy::{DataflowBuilder, Result, SpawnOptions};

use super::helpers::{self, WORKERS};

pub fn run() -> Result<()> {
    let runtime = helpers::runtime()?;
    let probe = RefCell::new(None);
    let mut spawned = runtime.spawn_multi(
        "timely-hashjoin",
        WORKERS,
        |builder: &mut DataflowBuilder<u64>| {
            let left = builder
                .input::<(u64, u64)>("left")?
                .exchange_by_hash("left-key", |pair| pair.0);
            let right = builder
                .input::<(u64, u64)>("right")?
                .exchange_by_hash("right-key", |pair| pair.0);
            let mut map1: HashMap<u64, Vec<u64>> = HashMap::new();
            let mut map2: HashMap<u64, Vec<u64>> = HashMap::new();
            let joined = left.binary(right, "HashJoin", move |input1, input2, output| {
                while let Some((time, data)) = input1.next() {
                    let mut joined = Vec::new();
                    for (key, val1) in data {
                        if let Some(values) = map2.get(&key) {
                            joined.extend(values.iter().map(|val2| (val1, *val2)));
                        }
                        map1.entry(key).or_default().push(val1);
                    }
                    output.push_vec(time, joined);
                }
                while let Some((time, data)) = input2.next() {
                    let mut joined = Vec::new();
                    for (key, val2) in data {
                        if let Some(values) = map1.get(&key) {
                            joined.extend(values.iter().map(|val1| (*val1, val2)));
                        }
                        map2.entry(key).or_default().push(val2);
                    }
                    output.push_vec(time, joined);
                }
                Ok(())
            })?;
            let (joined, handle) = joined.probe();
            *probe.borrow_mut() = Some(handle);
            joined.output("joined")?;
            Ok(())
        },
        SpawnOptions::default(),
    )?;
    let probe = probe.into_inner().expect("build installs a probe");
    let left = spawned.take_all_inputs::<(u64, u64)>("left")?;
    let right = spawned.take_all_inputs::<(u64, u64)>("right")?;
    let outputs = helpers::collect::<(u64, u64)>(&mut spawned, "joined")?;
    let completion = helpers::start_completion(spawned);
    let hasher = BuildHasherDefault::<DefaultHasher>::new();
    let mut left_records = Vec::new();
    let mut right_records = Vec::new();
    let feed: Result<()> = (|| {
        for epoch in 0..4u64 {
            for worker in 0..WORKERS {
                let mut batch1 = Vec::new();
                let mut batch2 = Vec::new();
                for index in (epoch * 2) as i32..(epoch * 2 + 2) as i32 {
                    let key1 = hasher.hash_one((index, worker, 0i32)) % 4;
                    let val1 = hasher.hash_one((index, worker, 1i32)) % 4;
                    let key2 = hasher.hash_one((index, worker, 2i32)) % 4;
                    let val2 = hasher.hash_one((index, worker, 3i32)) % 4;
                    batch1.push((key1, val1));
                    batch2.push((key2, val2));
                    left_records.push((epoch, key1, val1));
                    right_records.push((epoch, key2, val2));
                }
                left[worker].send(epoch, batch1)?;
                right[worker].send(epoch, batch2)?;
                left[worker].advance_to(epoch + 1)?;
                right[worker].advance_to(epoch + 1)?;
            }
            helpers::wait_epoch(&probe, epoch, &completion);
        }
        Ok(())
    })();
    drop((left, right));
    helpers::join_completion(completion)?;
    feed?;
    assert!(probe.is_done());
    let mut expected = vec![Vec::new(); WORKERS];
    for &(time1, key1, val1) in &left_records {
        for &(time2, key2, val2) in &right_records {
            if key1 == key2 {
                expected[key1 as usize % WORKERS].push((time1.max(time2), (val1, val2)));
            }
        }
    }
    for (mut actual, mut expected) in helpers::flatten(outputs).into_iter().zip(expected) {
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
    Ok(())
}

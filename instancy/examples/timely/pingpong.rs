//! Adapted from upstream pingpong.rs:1-27. Fixes iterations=4,
//! elements=6, workers=2. Keeps three zero-valued records per worker,
//! exchange BEFORE increment, the inclusive <= iterations filter, and
//! feedback with summary +1. The original scalar timestamp t is represented
//! by Product(0,t), an order-preserving bijection for these time-zero inputs.
//! Trace records retain t explicitly because nested outputs project to zero.
//! The terminal output observes records the original filter discards.

use instancy::{DataflowBuilder, IterateResult, Result, SpawnOptions};

use super::helpers::{self, WORKERS};

const ITERATIONS: u64 = 4;
const ELEMENTS: usize = 6;

pub fn run() -> Result<()> {
    let runtime = helpers::runtime()?;
    let mut spawned = runtime.spawn_multi(
        "timely-pingpong",
        WORKERS,
        |builder: &mut DataflowBuilder<u64>| {
            let mut trace = None;
            let result =
                builder
                    .input::<(u64, u64)>("input")?
                    .iterate::<u64>("feedback+1", 1, |stream| {
                        let step = stream
                            .exchange_by_hash("by-counter-before-increment", |record| record.0)
                            .map("increment", |time, (counter, _)| (counter + 1, time.inner));
                        trace = Some(step.clone().output("trace"));
                        IterateResult {
                            feedback: step
                                .clone()
                                .filter("inclusive-limit", |_time, record| record.0 <= ITERATIONS),
                            output: step.filter("discarded", |_time, record| record.0 > ITERATIONS),
                        }
                    });
            trace.expect("loop installs trace")?;
            result.output("discarded")?;
            Ok(())
        },
        SpawnOptions::default(),
    )?;
    let inputs = spawned.take_all_inputs::<(u64, u64)>("input")?;
    let trace = helpers::collect::<(u64, u64)>(&mut spawned, "trace")?;
    let discarded = helpers::collect::<(u64, u64)>(&mut spawned, "discarded")?;
    let completion = helpers::start_completion(spawned);
    let feed: Result<()> = (|| {
        for (worker, sender) in inputs.iter().enumerate() {
            let local = (0..ELEMENTS)
                .filter(|element| element % WORKERS == worker)
                .map(|_| (0, 0))
                .collect();
            sender.send(0, local)?;
        }
        Ok(())
    })();
    drop(inputs);
    helpers::join_completion(completion)?;
    feed?;
    for (worker, mut actual) in helpers::flatten(trace).into_iter().enumerate() {
        let mut expected = Vec::new();
        for time in 0..=ITERATIONS {
            if time as usize % WORKERS == worker {
                expected.extend(std::iter::repeat_n((0, (time + 1, time)), ELEMENTS));
            }
        }
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }
    assert_eq!(
        helpers::sorted(helpers::flatten(discarded)),
        vec![(0, (ITERATIONS + 1, ITERATIONS)); ELEMENTS]
    );
    Ok(())
}

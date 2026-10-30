//! Adapted from upstream event_driven.rs:1-54. Fixes dataflows=2,
//! length=3 and rounds=12 instead of the unbounded loop, exercising BOTH
//! record and progress-only modes. Both dataflows remain alive together.
//! Timely's timestamp-neutral region is flattened; its three identity maps,
//! round-robin activation, input times, and per-round probes are unchanged.
//! Scheduler step counts are not portable across the two runtimes.

use std::cell::RefCell;

use instancy::{DataflowBuilder, Result, SpawnOptions};

use super::helpers::{self, WORKERS};

pub fn run() -> Result<()> {
    run_mode(false)?;
    run_mode(true)
}

fn run_mode(record: bool) -> Result<()> {
    let runtime = helpers::runtime()?;
    let mut flows = Vec::new();
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut probes = Vec::new();
    for dataflow in 0..2 {
        let probe = RefCell::new(None);
        let mut spawned = runtime.spawn_multi(
            &format!("timely-event-driven-{dataflow}"),
            WORKERS,
            |builder: &mut DataflowBuilder<u64>| {
                let mut stream = builder.input::<()>("input")?;
                for step in 0..3 {
                    stream = stream.map(format!("identity-{step}"), |_time, value| value);
                }
                let (stream, handle) = stream.probe();
                *probe.borrow_mut() = Some(handle);
                stream.output("output")?;
                Ok(())
            },
            SpawnOptions::default(),
        )?;
        probes.push(probe.into_inner().expect("build installs a probe"));
        inputs.push(spawned.take_all_inputs::<()>("input")?);
        outputs.push(helpers::collect::<()>(&mut spawned, "output")?);
        flows.push(helpers::start_completion(spawned));
    }
    let mut times = [0u64; 2];
    let mut expected = [Vec::new(), Vec::new()];
    let feed: Result<()> = (|| {
        for round in 0..12u64 {
            let dataflow = round as usize % 2;
            if record {
                for sender in &inputs[dataflow] {
                    sender.send(times[dataflow], vec![()])?;
                }
                expected[dataflow].push((times[dataflow], ()));
            }
            for sender in &inputs[dataflow] {
                sender.advance_to(round)?;
            }
            times[dataflow] = round;
            if round > 0 {
                helpers::wait_epoch(&probes[dataflow], round - 1, &flows[dataflow]);
            }
        }
        Ok(())
    })();
    drop(inputs);
    // Join every independently spawned flow even if one reports an error.
    let results: Vec<_> = flows.into_iter().map(helpers::join_completion).collect();
    for result in results {
        result?;
    }
    feed?;
    for ((receivers, mut expected), probe) in outputs.into_iter().zip(expected).zip(probes) {
        assert!(probe.is_done());
        expected.sort();
        for mut records in helpers::flatten(receivers) {
            records.sort();
            assert_eq!(records, expected);
        }
    }
    Ok(())
}

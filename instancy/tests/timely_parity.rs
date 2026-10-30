//! Behavioral ports from timely-dataflow at commit 492787b.
//! Source coverage and deliberate API exclusions are recorded in timely/coverage.tsv.
//! Upstream notices are preserved in examples/timely/LICENSE.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use instancy::dataflow::OutputEvent;
use instancy::{DataflowBuilder, ProbeHandle, RuntimeConfig, RuntimeHandle, SpawnOptions};

#[path = "timely/support.rs"]
mod support;
#[path = "../examples/timely/mod.rs"]
mod timely;

#[test]
fn native_examples() {
    for (name, _) in timely::SCENARIOS {
        support::run_child("example_child", &[("INSTANCY_TIMELY_EXAMPLE", name)]);
    }
}

#[test]
#[ignore = "subprocess entry point; native_examples invokes every registered scenario"]
fn example_child() {
    let name = std::env::var("INSTANCY_TIMELY_EXAMPLE").expect("scenario supplied by parent");
    eprintln!("Timely example: {name}");
    timely::run_named(&name).unwrap();
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
    .unwrap()
}

fn wait_for(description: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out: {description}");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn gh_523_empty_container_before_exchange() {
    support::isolated("gh_523_empty_container_before_exchange", || {
        let builder = DataflowBuilder::<u64>::new("timely-gh-523");
        let (stream, probe) = builder
            .input::<u64>("input")
            .unwrap()
            .unary("empty-then-data", |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    session.give_vec(&mut Vec::new());
                    session.give_iterator(data);
                }
                Ok(())
            })
            .exchange_by_hash("exchange", |value| *value)
            .probe();
        stream.output("output").unwrap();
        let rt = runtime();
        let mut spawned = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let sender = spawned.take_input::<u64>("input").unwrap();
        let output = spawned.take_output::<u64>("output").unwrap();
        for round in 0..2 {
            sender.send(round, vec![round]).unwrap();
            sender.advance_to(round + 1).unwrap();
        }
        drop(sender);
        spawned.join_blocking().unwrap();
        assert!(probe.is_done());
        let mut records: Vec<_> = output
            .collect_data()
            .into_iter()
            .flat_map(|(time, batch)| batch.into_iter().map(move |value| (time, value)))
            .collect();
        records.sort();
        assert_eq!(records, vec![(0, 0), (1, 1)]);
    });
}

#[test]
fn gh_824_flush_before_frontier_advance() {
    support::isolated("gh_824_flush_before_frontier_advance", || {
        let probe_slot = Mutex::new(None::<ProbeHandle<u64>>);
        let rt = runtime();
        let mut spawned = rt
            .spawn_multi(
                "timely-gh-824",
                2,
                |builder| {
                    let (stream, probe) = builder
                        .input::<u64>("input")?
                        .exchange_by_hash("exchange", |value| *value)
                        .probe();
                    *probe_slot.lock().unwrap() = Some(probe);
                    stream.output("output")?;
                    Ok(())
                },
                SpawnOptions::default(),
            )
            .unwrap();
        let probe = probe_slot.into_inner().unwrap().unwrap();
        let senders = spawned.take_all_inputs::<u64>("input").unwrap();
        let receivers = spawned.take_all_outputs::<u64>("output").unwrap();
        senders[0].advance_to(1).unwrap();
        senders[0].send(1, vec![0]).unwrap();
        senders[1].advance_to(2).unwrap();

        // Observing delivery forces processing between send and advance; Instancy
        // schedules asynchronously and has no explicit input.flush/worker.step.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                Instant::now() < deadline,
                "exchanged record was not delivered"
            );
            if let Some(OutputEvent::Data { time, data }) =
                receivers[0].recv_timeout(Duration::from_millis(10))
            {
                assert_eq!((time, data), (1, vec![0]));
                break;
            }
        }
        assert!(!probe.done_with(&1), "the first input is still open at 1");
        senders[0].advance_to(2).unwrap();
        wait_for("both exchange frontiers reach 2", || probe.done_with(&1));
        drop(senders);
        spawned.join_blocking().unwrap();
        assert!(probe.is_done());
        for receiver in receivers {
            assert!(
                receiver.collect_data().is_empty(),
                "unexpected extra records"
            );
        }
    });
}

#[test]
fn core_map_builder_records() {
    support::isolated("core_map_builder_records", || {
        let builder = DataflowBuilder::<u64>::new("timely-map-builder");
        builder
            .source("input", vec![(0, (0..10i32).collect())])
            .flat_map("flat-map-builder", |_time, value| vec![value + 1])
            .map("map-1", |_time, value| value + 1)
            .map("map-2", |_time, value| value + 1)
            .map("map-3", |_time, value| value + 1)
            .map("some", |_time, value| Some(value))
            .flat_map("into-stream", |_time, value| value.into_iter().collect())
            .output("output")
            .unwrap();
        let rt = runtime();
        let mut spawned = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let output = spawned.take_output::<i32>("output").unwrap();
        spawned.join_blocking().unwrap();
        let records: Vec<_> = output
            .collect_data()
            .into_iter()
            .flat_map(|(time, batch)| {
                assert_eq!(time, 0);
                batch
            })
            .collect();
        assert_eq!(records, (4..14).collect::<Vec<_>>());
    });
}

#[test]
fn core_probe_frontier_and_close() {
    support::isolated("core_probe_frontier_and_close", || {
        let builder = DataflowBuilder::<u64>::new("timely-probe");
        let (stream, probe) = builder.input::<u64>("input").unwrap().probe();
        stream.output("output").unwrap();
        let rt = runtime();
        let mut spawned = rt
            .spawn(builder.build().unwrap(), SpawnOptions::default())
            .unwrap();
        let sender = spawned.take_input::<u64>("input").unwrap();
        let output = spawned.take_output::<u64>("output").unwrap();
        for round in 0..10 {
            let frontier = probe.frontier();
            assert!(!probe.is_done());
            assert!(frontier.less_equal(&round));
            assert!(frontier.less_than(&(round + 1)));
            sender.advance_to(round + 1).unwrap();
            wait_for("input frontier advance reaches probe", || {
                probe.done_with(&round)
            });
        }
        drop(sender);
        spawned.join_blocking().unwrap();
        assert!(probe.is_done());
        assert!(output.collect_data().is_empty());
    });
}

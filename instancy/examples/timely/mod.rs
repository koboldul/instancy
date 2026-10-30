//! Finite native Instancy adaptations of compatible Timely examples.
//!
//! Upstream: TimelyDataflow/timely-dataflow, commit
//! `492787b7cff9463aab8a793a306a619c4175885b`, `timely/examples/*.rs`.
//! Original copyright and permission notices are in the adjacent LICENSE.
//!
//! Every scenario checks records, timestamps, and worker placement. Physical
//! scheduling and benchmark wall-clock rates are not deterministic assertions.
//! See each scenario's module for the exact finite parameters and adaptations.
//!
//! `event_driven` is partial: timestamp-neutral region identity and its nested
//! progress topology are not covered by the flattened native pipeline.
//! Unsupported sources are not registered as successful scenarios: barrier,
//! capture_recv, capture_send, columnar, logging-recv, logging-send,
//! logging_replay, loopdemo, openloop, pagerank, rc, sequence, spill_compare,
//! threadless, and unordered_input.

mod basics;
mod bfs;
mod event_driven;
mod hashjoin;
mod helpers;
mod pingpong;
mod unionfind;
mod wordcount;

pub type Scenario = (&'static str, fn() -> instancy::Result<()>);

pub const SCENARIOS: &[Scenario] = &[
    ("simple", basics::simple),
    ("hello", basics::hello),
    ("exchange", basics::exchange),
    ("distinct", basics::distinct),
    ("event_driven", event_driven::run),
    ("hashjoin", hashjoin::run),
    ("wordcount", wordcount::run),
    ("unionfind", unionfind::run),
    ("pingpong", pingpong::run),
    ("bfs", bfs::run),
];

pub fn run_named(name: &str) -> instancy::Result<()> {
    if name == "all" {
        for &(name, run) in SCENARIOS {
            run()?;
            println!("{name}: passed");
        }
        return Ok(());
    }
    let (_, run) = SCENARIOS
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .ok_or_else(|| {
            instancy::Error::from(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown Timely scenario {name:?}; select all or a name in SCENARIOS"),
            ))
        })?;
    run()
}

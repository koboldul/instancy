//! Adapted from upstream wordcount.rs:1-69. Two workers each send
//! ("round", +1) at times 0..10, as upstream does. Word-length exchange,
//! frontier-complete buffering, persistent signed counts, and EVERY emitted
//! running update are preserved; this is not per-epoch final aggregation.

use std::collections::HashMap;

use instancy::Result;

use super::helpers::{self, WORKERS};

pub fn run() -> Result<()> {
    let partition = (0..10)
        .map(|round| (round, vec![("round".to_owned(), 1i64)]))
        .collect::<Vec<_>>();
    let results = helpers::unary("timely-wordcount", vec![partition; WORKERS], |stream| {
        let mut queues: HashMap<u64, Vec<(String, i64)>> = HashMap::new();
        let mut counts: HashMap<String, i64> = HashMap::new();
        Ok(stream
            .flat_map("split", |_time, (text, diff)| {
                text.split_whitespace()
                    .map(|word| (word.to_owned(), diff))
                    .collect()
            })
            .exchange_by_hash("by-word-length", |(word, _)| word.len() as u64)
            .unary_notify("WordCount", move |input, output, context| {
                while let Some((time, data)) = input.next() {
                    queues.entry(time).or_default().extend(data);
                    context.notify_at(time);
                }
                while let Some(time) = context.next_notification() {
                    let updates = queues
                        .remove(&time)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(word, diff)| {
                            let count = counts.entry(word.clone()).or_default();
                            *count += diff;
                            (word, *count)
                        })
                        .collect();
                    output.push_vec(time, updates);
                }
                Ok(())
            }))
    })?;
    assert!(results[0].is_empty(), "\"round\" has odd length");
    let mut expected = Vec::new();
    for time in 0..10u64 {
        for offset in 1..=WORKERS {
            expected.push((
                time,
                (
                    "round".to_owned(),
                    (time as usize * WORKERS + offset) as i64,
                ),
            ));
        }
    }
    assert_eq!(helpers::sorted(results), expected);
    Ok(())
}

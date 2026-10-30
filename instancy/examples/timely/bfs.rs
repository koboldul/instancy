//! Adapted from upstream bfs.rs:1-147. Fixes nodes=8, edges=32,
//! workers=2, keeping the exact DefaultHasher graph generator, source-node
//! partition, sorted CSR adjacency, first-visit parent choice, root (0,0)
//! seeded once PER worker, and summary +1 feedback.
//!
//! Graph records are at time zero. Root visits enter at time one, exactly as
//! in the original concat-after-BFS feedback wiring. A tagged merged input
//! with frontier notifications waits on both graph and visit streams.
//! Exchange uses built-in tuple codecs; the enum stays local to operators.
//! Product(0,t) represents scalar time t bijectively. Discovery records
//! explicitly retain t for assertions after leaving the nested scope.
//! Nondeterministic same-level parent ties are checked against shortest paths.

use std::collections::{BTreeMap, VecDeque};
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

use instancy::{DataflowBuilder, IterateResult, Product, Result, SpawnOptions};

use super::helpers::{self, WORKERS};

const NODES: usize = 8;
const EDGES: usize = 32;

#[derive(Clone, Debug)]
enum Record {
    Edge(u32, u32),
    Seed,
    Visit(u32, u32),
    Discovered { node: u32, parent: u32, time: u64 },
}

fn encode(record: Record) -> (u8, (u32, u32)) {
    match record {
        Record::Edge(node, target) => (0, (node, target)),
        Record::Seed => (1, (0, 0)),
        Record::Visit(node, parent) => (2, (node, parent)),
        Record::Discovered { .. } => unreachable!("discoveries never enter feedback"),
    }
}

fn decode((tag, (node, target)): (u8, (u32, u32))) -> Record {
    match tag {
        0 => Record::Edge(node, target),
        1 => Record::Seed,
        2 => Record::Visit(node, target),
        _ => unreachable!("only locally encoded tags are exchanged"),
    }
}

pub fn run() -> Result<()> {
    let runtime = helpers::runtime()?;
    let mut spawned = runtime.spawn_multi(
        "timely-bfs",
        WORKERS,
        |builder: &mut DataflowBuilder<u64>| {
            let result =
                builder
                    .input::<Record>("input")?
                    .iterate::<u64>("feedback+1", 1, |stream| {
                        let mut queues: BTreeMap<Product<u64, u64>, Vec<Record>> = BTreeMap::new();
                        let mut offsets = vec![0; 1 + NODES / WORKERS];
                        let mut targets = Vec::new();
                        let mut done = [u32::MAX; 1 + NODES / WORKERS];
                        let step = stream
                            .map("encode", |_time, record| encode(record))
                            .exchange_by_hash("by-source-node", |record| u64::from(record.1.0))
                            .map("decode", |_time, record| decode(record))
                            .unary_notify("BFS", move |input, output, context| {
                                while let Some((time, data)) = input.next() {
                                    queues.entry(time).or_default().extend(data);
                                    context.notify_at(time);
                                }
                                while let Some(time) = context.next_notification() {
                                    let mut batch = queues.remove(&time).unwrap_or_default();
                                    if time.inner == 0 {
                                        let mut edges = batch
                                            .iter()
                                            .filter_map(|record| match record {
                                                Record::Edge(src, dst) => Some((*src, *dst)),
                                                _ => None,
                                            })
                                            .collect::<Vec<_>>();
                                        edges.sort();
                                        offsets.clear();
                                        offsets.push(0);
                                        let mut previous = 0;
                                        for (src, dst) in edges {
                                            let local = src as usize / WORKERS;
                                            while previous < local {
                                                previous += 1;
                                                offsets.push(targets.len());
                                            }
                                            targets.push(dst);
                                        }
                                        while offsets.len() < 1 + NODES / WORKERS {
                                            offsets.push(targets.len());
                                        }
                                    }
                                    let mut changes = Vec::new();
                                    for record in batch.drain(..) {
                                        match record {
                                            Record::Seed => changes.push(Record::Visit(0, 0)),
                                            Record::Visit(node, parent) => {
                                                let local = node as usize / WORKERS;
                                                if done[local] != u32::MAX {
                                                    continue;
                                                }
                                                done[local] = parent;
                                                changes.push(Record::Discovered {
                                                    node,
                                                    parent,
                                                    time: time.inner,
                                                });
                                                for &target in
                                                    &targets[offsets[local]..offsets[local + 1]]
                                                {
                                                    changes.push(Record::Visit(target, node));
                                                }
                                            }
                                            Record::Edge(_, _) => {}
                                            Record::Discovered { .. } => unreachable!(),
                                        }
                                    }
                                    output.push_vec(time, changes);
                                }
                                Ok(())
                            });
                        IterateResult {
                            feedback: step.clone().filter("next-level", |_time, record| {
                                matches!(record, Record::Visit(..))
                            }),
                            output: step.filter("parents", |_time, record| {
                                matches!(record, Record::Discovered { .. })
                            }),
                        }
                    });
            result.output("parents")?;
            Ok(())
        },
        SpawnOptions::default(),
    )?;
    let inputs = spawned.take_all_inputs::<Record>("input")?;
    let outputs = helpers::collect::<Record>(&mut spawned, "parents")?;
    let completion = helpers::start_completion(spawned);
    let hasher = BuildHasherDefault::<DefaultHasher>::new();
    let mut graph = vec![Vec::new(); NODES];
    let feed: Result<()> = (|| {
        for (worker, sender) in inputs.iter().enumerate() {
            let mut data = Vec::new();
            for index in 0..EDGES / WORKERS {
                let src = hasher.hash_one((index, worker, 0i32)) as usize % NODES;
                let dst = hasher.hash_one((index, worker, 1i32)) as usize % NODES;
                graph[src].push(dst);
                data.push(Record::Edge(src as u32, dst as u32));
            }
            data.push(Record::Seed);
            sender.send(0, data)?;
        }
        Ok(())
    })();
    drop(inputs);
    helpers::join_completion(completion)?;
    feed?;
    let mut distance = [usize::MAX; NODES];
    distance[0] = 0;
    let mut queue = VecDeque::from([0]);
    while let Some(node) = queue.pop_front() {
        for &target in &graph[node] {
            if distance[target] == usize::MAX {
                distance[target] = distance[node] + 1;
                queue.push_back(target);
            }
        }
    }
    let mut discovered = [false; NODES];
    for (worker, records) in helpers::flatten(outputs).into_iter().enumerate() {
        for (outer, record) in records {
            let Record::Discovered { node, parent, time } = record else {
                panic!("only completed parents may leave BFS");
            };
            let node = node as usize;
            let parent = parent as usize;
            assert_eq!(outer, 0);
            assert_eq!(node % WORKERS, worker);
            assert!(!discovered[node], "a node must be discovered only once");
            discovered[node] = true;
            assert_eq!(time as usize, distance[node] + 1);
            if node == 0 {
                assert_eq!((parent, time), (0, 1));
            } else {
                assert!(graph[parent].contains(&node));
                assert_eq!(distance[parent] + 1, distance[node]);
            }
        }
    }
    assert_eq!(discovered, distance.map(|distance| distance != usize::MAX));
    Ok(())
}

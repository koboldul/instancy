//! Adapted from upstream unionfind.rs:1-95. Fixes nodes=8, edges=16,
//! batch=2 and workers=2; retains the DefaultHasher-generated per-worker
//! inputs and both union-by-rank stages (local, then exchange to worker zero).
//! Exchange arrival order can select different valid spanning forests.
//! Assertions compare connectivity at EVERY original batch timestamp.

use std::cmp::Ordering;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};

use instancy::{Pipe, Result};

use super::helpers::{self, WORKERS};

const NODES: usize = 8;

fn union_find(stream: Pipe<u64, (usize, usize)>, name: &str) -> Pipe<u64, (usize, usize)> {
    let mut roots = Vec::new();
    let mut ranks = Vec::new();
    stream.unary(name, move |input, output| {
        while let Some((time, data)) = input.next() {
            let mut forest = Vec::new();
            for (mut x, mut y) in data {
                for node in roots.len()..=x.max(y) {
                    roots.push(node);
                    ranks.push(0u8);
                }
                while x != roots[x] {
                    x = roots[x];
                }
                while y != roots[y] {
                    y = roots[y];
                }
                if x == y {
                    continue;
                }
                forest.push((x, y));
                match ranks[x].cmp(&ranks[y]) {
                    Ordering::Less => roots[x] = y,
                    Ordering::Greater => roots[y] = x,
                    Ordering::Equal => {
                        roots[y] = x;
                        ranks[x] += 1;
                    }
                }
            }
            output.push_vec(time, forest);
        }
        Ok(())
    })
}

fn connectivity(edges: &[(usize, usize)]) -> [[bool; NODES]; NODES] {
    let mut reachable = [[false; NODES]; NODES];
    for (node, row) in reachable.iter_mut().enumerate() {
        row[node] = true;
    }
    for &(x, y) in edges {
        reachable[x][y] = true;
        reachable[y][x] = true;
    }
    for middle in 0..NODES {
        for from in 0..NODES {
            for to in 0..NODES {
                reachable[from][to] |= reachable[from][middle] && reachable[middle][to];
            }
        }
    }
    reachable
}

pub fn run() -> Result<()> {
    let hasher = BuildHasherDefault::<DefaultHasher>::new();
    let partitions = (0..WORKERS)
        .map(|worker| {
            (0..4u64)
                .map(|epoch| {
                    let edges = ((epoch * 2) as i32..(epoch * 2 + 2) as i32)
                        .map(|index| {
                            (
                                hasher.hash_one((index, worker, 0i32)) as usize % NODES,
                                hasher.hash_one((index, worker, 1i32)) as usize % NODES,
                            )
                        })
                        .collect();
                    (epoch, edges)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let results = helpers::unary("timely-unionfind", partitions.clone(), |stream| {
        let local = union_find(stream, "local-union-find")
            .map("encode-node-ids", |_time, (x, y)| (x as u64, y as u64))
            .exchange_by_hash("worker-zero", |_| 0)
            .map("decode-node-ids", |_time, (x, y)| {
                (
                    usize::try_from(x).expect("node ID originated as usize"),
                    usize::try_from(y).expect("node ID originated as usize"),
                )
            });
        Ok(union_find(local, "global-union-find"))
    })?;
    assert!(results[1].is_empty());
    let mut forest = Vec::new();
    let mut input = Vec::new();
    let records = helpers::sorted(results);
    for epoch in 0..4u64 {
        for partition in &partitions {
            input.extend(partition[epoch as usize].1.iter().copied());
        }
        for &(_, edge) in records.iter().filter(|(time, _)| *time == epoch) {
            assert!(
                !connectivity(&forest)[edge.0][edge.1],
                "forest must not cycle"
            );
            forest.push(edge);
        }
        assert_eq!(connectivity(&forest), connectivity(&input));
    }
    assert!(records.iter().all(|(time, _)| *time < 4));
    Ok(())
}

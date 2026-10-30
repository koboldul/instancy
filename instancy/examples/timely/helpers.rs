use std::cell::RefCell;
use std::collections::BTreeMap;

use instancy::{
    DataflowBuilder, MultiSpawnedDataflow, Pipe, ProbeHandle, Result, RuntimeConfig, RuntimeHandle,
    SpawnOptions,
};

pub const WORKERS: usize = 2;
pub type Partition<D> = Vec<(u64, Vec<D>)>;
pub type Records<D> = Vec<Vec<(u64, D)>>;

pub fn runtime() -> Result<RuntimeHandle> {
    RuntimeHandle::new(RuntimeConfig {
        worker_threads: 2,
        ..RuntimeConfig::default()
    })
}

pub fn wait_epoch(
    probe: &ProbeHandle<u64>,
    time: u64,
    completion: &std::thread::JoinHandle<Result<()>>,
) {
    while !probe.done_with(&time) && !completion.is_finished() {
        std::thread::yield_now();
    }
}

pub fn start_completion(spawned: MultiSpawnedDataflow<u64>) -> std::thread::JoinHandle<Result<()>> {
    std::thread::spawn(move || spawned.join_blocking())
}

pub fn join_completion(completion: std::thread::JoinHandle<Result<()>>) -> Result<()> {
    completion
        .join()
        .unwrap_or_else(|_| Err(std::io::Error::other("completion waiter panicked").into()))
}

pub fn collect<D: Send + 'static>(
    spawned: &mut MultiSpawnedDataflow<u64>,
    name: &str,
) -> Result<Vec<instancy::OutputReceiver<u64, D>>> {
    spawned.take_all_outputs(name)
}

pub fn flatten<D: Send + 'static>(outputs: Vec<instancy::OutputReceiver<u64, D>>) -> Records<D> {
    outputs
        .into_iter()
        .map(|receiver| {
            receiver
                .collect_data()
                .into_iter()
                .flat_map(|(time, data)| data.into_iter().map(move |datum| (time, datum)))
                .collect()
        })
        .collect()
}

pub fn unary<D, E, F>(name: &str, partitions: Vec<Partition<D>>, build: F) -> Result<Records<E>>
where
    D: Clone + Send + 'static,
    E: Clone + Send + 'static,
    F: Fn(Pipe<u64, D>) -> Result<Pipe<u64, E>>,
{
    let runtime = runtime()?;
    let probe = RefCell::new(None);
    let workers = partitions.len();
    let mut spawned = runtime.spawn_multi(
        name,
        workers,
        |builder: &mut DataflowBuilder<u64>| {
            let (stream, handle) = build(builder.input::<D>("input")?)?.probe();
            *probe.borrow_mut() = Some(handle);
            stream.output("output")?;
            Ok(())
        },
        SpawnOptions::default(),
    )?;
    let probe = probe.into_inner().expect("build installs a probe");
    let senders = spawned.take_all_inputs::<D>("input")?;
    let outputs = collect::<E>(&mut spawned, "output")?;
    let completion = start_completion(spawned);
    let mut epochs: BTreeMap<u64, Vec<Vec<D>>> = BTreeMap::new();
    for (worker, partition) in partitions.into_iter().enumerate() {
        for (time, batch) in partition {
            epochs
                .entry(time)
                .or_insert_with(|| vec![Vec::new(); workers])[worker]
                .extend(batch);
        }
    }
    let feed: Result<()> = (|| {
        for (time, batches) in epochs {
            for (sender, data) in senders.iter().zip(batches) {
                sender.send(time, data)?;
                sender.advance_to(time + 1)?;
            }
            wait_epoch(&probe, time, &completion);
        }
        Ok(())
    })();
    drop(senders);
    // Prefer the runtime's actual execution error over a consequential closed input.
    join_completion(completion)?;
    feed?;
    assert!(probe.is_done());
    Ok(flatten(outputs))
}

pub fn sorted<D: Ord>(records: Records<D>) -> Vec<(u64, D)> {
    let mut records: Vec<_> = records.into_iter().flatten().collect();
    records.sort();
    records
}

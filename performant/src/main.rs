use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::ErrorKind,
    os::unix::fs::FileExt,
    str::from_utf8_unchecked,
    sync::{
        OnceLock,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, available_parallelism},
};

// Config
static CHUNK_SIZE: usize = 0x100;

// Shared counters
static WORK_COUNTER: OnceLock<AtomicUsize> = OnceLock::new();
static WORKERS_DONE: OnceLock<AtomicUsize> = OnceLock::new();
static WORKERS_COUNT: OnceLock<usize> = OnceLock::new();

// Thread locals
thread_local! {
    static THREAD_ID: OnceLock<usize> = OnceLock::new();
}

#[derive(Debug)]
struct ChunkResult(HashMap<String, Record>);

#[derive(Clone)]
struct ReaderState {
    channel: mpsc::Sender<ChunkResult>,
    file: String,
}

struct JoinerState {
    channel: mpsc::Receiver<ChunkResult>,
}

fn main() {
    let mut args = std::env::args();
    let _ = args.next().unwrap(); // Ignore first arg
    let file_name = args.next().expect("Expected input file");

    let result = entrypoint(file_name);
    println!("{result}");
}

fn entrypoint(file: String) -> String {
    WORKERS_COUNT.get_or_init(|| available_parallelism().unwrap().into());
    // WORKERS_COUNT.get_or_init(|| 4);
    WORKERS_DONE.get_or_init(|| AtomicUsize::new(0));
    WORK_COUNTER.get_or_init(|| AtomicUsize::new(0));

    let cores = WORKERS_COUNT.get().unwrap();

    let (send, recv) = mpsc::channel();
    let reader_state = ReaderState {
        channel: send,
        file: file,
    };
    let joiner_state = JoinerState { channel: recv };

    let mut threads = Vec::with_capacity(*cores);

    for id in 0..*cores - 1 {
        let state = reader_state.clone();

        threads.push(thread::spawn(move || reader(id, state)));
    }

    let joiner_thread = thread::spawn(move || joiner(*cores - 1, joiner_state));

    for handle in threads {
        handle.join().unwrap();
    }

    joiner_thread.join().unwrap()
}

fn reader(id: usize, state: ReaderState) {
    THREAD_ID.with(|once_lock| {
        once_lock.get_or_init(|| id);
    });

    let file = File::open(state.file).unwrap();
    let len = file.metadata().unwrap().len();

    // NOTE: Since we may need to read more than CHUNK_SIZE bytes when the last line of the chunk
    // ends after the chunk itself, we read more data upfront.
    let mut buf = vec![0; CHUNK_SIZE * 2];

    loop {
        let offset = next_chunk_offset();

        // TODO how can we avoid creating a new hashmap everytime?
        // Maybe have two allocations per-thread so that one is with the joiner and one with the
        // reader? The problem then is how can the joiner send it after it's done with it?
        let mut records = ChunkResult(HashMap::with_capacity(10000));
        // We know we're done when the next chunk starts after the end of the file
        if offset > (len - 1) as usize {
            break;
        }

        // TODO this shouldnt work for the last chunk because it errors when it cannot read as many
        // bytes as buf's len
        match file.read_exact_at(&mut buf, offset as u64) {
            Ok(_) => (),
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    // TODO it may be that this doesn't actually read all bytes up until the end of
                    // the file
                    let bytes = file.read_at(&mut buf, offset as u64).unwrap();

                    if offset + bytes != len as usize {
                        panic!("read {} of total {len} bytes", offset + bytes);
                    }
                } else {
                    panic!();
                }
            }
        }
        let offset = start_offset(&mut buf);

        process_chunk(&buf, offset, &mut records);

        if records.0.get("Aasiaat").is_some() {
            THREAD_ID.with(|id| {
                println!(
                    "{}, sending shit, workers done: {}",
                    id.get().unwrap(),
                    WORKERS_DONE.get().unwrap().load(Ordering::Relaxed)
                );
            });
        };
        state.channel.send(records).unwrap();
    }

    let workers_done = WORKERS_DONE.get().unwrap();

    let mut old = workers_done.load(Ordering::Relaxed);
    loop {
        match workers_done.compare_exchange_weak(old, old + 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(x) => old = x,
        }
    }
}

fn process_chunk(buf: &[u8], offset: usize, records: &mut ChunkResult) {
    let mut bytes = offset;
    let buf = &buf[offset..];

    let mut seen = false;

    for line in buf.split(|c| *c == b'\n') {
        if line.is_empty() || bytes > CHUNK_SIZE {
            if seen {
                println!("{records:?}");
            }
            return;
        }
        bytes += line.len();
        let mut fields = line.splitn(2, |c| *c == b';');

        if let (Some(name), Some(temp)) = (fields.next(), fields.next()) {
            let temp: f64 = unsafe { from_utf8_unchecked(temp) }.parse().unwrap();

            let name = unsafe { from_utf8_unchecked(name) };

            // TODO compare String with &[u8] without incurring an allocation
            let entry = if let Some(entry) = records.0.get_mut(name) {
                entry
            } else {
                let name = name.to_string();
                if name == "Aasiaat" {
                    seen = true;
                }
                records.0.entry(name).or_insert(Record::new())
            };
            entry.min = entry.min.min(temp);
            entry.max = entry.max.max(temp);
            entry.acc += temp;
            entry.count += 1;
        } else {
            panic!();
        }
    }
}

/// Returns the actual offset to start reading from
///
/// Since chunks are arbitrarily sized, they may not start at line boundaries. So for every chunk
/// besides the first, the actual offset is the character after the first line break. For this to
/// work, every chunk besides the last ends at the first line break after the chunk's end.
fn start_offset(buf: &Vec<u8>) -> usize {
    buf.iter()
        .position(|c| *c == b'\n')
        .map(|idx| idx + 1)
        .unwrap_or_default()
}

/// Start of the next chunk to be read
///
/// Calculated using WORK_COUNTER and CHUNK_SIZE, tries to increment WORK_COUNTER atomically. If
/// that succeeds, returns WORK_COUNTER * CHUNK_SIZE. Loops until the cmp_exch is successful.
fn next_chunk_offset() -> usize {
    let work_counter = WORK_COUNTER.get().unwrap();
    let mut old = work_counter.load(Ordering::Relaxed);
    // TODO use std::hint::spin_loop()?
    loop {
        // Load current value
        match work_counter.compare_exchange_weak(old, old + 1, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(x) => old = x,
        }
    }

    old * CHUNK_SIZE
}

fn joiner(_id: usize, state: JoinerState) -> String {
    let worker_count = WORKERS_COUNT.get().unwrap();
    let workers_done = WORKERS_DONE.get().unwrap();

    let mut results: BTreeMap<String, Record> = BTreeMap::new();

    while workers_done.load(Ordering::Relaxed) < worker_count - 2 {
        if let Ok(chunk_result) = state.channel.try_recv() {
            if chunk_result.0.get("Aasiaat").is_some() {
                println!("recv shit");
            }
            merge(&mut results, chunk_result.0);
        }
    }

    while let Ok(chunk_result) = state.channel.try_recv() {
        if chunk_result.0.get("Aasiaat").is_some() {
            println!("recv shitty");
        }
        merge(&mut results, chunk_result.0);
    }

    format_results(results)
}

fn merge(results: &mut BTreeMap<String, Record>, chunk: HashMap<String, Record>) {
    chunk.into_iter().for_each(|(name, record)| {
        if let Some(existing_record) = results.get_mut(&name) {
            existing_record.min = existing_record.min.min(record.min);
            existing_record.max = existing_record.max.max(record.max);
            existing_record.acc += record.acc;
            existing_record.count += record.count;
        } else {
            results.insert(name, record);
        };
    });
}

#[derive(Debug)]
struct Record {
    min: f64,
    max: f64,
    acc: f64,
    count: usize,
}

impl Record {
    fn new() -> Self {
        Self {
            min: f64::MAX,
            max: f64::MIN,
            acc: 0.,
            count: 0,
        }
    }
}

fn format_results(stations: BTreeMap<String, Record>) -> String {
    let mut string = String::with_capacity(stations.len() * 10);

    println!("printy: {:?}", stations.get("Aasiaat").unwrap());

    string.push('{');

    string.push_str(
        &stations
            .into_iter()
            .map(|(name, record)| {
                format!(
                    "{}={:.1}/{:.1}/{:.1}, ",
                    name,
                    record.min,
                    record.acc / (record.count as f64),
                    record.max
                )
            })
            .reduce(|mut acc, c| {
                acc.push_str(&c);
                acc
            })
            .unwrap(),
    );
    string.pop(); // Remove ','
    string.pop(); // Remove ' '
    string.push('}');

    string
}

#[cfg(test)]
mod tests {
    use crate::entrypoint;

    #[test]
    fn diff_to_plain() {
        let result = entrypoint("../data/small-measurements.txt".to_string());
        let expected = std::fs::read_to_string("../data/small-result-ref.txt").unwrap();
        assert_eq!(expected, result);
    }
}

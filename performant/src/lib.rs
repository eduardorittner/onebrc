use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::ErrorKind,
    os::unix::fs::FileExt,
    str::from_utf8_unchecked,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, available_parallelism},
};

// TODO: remove panics from the program and see if that makes a difference
// TODO: use target-cpu=native

/// The maximum length of a line. In practice it's a little bit lower than 128, but keeping it as a
/// power of 2 probably won't hurt.
static LINE_SIZE: usize = 128;

#[derive(Debug)]
/// Execution context used by both joiner and readers
pub struct ExecutionCtx {
    // TODO can we use smaller elements (AtomicU8?, U16?)
    work_counter: AtomicUsize,
    readers_done: AtomicUsize,
    workers_count: usize,
}

impl ExecutionCtx {
    pub fn new(workers_count: usize) -> Self {
        Self {
            work_counter: AtomicUsize::new(0),
            readers_done: AtomicUsize::new(0),
            workers_count,
        }
    }
}

// TODO: anyway to remove this owned String?
#[derive(Debug)]
pub struct ChunkResult(pub HashMap<String, Record>);

#[derive(Clone)]
/// Context specific to readers
pub struct ReaderCtx {
    pub channel: mpsc::Sender<ChunkResult>,
    // TODO: use a &str here? Shouldn't matter too much
    pub file: String,
    pub chunk_size: usize,
    pub exec_ctx: *const ExecutionCtx,
}

// SAFETY: context_ptr is guaranteed to be valid for the lifetime of all threads
// since its lifetime is restricted to `entrypoint()`, and all threads finish before
// `entrypoint()`.
unsafe impl Send for ReaderCtx {}

impl ReaderCtx {
    pub fn context(&self) -> &ExecutionCtx {
        // SAFETY: `self.exec_ctx` is only freed after all threads have finished. Therefore it's
        // always safe to deference it from any joiner/reader thread.
        unsafe { &*self.exec_ctx }
    }
}

/// Context specific to the joiner
pub struct JoinerCtx {
    pub channel: mpsc::Receiver<ChunkResult>,
    pub exec_ctx: *const ExecutionCtx,
}

// SAFETY: context_ptr is guaranteed to be valid for the lifetime of all threads
// since its lifetime is restricted to `entrypoint()`, and all threads finish before
// `entrypoint()`.
unsafe impl Send for JoinerCtx {}

impl JoinerCtx {
    // TODO: attribute inline here
    pub fn context(&self) -> &ExecutionCtx {
        unsafe { &*self.exec_ctx }
    }
}

pub fn entrypoint(file: String, chunk_size: usize) -> String {
    let cores = available_parallelism().unwrap().into();
    let context = ExecutionCtx::new(cores);

    let (send, recv) = mpsc::channel();
    let context_ptr = &context as *const ExecutionCtx;

    let reader_state = ReaderCtx {
        channel: send,
        file: file,
        chunk_size,
        exec_ctx: context_ptr,
    };
    let joiner_state = JoinerCtx {
        channel: recv,
        exec_ctx: context_ptr,
    };

    let reader_count = cores - 1;

    let threads: Vec<_> = (0..reader_count)
        .map(|id| {
            let state = reader_state.clone();
            thread::spawn(move || reader(id, state))
        })
        .collect();

    // TODO: do we need to wait for reader threads or can we just wait on joiner?
    let joiner_thread = thread::spawn(move || joiner(reader_count, joiner_state));

    for handle in threads {
        handle.join().unwrap();
    }

    joiner_thread.join().unwrap()
}

fn reader(_id: usize, state: ReaderCtx) {
    let file = File::open(&state.file).unwrap();
    let len = file.metadata().unwrap().len();

    // NOTE: Since we may need to read more than chunk_size bytes when the last line of the chunk
    // ends after the chunk itself, we read more data upfront.
    let mut buf = vec![0; state.chunk_size + LINE_SIZE];

    loop {
        let offset = next_chunk_offset(&state);

        // We know we're done when the next chunk starts after the end of the file
        if offset >= len as usize {
            break;
        }

        // TODO how can we avoid creating a new hashmap everytime?
        // Maybe have two allocations per-thread so that one is with the joiner and one with the
        // reader? The problem then is how can the joiner send it back to the reader after it's
        // done with it?
        let mut records = ChunkResult(HashMap::with_capacity(256));

        let bytes_read = read_bytes(&mut buf, offset, &file, len as usize);

        let offset = if offset != 0 {
            start_offset(&buf)
        } else {
            // We do not look-ahead for the first chunk
            offset
        };

        process_chunk(&state, &buf[..bytes_read], offset, &mut records);

        state.channel.send(records).unwrap();
    }

    atomic_increment(&state.context().readers_done);
}

/// Reads the correct number of bytes from the file to buf
///
/// Tries to read exactly `buf.len()` bytes. If that doesn't work due to encountering an EOF early,
/// then calls `read_at()` repeatedly until EOF is reached.
// TODO: inline
fn read_bytes(mut buf: &mut [u8], offset: usize, file: &File, len: usize) -> usize {
    match file.read_exact_at(&mut buf, offset as u64) {
        Ok(()) => buf.len(),
        Err(e) if matches!(e.kind(), ErrorKind::UnexpectedEof) => {
            let mut bytes = 0;

            // Call `read_at()` in a loop until EOF is reached
            while bytes < buf.len() && offset + bytes < len {
                bytes += file.read_at(&mut buf[bytes..], offset as u64).unwrap();
            }

            debug_assert_eq!(offset + bytes, len);

            bytes
        }
        Err(e) => {
            panic!("error reading file: {e:?}");
        }
    }
}

fn process_chunk(state: &ReaderCtx, buf: &[u8], offset: usize, records: &mut ChunkResult) {
    let mut bytes = offset;
    let buf = &buf[offset..];

    for line in buf.split(|c| *c == b'\n') {
        if line.is_empty() || bytes > state.chunk_size {
            return;
        }
        bytes += line.len() + 1; // Newline is not counted in `line.len()`
        // TODO: parse this manually with SIMD
        let mut fields = line.splitn(2, |c| *c == b';');

        if let (Some(name), Some(temp)) = (fields.next(), fields.next()) {
            let temp = parse_temp(temp);
            let name = unsafe { from_utf8_unchecked(name) };

            let entry = if let Some(entry) = records.0.get_mut(name) {
                entry
            } else {
                let name = name.to_string();
                records.0.entry(name).or_insert(Record::new())
            };
            entry.min = entry.min.min(temp);
            entry.max = entry.max.max(temp);
            entry.acc += temp as isize;
            entry.count += 1;
        } else {
        }
    }
}

// TODO: inline
fn parse_temp(input: &[u8]) -> i16 {
    let to_number = |ascii: u8| (ascii - 48) as i16;
    match input {
        [b'0'..=b'9', b'.', b'0'..=b'9'] => to_number(input[0]) * 10 + to_number(input[2]),
        [b'0'..=b'9', b'0'..=b'9', b'.', b'0'..=b'9'] => {
            to_number(input[0]) * 100 + to_number(input[1]) * 10 + to_number(input[3])
        }
        [b'-', b'0'..=b'9', b'.', b'0'..=b'9'] => -(to_number(input[1]) * 10 + to_number(input[3])),
        [b'-', b'0'..=b'9', b'0'..=b'9', b'.', b'0'..=b'9'] => {
            -(to_number(input[1]) * 100 + to_number(input[2]) * 10 + to_number(input[4]))
        }
        _ => unreachable!(),
    }
}

/// Increments an atomic integer
fn atomic_increment(val: &AtomicUsize) -> usize {
    let mut old = val.load(Ordering::Relaxed);
    loop {
        match val.compare_exchange_weak(old, old + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => old = x,
        }
    }
    old
}

/// Returns the actual offset to start reading from
///
/// Since chunks are arbitrarily sized, they may not start at line boundaries. So for every chunk
/// besides the first, the actual offset is the character after the first line break. For this to
/// work, every chunk besides the last ends at the first line break after the chunk's end.
// TODO: inline
fn start_offset(buf: &[u8]) -> usize {
    buf.iter()
        .take(128)
        .position(|c| *c == b'\n')
        .map(|idx| idx + 1)
        .unwrap_or_default()
}

/// Start of the next chunk to be read
///
/// Calculated using work_counter and chunk_size, tries to increment WORK_COUNTER atomically. If
/// that succeeds, returns work_counter * chunk_size. Loops until the cmp_exch is successful.
// TODO: inline
fn next_chunk_offset(state: &ReaderCtx) -> usize {
    atomic_increment(&state.context().work_counter) * state.chunk_size
}

fn joiner(_id: usize, state: JoinerCtx) -> String {
    let worker_count = state.context().workers_count;
    let readers_done = &state.context().readers_done;
    let reader_count = worker_count - 1;

    let mut results: BTreeMap<String, Record> = BTreeMap::new();

    while readers_done.load(Ordering::Relaxed) < reader_count {
        if let Ok(chunk_result) = state.channel.try_recv() {
            merge(&mut results, chunk_result.0);
        }
    }

    while let Ok(chunk_result) = state.channel.try_recv() {
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
pub struct Record {
    min: i16,
    max: i16,
    acc: isize,
    count: usize,
}

impl Record {
    pub fn new() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            acc: 0,
            count: 0,
        }
    }
}

// TODO: use BufWriter instead of manually pushing strs
fn format_results(stations: BTreeMap<String, Record>) -> String {
    let mut string = String::with_capacity(stations.len() * 10);

    string.push('{');

    string.push_str(
        &stations
            .into_iter()
            .map(|(name, record)| {
                format!(
                    "{}={:.1}/{:.1}/{:.1}, ",
                    name,
                    (record.min as f64) / 10.,
                    (record.acc as f64) / 10. / (record.count as f64),
                    (record.max as f64) / 10.
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
    string.push('\n');

    string
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::entrypoint;

    #[derive(Debug, PartialEq)]
    struct Record {
        min: f64,
        max: f64,
        mean: f64,
    }

    fn parse(input: &str) -> HashMap<String, Record> {
        fn parse_values(input: &str) -> Record {
            let values: Vec<_> = input
                .split("/")
                .map(|input| input.parse().unwrap())
                .collect();

            assert_eq!(3, values.len());

            Record {
                min: values[0],
                mean: values[1],
                max: values[2],
            }
        }

        let input = input
            .strip_prefix("{")
            .unwrap()
            .strip_suffix("}\n")
            .unwrap();

        let map: HashMap<_, _> = input
            .split(",")
            .map(|station| station.split_once("=").unwrap())
            .map(|(name, values)| (name.trim_start().to_string(), parse_values(values)))
            .collect();

        map
    }

    fn compare_results(expected: String, result: String) -> Result<(), String> {
        if expected != result {
            let expected_map = parse(&expected);
            let actual_map = parse(&result);

            for (key, val) in &expected_map {
                if let Some(actual_val) = actual_map.get(key) {
                    if val != actual_val {
                        return Err(format!(
                            "Station '{}' should have values {:?}, had {:?}",
                            key, val, actual_val
                        ));
                    }
                } else {
                    return Err(format!(
                        "Should contain station '{}' with values {:?}, but doesn't",
                        key, val
                    ));
                }
            }

            return Err(format!(
                "Results don't match:\nExpected: {}\nActual: {}",
                expected, result
            ));
        }
        Ok(())
    }

    #[test]
    fn diff_to_plain() {
        let result = entrypoint("../data/small-measurements.txt".to_string(), 0x100);
        let expected = std::fs::read_to_string("../data/small-ref.txt").unwrap();

        compare_results(expected, result).unwrap();
    }

    #[test]
    fn proptest_chunk_sizes() {
        let expected = std::fs::read_to_string("../data/small-ref.txt").unwrap();

        for size in 1..128 {
            let chunk_size = u32::MAX as usize / size;
            let result = entrypoint("../data/small-measurements.txt".to_string(), chunk_size);
            if let Err(error) = compare_results(expected.clone(), result) {
                panic!("Failed with chunk_size {}: {}", chunk_size, error);
            }
        }
    }
}

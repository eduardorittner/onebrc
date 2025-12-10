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

#[derive(Debug)]
/// Execution context used by both joiner and readers
pub struct ExecutionCtx {
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

#[derive(Debug)]
pub struct ChunkResult(pub HashMap<String, Record>);

#[derive(Clone)]
/// Context specific to readers
pub struct ReaderCtx {
    pub channel: mpsc::Sender<ChunkResult>,
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

    let joiner_thread = thread::spawn(move || joiner(reader_count, joiner_state));

    for handle in threads {
        handle.join().unwrap();
    }

    joiner_thread.join().unwrap()
}

fn reader(_id: usize, state: ReaderCtx) {
    let file = File::open(&state.file).unwrap();
    let len = file.metadata().unwrap().len();

    // NOTE: Since we may need to read more than CHUNK_SIZE bytes when the last line of the chunk
    // ends after the chunk itself, we read more data upfront.
    let mut buf = vec![0; state.chunk_size * 2];

    loop {
        let offset = next_chunk_offset(&state);

        // TODO how can we avoid creating a new hashmap everytime?
        // Maybe have two allocations per-thread so that one is with the joiner and one with the
        // reader? The problem then is how can the joiner send it back to the reader after it's
        // done with it?
        let mut records = ChunkResult(HashMap::with_capacity(100));

        // We know we're done when the next chunk starts after the end of the file
        if offset >= len as usize {
            break;
        }

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
        let mut fields = line.splitn(2, |c| *c == b';');

        if let (Some(name), Some(temp)) = (fields.next(), fields.next()) {
            let temp = parse_temp(temp);
            let name = unsafe { from_utf8_unchecked(name) };

            // TODO compare String with &[u8] without incurring an allocation
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

fn parse_temp(input: &[u8]) -> i16 {
    let input = unsafe { from_utf8_unchecked(input) };
    // TODO: optimize parsing, since numbers have a very simple, fixed format, we don't need to
    // call the std `parse()` function and can just parse the values ourselves.
    let (left, right) = input.split_once('.').unwrap();

    let (left, negative) = if left.starts_with('-') {
        (&left[1..], true)
    } else {
        (left, false)
    };

    let left = left.parse::<i16>().unwrap() * 10;
    let right: i16 = right.parse().unwrap();

    let val = if negative {
        -left - right
    } else {
        left + right
    };

    val
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
fn start_offset(buf: &[u8]) -> usize {
    buf.iter()
        .position(|c| *c == b'\n')
        .map(|idx| idx + 1)
        .unwrap_or_default()
}

/// Start of the next chunk to be read
///
/// Calculated using work_counter and chunk_size, tries to increment WORK_COUNTER atomically. If
/// that succeeds, returns work_counter * chunk_size. Loops until the cmp_exch is successful.
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

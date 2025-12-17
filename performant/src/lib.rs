#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufWriter, ErrorKind, Write},
    os::unix::fs::FileExt,
    str::from_utf8_unchecked,
    sync::{
        atomic::{AtomicU8, AtomicU32, Ordering},
        mpsc,
    },
    thread::{self, available_parallelism},
};

// TODO: remove panics from the program and see if that makes a difference
// TODO: use target-cpu=native

/// The maximum length of a line. In practice it's a little bit lower than 128, but keeping it as a
/// power of 2 probably won't hurt.
static LINE_SIZE: usize = 128;

#[derive(Debug, Default)]
/// Execution context used by both joiner and readers
pub struct ExecutionCtx {
    work_counter: AtomicU32,
    readers_done: AtomicU8,
    workers_count: usize,
}

impl ExecutionCtx {
    #[inline]
    pub fn new(workers_count: usize) -> Self {
        debug_assert!(workers_count <= u8::MAX as usize);
        Self {
            workers_count,
            ..Default::default()
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
pub struct JoinerCtx<W: Write> {
    pub channel: mpsc::Receiver<ChunkResult>,
    pub exec_ctx: *const ExecutionCtx,
    pub writer: W,
}

// SAFETY: context_ptr is guaranteed to be valid for the lifetime of all threads
// since its lifetime is restricted to `entrypoint()`, and all threads finish before
// `entrypoint()`.
unsafe impl<W: Write> Send for JoinerCtx<W> {}

impl<W: Write> JoinerCtx<W> {
    #[inline(always)]
    pub fn context(&self) -> &ExecutionCtx {
        unsafe { &*self.exec_ctx }
    }
}

pub fn entrypoint<W: Write>(file: String, chunk_size: usize, writer: W) {
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
        writer,
    };

    let reader_count = cores - 1;

    let threads: Vec<_> = (0..reader_count)
        .map(|id| {
            let state = reader_state.clone();
            thread::spawn(move || reader(id, state))
        })
        .collect();

    // NOTE: By not calling joiner on another thread, this means that any hanging readers
    // (panicking, buggy, etc.) will cause the whole program to never end, since the work will
    // never be done and we will never be able to wait on the reader handles.
    joiner(reader_count, joiner_state);

    for handle in threads {
        handle.join().unwrap();
    }
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

    atomic_u8_increment(&state.context().readers_done);
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

fn process_chunk_scalar(state: &ReaderCtx, buf: &[u8], offset: usize, records: &mut ChunkResult) {
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

pub fn process_chunk(state: &ReaderCtx, buf: &[u8], offset: usize, records: &mut ChunkResult) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // Unsafe block to call the AVX2 version
            return unsafe { process_chunk_avx2(state, buf, offset, records) };
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") {
            // Unsafe block to call the NEON version
            return unsafe { process_chunk_neon(state, buf, offset, records) };
        }
    }

    // Fallback to the scalar implementation
    process_chunk_scalar(state, buf, offset, records)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn process_chunk_avx2(
    state: &ReaderCtx,
    buf: &[u8],
    offset: usize,
    records: &mut ChunkResult,
) {
    let mut bytes = offset;
    let mut line_start = offset;

    let sep_mask = _mm256_set1_epi8(b';' as i8);
    let line_feed_mask = _mm256_set1_epi8(b'\n' as i8);

    while bytes < state.chunk_size {
        let chunk = unsafe { _mm256_loadu_si256(buf.as_ptr().add(bytes) as *const _) };

        let eq_sep = _mm256_cmpeq_epi8(chunk, sep_mask);
        let sep_match_mask = _mm256_movemask_epi8(eq_sep);

        let eq_lf = _mm256_cmpeq_epi8(chunk, line_feed_mask);
        let lf_match_mask = _mm256_movemask_epi8(eq_lf);

        if (sep_match_mask | lf_match_mask) != 0 {
            let line_feed_idx = lf_match_mask.trailing_zeros() as usize;
            let sep_idx = sep_match_mask.trailing_zeros() as usize;

            if line_feed_idx < 32 {
                let line_end = bytes + line_feed_idx;
                let sep_pos = line_start + sep_idx;
                let name = unsafe { from_utf8_unchecked(&buf[line_start..sep_pos]) };
                let temp = parse_temp(&buf[sep_pos + 1..line_end]);
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

                line_start = line_end + 1;
            }

            bytes = line_start;
        } else {
            bytes += 32;
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn process_chunk_neon(
    state: &ReaderCtx,
    buf: &[u8],
    offset: usize,
    records: &mut ChunkResult,
) {
    let mut bytes = offset;
    let mut line_start = offset;

    // 16-byte vector with all lanes ';'
    let sep_mask = vdupq_n_u8(b';');
    // 16-byte vector with all lanes '\n'
    let line_feed_mask = vdupq_n_u8(b'\n');

    while bytes < state.chunk_size {
        // Load 16 bytes from the buffer into a NEON vector
        let chunk = unsafe { vld1q_u8(buf.as_ptr().add(bytes)) };

        // Compare the chunk with the separator mask to find occurrences of ';'
        let eq_sep = vceqq_u8(chunk, sep_mask);
        // Compare the chunk with the newline mask to find occurrences of '\n'
        let eq_lf = vceqq_u8(chunk, line_feed_mask);

        // Combine the separator and newline masks to find occurrences of either character
        let combined_mask = vorrq_u8(eq_sep, eq_lf);
        // Extract the high 64 bits of the combined mask
        let mask_high = vgetq_lane_u64(vreinterpretq_u64_u8(combined_mask), 1);
        // Extract the low 64 bits of the combined mask
        let mask_low = vgetq_lane_u64(vreinterpretq_u64_u8(combined_mask), 0);

        // If either the high or low 64 bits of the mask are not zero, then we have a match
        if (mask_low | mask_high) != 0 {
            // Extract the high 64 bits of the newline mask
            let lf_mask_high = vgetq_lane_u64(vreinterpretq_u64_u8(eq_lf), 1);
            // Extract the low 64 bits of the newline mask
            let lf_mask_low = vgetq_lane_u64(vreinterpretq_u64_u8(eq_lf), 0);
            // Find the index of the first newline character in the 16-byte chunk
            let line_feed_idx = (((lf_mask_high as u128) << 64) | lf_mask_low as u128)
                .trailing_zeros() as usize
                / 8;

            // Extract the high 64 bits of the separator mask
            let sep_mask_high = vgetq_lane_u64(vreinterpretq_u64_u8(eq_sep), 1);
            // Extract the low 64 bits of the separator mask
            let sep_mask_low = vgetq_lane_u64(vreinterpretq_u64_u8(eq_sep), 0);
            // Find the index of the first separator character in the 16-byte chunk
            let sep_idx = (((sep_mask_high as u128) << 64) | sep_mask_low as u128).trailing_zeros()
                as usize
                / 8;

            // If a newline character was found in the chunk
            if line_feed_idx < 16 {
                // Calculate the absolute end of the line
                let line_end = bytes + line_feed_idx;
                // Calculate the absolute position of the separator
                let sep_pos = bytes + sep_idx;
                // Get the name of the station
                let name = &buf[line_start..sep_pos];
                // Parse the temperature
                let temp = parse_temp(&buf[sep_pos + 1..line_end]);
                // Get the record for the station, or create a new one
                let entry = if let Some(entry) = records
                    .0
                    .get_mut(unsafe { std::str::from_utf8_unchecked(name) })
                {
                    entry
                } else {
                    let name = unsafe { std::str::from_utf8_unchecked(name) }.to_string();
                    records.0.entry(name).or_insert(Record::new())
                };
                // Update the record with the new temperature
                entry.min = entry.min.min(temp);
                entry.max = entry.max.max(temp);
                entry.acc += temp as isize;
                entry.count += 1;

                // Update the start of the next line
                line_start = line_end + 1;
            }
        }
        // Advance to the next 16-byte chunk
        bytes += 16;
    }
}

#[inline(always)]
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

#[inline(always)]
/// Increments an atomic u8
fn atomic_u8_increment(val: &AtomicU8) -> u8 {
    let mut old = val.load(Ordering::Relaxed);
    loop {
        match val.compare_exchange_weak(old, old + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => old = x,
        }
    }
    old
}

#[inline(always)]
/// Increments an atomic u32
// NOTE: This ideally would be a generic function, but as of when this code was written there is no
// generic Atomic trait, or generic Atomic<T> types so this will have to do for now.
fn atomic_u32_increment(val: &AtomicU32) -> u32 {
    let mut old = val.load(Ordering::Relaxed);
    loop {
        match val.compare_exchange_weak(old, old + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => old = x,
        }
    }
    old
}

#[inline(always)]
/// Returns the actual offset to start reading from
///
/// Since chunks are arbitrarily sized, they may not start at line boundaries. So for every chunk
/// besides the first, the actual offset is the character after the first line break. For this to
/// work, every chunk besides the last ends at the first line break after the chunk's end.
fn start_offset(buf: &[u8]) -> usize {
    buf.iter()
        .take(128)
        .position(|c| *c == b'\n')
        .map(|idx| idx + 1)
        .unwrap_or_default()
}

#[inline(always)]
/// Start of the next chunk to be read
///
/// Calculated using work_counter and chunk_size, tries to increment WORK_COUNTER atomically. If
/// that succeeds, returns work_counter * chunk_size. Loops until the cmp_exch is successful.
fn next_chunk_offset(state: &ReaderCtx) -> usize {
    atomic_u32_increment(&state.context().work_counter) as usize * state.chunk_size
}

fn joiner<W: Write>(_id: usize, state: JoinerCtx<W>) {
    let worker_count = state.context().workers_count;
    let readers_done = &state.context().readers_done;
    let reader_count = worker_count - 1;

    let mut results: BTreeMap<String, Record> = BTreeMap::new();

    while (readers_done.load(Ordering::Relaxed) as usize) < reader_count {
        if let Ok(chunk_result) = state.channel.try_recv() {
            merge(&mut results, chunk_result.0);
        }
    }

    while let Ok(chunk_result) = state.channel.try_recv() {
        merge(&mut results, chunk_result.0);
    }

    format_results(results, state.writer);
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

#[cfg_attr(test, derive(serde::Serialize))]
#[derive(Debug)]
pub struct Record {
    min: i16,
    max: i16,
    acc: isize,
    count: usize,
}

impl Record {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            min: i16::MAX,
            max: i16::MIN,
            acc: 0,
            count: 0,
        }
    }
}

fn format_results<W: Write>(stations: BTreeMap<String, Record>, writer: W) {
    let mut writer = BufWriter::new(writer);

    let mut stations = stations.into_iter().peekable();

    write!(writer, "{{").unwrap();

    while let Some((name, record)) = stations.next() {
        write!(
            writer,
            "{}={:.1}/{:.1}/{:.1}",
            name,
            (record.min as f64) / 10.,
            (record.acc as f64) / 10. / (record.count as f64),
            (record.max as f64) / 10.
        )
        .unwrap();

        if stations.peek().is_some() {
            write!(writer, ", ").unwrap();
        }
    }
    write!(writer, "}}\n").unwrap();
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
        let mut buf = Vec::with_capacity(10000);
        entrypoint(
            "../data/small-measurements.txt".to_string(),
            0x100,
            &mut buf,
        );
        let expected = std::fs::read_to_string("../data/small-ref.txt").unwrap();
        let result = String::from_utf8(buf).unwrap();

        compare_results(expected, result).unwrap();
    }

    #[test]
    fn proptest_chunk_sizes() {
        let expected = std::fs::read_to_string("../data/small-ref.txt").unwrap();

        for size in 1..128 {
            let chunk_size = u32::MAX as usize / size;
            // TODO: ideally we would reuse this allocation across chunk sizes
            let mut buf = Vec::with_capacity(10000);
            entrypoint(
                "../data/small-measurements.txt".to_string(),
                chunk_size,
                &mut buf,
            );
            let result = String::from_utf8(buf).unwrap();
            if let Err(error) = compare_results(expected.clone(), result) {
                panic!("Failed with chunk_size {}: {}", chunk_size, error);
            }
        }
    }

    mod process_chunk_tests {
        use crate::{ChunkResult, ExecutionCtx, ReaderCtx, Record, process_chunk};
        use once_cell::sync::Lazy;
        use std::collections::HashMap;
        use std::sync::mpsc;

        static FILE_CONTENTS: Lazy<Vec<u8>> =
            Lazy::new(|| std::fs::read("../data/small-small-measurements.txt").unwrap());

        fn setup_reader_ctx() -> (ReaderCtx, ExecutionCtx) {
            let (send, _) = mpsc::channel();
            let exec_ctx = ExecutionCtx::new(1);
            let reader_ctx = ReaderCtx {
                channel: send,
                file: String::new(),
                chunk_size: FILE_CONTENTS.len(), // Process a large "chunk"
                exec_ctx: &exec_ctx as *const ExecutionCtx,
            };
            (reader_ctx, exec_ctx)
        }

        fn sort_records(records: ChunkResult) -> Vec<(String, Record)> {
            let mut sorted_records: Vec<_> = records.0.into_iter().collect();
            sorted_records.sort_by(|a, b| a.0.cmp(&b.0));
            sorted_records
        }

        #[test]
        fn process_chunk_at_start() {
            let (reader_ctx, _exec_ctx) = setup_reader_ctx();
            let mut records = ChunkResult(HashMap::new());

            process_chunk(&reader_ctx, &FILE_CONTENTS, 0, &mut records);

            let sorted_records = sort_records(records);

            insta::assert_debug_snapshot!(sorted_records);
        }

        #[test]
        fn process_chunk_with_offset_5() {
            let (reader_ctx, _exec_ctx) = setup_reader_ctx();

            // Find the 5th newline to get a non-trivial offset
            let offset = FILE_CONTENTS
                .iter()
                .enumerate()
                .filter(|&(_, &c)| c == b'\n')
                .nth(4)
                .unwrap()
                .0
                + 1;

            let mut records = ChunkResult(HashMap::new());
            process_chunk(&reader_ctx, &FILE_CONTENTS, offset, &mut records);

            let sorted_records = sort_records(records);

            insta::assert_debug_snapshot!(sorted_records);
        }

        #[test]
        fn process_chunk_with_offset_10() {
            let (reader_ctx, _exec_ctx) = setup_reader_ctx();

            // Find the 10th newline to get a non-trivial offset
            let offset = FILE_CONTENTS
                .iter()
                .enumerate()
                .filter(|&(_, &c)| c == b'\n')
                .nth(9)
                .unwrap()
                .0
                + 1;

            let mut records = ChunkResult(HashMap::new());
            process_chunk(&reader_ctx, &FILE_CONTENTS, offset, &mut records);

            let sorted_records = sort_records(records);

            insta::assert_debug_snapshot!(sorted_records);
        }
    }
}

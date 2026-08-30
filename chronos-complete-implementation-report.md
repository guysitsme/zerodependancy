# Chronos — Complete Implementation Report

**Project:** Zero-dependency, Gorilla-style time-series database
**Competition:** Zero Dependency 2026 (72-Hour Hackathon, Hackathon Raptors)
**Track:** D — Data & Storage
**Team size:** 3
**Document purpose:** a single reference containing every design decision, data format, algorithm, function signature, test case, timeline slot, and submission requirement for this project. Anyone on the team should be able to work entirely from this document without needing to ask "what does this function do" or "what's the byte layout here."

---

## Table of Contents

1. Executive Summary
2. Glossary
3. System Architecture
4. Chunk 1 — Compression Core (full detail)
5. Chunk 2 — Persistence Layer (full detail)
6. Chunk 3 — Rollups & Query Planning (full detail)
7. Chunk 4 — Demo Interface (full detail)
8. Cross-Cutting Concerns
9. Error Handling Philosophy
10. Testing Strategy (full matrix)
11. Language-Specific Standard Library Reference
12. STDLIB.md Template
13. README.md Template
14. Hour-by-Hour Timeline
15. Risk Register
16. Judging Rubric Mapping
17. Bonus Challenge Execution Plan
18. Submission Checklist
19. Post-Submission / Demo Script

---

## 1. Executive Summary

Chronos is a time-series database built from nothing but each team member's chosen language's standard library. It stores `(timestamp, value)` points per named series, compresses them using the algorithm from Facebook's Gorilla paper (delta-of-delta timestamps + XOR'd float values), persists them durably to disk in rotating chunks with checksums and crash-safe writes, precomputes hourly and daily rollups for fast wide-range queries, and exposes a line-based CLI/TCP protocol so the system can actually be operated — including a live `TAIL` command that streams new writes to connected clients in real time.

The system is split into four independently-testable subsystems ("chunks," not to be confused with data chunks — see Glossary), each owned by one of three team members, with fixed function-signature interfaces agreed before any code is written so all three people can work in parallel from hour zero.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| **Point** | A single `(timestamp, value)` pair for one series |
| **Series** | A named stream of points (e.g. `"engine_temp"`, `"cpu_usage"`) |
| **Data chunk** (lowercase, storage sense) | A file on disk holding a batch of compressed points for one series over a bounded time/count window |
| **Chunk** (capitalized, project sense — "Chunk 1," "Chunk 2") | One of the four subsystems/modules of this codebase — unrelated to "data chunk," unfortunately overlapping terminology carried over from the original planning doc |
| **Rollup** | A precomputed aggregate (avg/min/max/count) over a fixed time window, used to avoid decompressing raw data for wide queries |
| **Tier** | A rollup resolution level — hourly tier, daily tier |
| **Delta-of-delta** | The difference between two consecutive timestamp deltas; exploits regular sampling intervals |
| **Snapshot-read** | A concurrency pattern where readers operate on an immutable view of shared state, updated atomically by a single writer |
| **Rotation** | Closing an active data chunk and starting a new one, triggered by point count or time window |

---

## 3. System Architecture

### 3.1 Component diagram

```
                      ┌────────────────────────────┐
                      │   Chunk 4: Demo Interface   │
                      │   (CLI / TCP server)        │
                      │   WRITE / QUERY / TAIL      │
                      └───────────┬────────────────┘
                                  │
                 write_point()    │    query()
                                  ▼
                      ┌────────────────────────────┐
                      │  Chunk 3: Rollups & Query   │
                      │  Planner                    │
                      │  - hourly rollup builder    │
                      │  - daily rollup builder     │
                      │  - tier-selection planner   │
                      └───────────┬────────────────┘
                                  │
            find_chunks()  read_chunk()  write_point()
                                  ▼
                      ┌────────────────────────────┐
                      │  Chunk 2: Persistence Layer │
                      │  - chunk file I/O           │
                      │  - index (snapshot-read)    │
                      │  - checksums                │
                      │  - crash-safe writes        │
                      │  - concurrency control      │
                      └───────────┬────────────────┘
                                  │
                       encode()   │   decode()
                                  ▼
                      ┌────────────────────────────┐
                      │  Chunk 1: Compression Core  │
                      │  - BitWriter / BitReader    │
                      │  - delta-of-delta timestamps│
                      │  - XOR float compression    │
                      └────────────────────────────┘
```

### 3.2 Write path, step by step

1. A client sends `WRITE engine_temp 1735000000 87.3` to Chunk 4.
2. Chunk 4 parses the line, validates the three fields, calls `chunk3.write_point("engine_temp", 1735000000, 87.3)`.
3. Chunk 3 appends the point to (a) the active in-memory buffer destined for Chunk 2, and (b) the current hourly rollup accumulator for `engine_temp`.
4. Chunk 3 calls `chunk2.write_point(...)`, which appends to Chunk 2's active in-memory buffer for that series.
5. If a rotation condition is met (point count or time window), Chunk 2 calls `chunk1.encode(buffered_points)` to get compressed bytes, writes them via the crash-safe temp-then-rename sequence, computes and stores a checksum, and atomically swaps in a new index snapshot including the new chunk.
6. If the hourly bucket has closed, Chunk 3 finalizes that hour's `RollupRecord` and appends it to the hourly rollup file; if 24 hourly records for a day are now complete, it also finalizes a daily `RollupRecord`.
7. Chunk 4 checks its subscriber list for `engine_temp`; any open `TAIL` connections receive `1735000000,87.3\n` immediately.
8. Chunk 4 responds `OK` to the original `WRITE` command.

### 3.3 Read path, step by step

1. A client sends `QUERY engine_temp 1735000000 1735086400` (a 24-hour range) to Chunk 4.
2. Chunk 4 parses and calls `chunk3.query("engine_temp", 1735000000, 1735086400)`.
3. Chunk 3's planner computes `range_width = 86400` seconds, compares against `DAILY_THRESHOLD` and `HOURLY_THRESHOLD`.
4. If `range_width > DAILY_THRESHOLD` and the daily rollup file fully covers the range, Chunk 3 reads daily `RollupRecord`s directly from disk and returns them.
5. Otherwise, if hourly rollups cover it, it reads those instead.
6. Otherwise, Chunk 3 calls `chunk2.find_chunks("engine_temp", start, end)` to get overlapping chunk IDs, then `chunk2.read_chunk(id)` for each — which internally verifies the checksum and calls `chunk1.decode(bytes)` to get raw points back.
7. Chunk 3 filters/concatenates results into the requested exact range and returns them to Chunk 4.
8. Chunk 4 formats the result as newline-separated text and sends it back to the client.

---

## 4. Chunk 1 — Compression Core (full detail)

**Owner:** Person A. **Depends on:** nothing.

### 4.1 Data structures

```
struct BitWriter {
    buffer: Vec<u8>
    current_byte: u8      // accumulator, bits fill from MSB down
    bit_count: u8          // 0-7, bits currently used in current_byte
}

struct BitReader {
    buffer: &[u8]
    byte_pos: usize
    bit_pos: u8            // 0-7
}
```

### 4.2 BitWriter operations, exact behavior

```
write_bit(b):
    current_byte = (current_byte << 1) | b
    bit_count += 1
    if bit_count == 8:
        buffer.push(current_byte)
        current_byte = 0
        bit_count = 0

write_bits(value, n):
    for i in (n-1) down to 0:
        write_bit((value >> i) & 1)

flush():
    if bit_count > 0:
        current_byte = current_byte << (8 - bit_count)   // pad with zero bits on the right
        buffer.push(current_byte)
    return buffer
```

### 4.3 BitReader operations, exact behavior

```
read_bit():
    byte = buffer[byte_pos]
    bit = (byte >> (7 - bit_pos)) & 1
    bit_pos += 1
    if bit_pos == 8:
        bit_pos = 0
        byte_pos += 1
    return bit

read_bits(n):
    result = 0
    for i in 0..n:
        result = (result << 1) | read_bit()
    return result
```

**Edge case to test explicitly:** reading past the end of `buffer` (malformed/truncated input) must raise a clear error, not read garbage memory or silently return 0.

### 4.4 Timestamp encoding, fully worked example

Suppose timestamps are `[1000, 1060, 1120, 1180, 1500]` (seconds).

- `t0 = 1000` → stored raw (64 bits, or fewer if you bound your epoch — document your choice).
- `delta1 = t1 - t0 = 60` → stored raw (recommend 32 bits signed, generous enough for real gaps).
- `t2 = 1120`: `delta2 = 60`. `dod2 = delta2 - delta1 = 0` → write `0` (1 bit total).
- `t3 = 1180`: `delta3 = 60`. `dod3 = 0` → write `0` (1 bit total).
- `t4 = 1500`: `delta4 = 320`. `dod4 = 320 - 60 = 260`. 260 doesn't fit in 7 bits (max 63) or 9 bits (max 255) but fits in 12 bits (max 2047) → write `1110` then the 12-bit two's-complement representation of 260.

Total bits for 5 timestamps here: 64 (t0) + 32 (delta1) + 1 + 1 + (4 + 12) = 114 bits ≈ 14 bytes, versus 5 × 8 = 40 bytes raw. This is the number you'll want to reproduce in your own worked example for STDLIB.md.

**Sign handling:** delta-of-delta can be negative (sampling jitter, or values arriving slightly early/late). Store negative values using two's complement within the fixed bit width for that prefix tier, and sign-extend on decode — e.g. a 7-bit field represents range `[-64, 63]`.

### 4.5 Value encoding, fully worked example

Suppose two consecutive float64 values are `87.3` and `87.4`.

1. Reinterpret both as raw 64-bit patterns: `bits(87.3) = 0x4055D9999999999A`, `bits(87.4) = 0x4055E66666666666` (values illustrative — compute exactly via your language's bit-reinterpret function, don't hand-compute in production code).
2. `xor = bits(87.3) XOR bits(87.4)` — a 64-bit value with some leading zero bits (both numbers share the same sign and exponent, since they're close), some trailing zero bits, and a "meaningful" middle section where the mantissas differ.
3. Count leading zeros (`lz`) and trailing zeros (`tz`) of `xor`. Meaningful bit count `mbc = 64 - lz - tz`.
4. Encode: `1` (value changed) + `1` (new window, assuming previous window differs) + 5 bits for `lz` + 6 bits for `mbc` + `mbc` bits of the meaningful middle section.
5. On decode: reconstruct `xor` by placing the `mbc` meaningful bits back at position `[tz, tz+mbc)`, zero-padding the rest; `bits(new_value) = bits(prev_value) XOR xor`; reinterpret back to float64.

**Window-reuse optimization (do this, it's most of Gorilla's compression benefit on real data):** if the current XOR's `(lz, tz)` falls *within* the previous non-zero XOR's `(lz, tz)` window, skip storing new `lz`/`mbc` — write just `1` `0` (reuse) + the meaningful bits computed against the *previous* window's boundaries instead of recomputing tighter ones. This is what makes smoothly-varying sensor data compress to near 1–2 bits/value in the common case.

### 4.6 Full function signatures

```
fn encode(points: &[(u64, f64)]) -> Vec<u8>
fn decode(bytes: &[u8]) -> Result<Vec<(u64, f64)>, DecodeError>

enum DecodeError {
    TruncatedInput,
    InvalidHeader,
}
```

### 4.7 Compression benchmark (Innovation deliverable)

```
fn benchmark_compression() -> BenchmarkResult {
    points = generate_synthetic_series(duration_hours=24, interval_seconds=1, base_value=70.0, noise_amplitude=2.0)
    raw_size = points.len() * 16   // 8 bytes timestamp + 8 bytes float, naive
    compressed = encode(points)
    compressed_size = compressed.len()
    ratio = raw_size as f64 / compressed_size as f64
    gzip_size = naive_gzip_if_available(points_as_bytes)   // optional comparison, stdlib gzip is fine to use for THIS comparison only, since it's not part of the actual storage engine
    return BenchmarkResult { raw_size, compressed_size, ratio, gzip_size }
}
```

Print this as a table in README: `Raw: 1,382,400 bytes | Compressed: ~97,000 bytes | Ratio: ~14.2x` (illustrative numbers — your actual synthetic data will differ; report your real measured numbers, not these).

### 4.8 Full test list

1. `decode(encode([]))` returns empty list, no crash.
2. `decode(encode([(t, v)]))` returns exactly `[(t, v)]` for a single point (no delta to compute).
3. Round-trip for a monotonically increasing regular series (all `dod == 0` after the first two points).
4. Round-trip for a series with a large time gap (forces the `1111` 32-bit fallback path).
5. Round-trip for constant values (all XORs are zero).
6. Round-trip for wildly varying values (forces full `lz`/`mbc` re-encoding every point, no window reuse).
7. Round-trip for negative deltas (out-of-order or backward-jumping timestamps, if your spec allows them — otherwise, test that this is explicitly rejected with a clear error).
8. Fuzz: 10,000+ random point sequences of random length (including 0 and 1), assert round-trip equality every time.
9. `BitReader` reading past buffer end raises `TruncatedInput`, doesn't panic/segfault.

---

## 5. Chunk 2 — Persistence Layer (full detail)

**Owner:** Person B. **Depends on:** Chunk 1's `encode`/`decode`.

### 5.1 Directory layout on disk

```
/data
  /engine_temp/
    index.dat
    chunk_0001.dat
    chunk_0002.dat
    chunk_0002.dat.tmp     (transient, only exists mid-write)
  /cpu_usage/
    index.dat
    chunk_0001.dat
```

One subdirectory per series keeps things simple and avoids needing series IDs baked into every filename.

### 5.2 Chunk file format, byte-exact

| Offset | Field | Size | Notes |
|---|---|---|---|
| 0 | Magic bytes | 4 | ASCII `"CHRN"` — lets you detect the wrong file type immediately |
| 4 | Format version | 1 | Start at `1`; bump if you change the format mid-hackathon (you will) |
| 5 | Point count | 4 | Big-endian uint32 |
| 9 | Start timestamp | 8 | Big-endian uint64 |
| 17 | End timestamp | 8 | Big-endian uint64 |
| 25 | Payload length | 4 | Big-endian uint32 — length of the compressed payload that follows |
| 29 | Payload | variable | Chunk 1's `encode()` output |
| 29+len | Checksum | 4 (CRC32) or 32 (SHA-256) | Computed over bytes `[0, 29+len)` — i.e. header + payload, so header corruption is also caught |

**Why checksum the header too, not just the payload:** a corrupted point count or timestamp range would otherwise silently mislead the index and query planner even if the compressed payload itself is intact.

### 5.3 Index file format

Simplest correct approach: index.dat is just a newline-delimited text file (small enough that binary encoding isn't worth the complexity), one line per chunk:

```
chunk_0001.dat,1000,1000,1180,42
chunk_0002.dat,1180,1180,2400,58
```//columns: filename, chunk_id_or_derived, start_time, end_time, point_count

Rewritten in full on every rotation using the same temp-then-rename pattern as data chunks. At startup, if `index.dat` is missing or fails to parse, fall back to scanning every `chunk_*.dat` file's header directly and rebuilding the index — slower, but self-healing.

### 5.4 In-memory representation

```
struct IndexEntry {
    file_name: String,
    start_time: u64,
    end_time: u64,
    point_count: u32,
}

// The critical concurrency primitive: index is behind a single swappable reference
struct Index {
    entries: Arc<Vec<IndexEntry>>   // Rust: Arc for shared immutable access
                                     // Go: use an atomic.Pointer[[]IndexEntry] or equivalent
                                     // Python: use a plain variable reassignment under GIL, or a lock-protected reference swap
}
```

### 5.5 Full write sequence, exact steps

```
fn write_point(series, timestamp, value):
    active_buffer[series].append((timestamp, value))
    if should_rotate(active_buffer[series]):
        rotate(series)

fn should_rotate(buffer) -> bool:
    return buffer.len() >= MAX_POINTS_PER_CHUNK   // e.g. 10,000
        or (buffer.last().timestamp - buffer.first().timestamp) >= MAX_CHUNK_DURATION_SECONDS   // e.g. 7200 (2 hours)

fn rotate(series):
    points = active_buffer[series]
    compressed = chunk1.encode(points)
    header = build_header(points.len(), points.first().timestamp, points.last().timestamp, compressed.len())
    full_bytes = header + compressed
    checksum = compute_checksum(full_bytes)
    file_bytes = full_bytes + checksum

    chunk_id = next_chunk_id(series)
    final_path = f"/data/{series}/chunk_{chunk_id:04}.dat"
    tmp_path = final_path + ".tmp"

    write_all_bytes(tmp_path, file_bytes)
    fsync(tmp_path)
    rename(tmp_path, final_path)          // atomic

    new_entry = IndexEntry { final_path, points.first().timestamp, points.last().timestamp, points.len() }
    new_index_entries = current_index_snapshot(series).clone_and_append(new_entry)   // build fresh list
    write_index_file_atomically(series, new_index_entries)                          // temp-then-rename, same pattern
    atomic_swap(index_ref[series], new_index_entries)                               // publish new snapshot

    active_buffer[series].clear()
```

### 5.6 Full read sequence, exact steps

```
fn find_chunks(series, start_time, end_time) -> [chunk_id]:
    snapshot = index_ref[series]     // grab current snapshot, readers never block writer
    return [entry.file_name for entry in snapshot.entries
            if entry.end_time >= start_time and entry.start_time <= end_time]

fn read_chunk(chunk_id) -> [(timestamp, value)]:
    bytes = read_all_bytes(chunk_id)
    header = parse_header(bytes[0:29])
    payload = bytes[29 : 29+header.payload_length]
    stored_checksum = bytes[29+header.payload_length:]
    computed_checksum = compute_checksum(bytes[0 : 29+header.payload_length])
    if stored_checksum != computed_checksum:
        raise ChecksumMismatchError(chunk_id)
    return chunk1.decode(payload)
```

### 5.7 Concurrency — detailed design

**Model:** single-writer, multiple-reader, snapshot-read on the index; data chunk files are immutable once renamed into place, so there is nothing to lock for reading a chunk's bytes.

**Why not a global lock (documented alternative, rejected):** a single mutex around all reads and writes is the simplest possible approach and would technically satisfy "no data races," but it serializes readers against each other and against the writer even though reads of *already-written, immutable* chunk files need no coordination at all. For a demo emphasizing live `TAIL` queries running alongside writes, unnecessary read blocking would visibly hurt the demo's responsiveness.

**Why snapshot-read is safe here specifically:** the only mutable shared state a reader touches is the index. By replacing the *entire* index reference atomically rather than mutating it in place, a reader that grabbed a reference before an update either sees the fully-old index or, on its next lookup, the fully-new one — never a half-updated one. Combined with chunk files being written to a temp path and atomically renamed (so a reader can never open a "final" filename that isn't fully written), the reader never needs to coordinate with the writer at all.

**What this model does NOT protect against (document as a known limitation, don't pretend otherwise):** a single writer means write throughput doesn't scale with multiple cores — deliberately out of scope, since the target use case (one car / one host emitting telemetry) doesn't need multi-writer throughput. State this explicitly rather than letting a judge assume it's an oversight.

### 5.8 Full test list

1. Rotation triggers correctly at both the point-count threshold and the time-window threshold, independently.
2. `find_chunks` returns exactly the chunks overlapping a given range, including partial overlaps at both ends.
3. Checksum mismatch on a deliberately corrupted byte is detected and raises `ChecksumMismatchError`, not silently returning bad data.
4. Kill process (simulate via forced exit) after writing to `.tmp` but before rename → on restart, the previous chunk file is untouched and complete; the `.tmp` file is ignored or cleaned up.
5. Kill process after rename but before index update → on restart, rebuild-index-from-headers fallback correctly picks up the orphaned chunk.
6. Concurrent test: N reader threads calling `find_chunks`/`read_chunk` in a tight loop while writer thread continuously writes and rotates for e.g. 10 seconds; assert zero checksum errors, zero exceptions, all writer-acknowledged points eventually appear in a subsequent read.
7. Index file corruption (truncate `index.dat` mid-line) → startup falls back to header-scan rebuild without crashing.

---

## 6. Chunk 3 — Rollups & Query Planning (full detail)

**Owner:** Person C. **Depends on:** Chunk 2's `find_chunks`/`read_chunk`.

### 6.1 Rollup record format

```
struct RollupRecord {
    window_start: u64,   // start of the hour or day, in the same epoch as raw timestamps
    avg: f64,
    min: f64,
    max: f64,
    count: u32,
}
```

Stored as flat fixed-width binary records (no compression needed — rollup row counts are tiny relative to raw points): e.g. one file `hourly_rollup.dat` per series, records appended in order, each exactly `8+8+8+8+4 = 36` bytes, making random access by index trivial if ever needed.

### 6.2 Hourly accumulator (in-memory, per series, updated on every write)

```
struct HourlyAccumulator {
    window_start: u64,
    sum: f64,
    min: f64,
    max: f64,
    count: u32,
}

fn on_write_point(series, timestamp, value):
    acc = hourly_accumulators[series]
    bucket_start = (timestamp / 3600) * 3600
    if acc.window_start != bucket_start:
        if acc.count > 0:
            finalize_and_append_hourly_record(series, acc)
            maybe_finalize_daily_record(series, acc.window_start)
        acc = HourlyAccumulator { window_start: bucket_start, sum: 0, min: +inf, max: -inf, count: 0 }
    acc.sum += value
    acc.min = min(acc.min, value)
    acc.max = max(acc.max, value)
    acc.count += 1
    hourly_accumulators[series] = acc
```

**Important edge case:** late-arriving points for an already-closed hour bucket (clock skew, retransmission). Decide and document one of: (a) reject with an error, (b) reopen and re-finalize the affected hourly/daily record, (c) silently drop. Recommendation for hackathon scope: **(a) reject with a clear error**, documented as a known limitation — reopening finalized rollups adds real complexity for a case unlikely to come up in your demo data.

### 6.3 Daily record derivation (from hourly records, not raw points)

```
fn maybe_finalize_daily_record(series, hour_window_start):
    day_start = (hour_window_start / 86400) * 86400
    if hour_window_start == day_start + 23*3600:   // just closed the 24th hour of this day
        hourly_records = read_hourly_records_for_day(series, day_start)
        if hourly_records.len() == 24:
            total_count = sum(r.count for r in hourly_records)
            weighted_avg = sum(r.avg * r.count for r in hourly_records) / total_count
            day_min = min(r.min for r in hourly_records)
            day_max = max(r.max for r in hourly_records)
            append_daily_record(series, RollupRecord { day_start, weighted_avg, day_min, day_max, total_count })
```

### 6.4 Query planner, full logic with thresholds justified

```
const HOURLY_THRESHOLD_SECONDS = 3600 * 3    // ranges under 3 hours: raw data is cheap enough, and precision matters more
const DAILY_THRESHOLD_SECONDS  = 86400 * 2   // ranges under 2 days: hourly resolution is a reasonable default

fn query(series, start_time, end_time) -> QueryResult:
    range_width = end_time - start_time
    if range_width >= DAILY_THRESHOLD_SECONDS and daily_rollup_fully_covers(series, start_time, end_time):
        return QueryResult::Rollup(read_daily_records(series, start_time, end_time))
    if range_width >= HOURLY_THRESHOLD_SECONDS and hourly_rollup_fully_covers(series, start_time, end_time):
        return QueryResult::Rollup(read_hourly_records(series, start_time, end_time))
    chunk_ids = chunk2.find_chunks(series, start_time, end_time)
    all_points = []
    for id in chunk_ids:
        all_points.extend(chunk2.read_chunk(id))
    filtered = [p for p in all_points if start_time <= p.timestamp <= end_time]
    return QueryResult::Raw(filtered)
```

**Document the threshold reasoning explicitly in STDLIB.md** — this is your primary evidence for the Innovation criterion: "a query narrower than 3 hours returns raw points because precision matters more than speed at that scale; a query spanning multiple days returns daily rollups because the volume of raw data would otherwise dominate response time for no perceptible query benefit."

### 6.5 Full test list

1. Hourly accumulator correctly finalizes on an hour boundary crossing.
2. Daily record's weighted average matches direct aggregation of all raw points for that day (cross-check against compounding rounding error from re-aggregating rollups-of-rollups).
3. Planner selects raw path for a narrow range, hourly path for a medium range, daily path for a wide range — test at values just below and just above each threshold.
4. Planner falls back to raw/hourly correctly when the wider tier doesn't fully cover the requested range (e.g. query spans a boundary where daily rollup exists for day 1 but day 2 hasn't finished yet).
5. Late-arriving point for a closed hour is rejected with a clear, documented error (per the decision in 6.2).

---

## 7. Chunk 4 — Demo Interface (full detail)

**Owner:** Person C, built after Chunk 3 stabilizes.

### 7.1 Protocol specification, exact grammar

```
COMMAND     := WRITE_CMD | QUERY_CMD | TAIL_CMD
WRITE_CMD   := "WRITE" SP SERIES SP TIMESTAMP SP VALUE
QUERY_CMD   := "QUERY" SP SERIES SP TIMESTAMP SP TIMESTAMP
TAIL_CMD    := "TAIL" SP SERIES
SERIES      := [a-zA-Z0-9_]+
TIMESTAMP   := [0-9]+
VALUE       := -?[0-9]+(\.[0-9]+)?
SP          := single space
```

### 7.2 Responses, exact format

```
WRITE success:  "OK\n"
WRITE failure:  "ERR <reason>\n"                         e.g. "ERR invalid value: not a number"
QUERY success:  "<timestamp>,<value>\n" per line, then "END\n"
                (or, for rollup results: "<window_start>,<avg>,<min>,<max>,<count>\n" per line, then "END\n")
QUERY failure:  "ERR <reason>\n"
TAIL:           no immediate response; connection stays open;
                each new point for that series is pushed as "<timestamp>,<value>\n" as it happens;
                client closes the connection to stop tailing
```

### 7.3 Server structure (TCP variant)

```
fn run_server(port):
    listener = tcp_listen(port)
    loop:
        conn = listener.accept()
        spawn_thread(handle_connection, conn)

fn handle_connection(conn):
    loop:
        line = conn.read_line()
        if line is None: break   // client disconnected
        match parse_command(line):
            Write(series, ts, val) -> {
                result = chunk3.write_point(series, ts, val)
                conn.write(result.is_ok() ? "OK\n" : format("ERR {}\n", result.error))
            }
            Query(series, start, end) -> {
                result = chunk3.query(series, start, end)
                for row in format_rows(result):
                    conn.write(row + "\n")
                conn.write("END\n")
            }
            Tail(series) -> {
                register_subscriber(series, conn)
                // this connection now only receives pushed data until it disconnects;
                // stop reading further commands on it
                block_until_disconnect(conn)
                unregister_subscriber(series, conn)
            }
            Invalid(reason) -> conn.write(format("ERR {}\n", reason))
```

### 7.4 TAIL push mechanism

```
subscribers: map[series] -> list of connection handles   // protected by its own lock, separate from Chunk 2's index concurrency

fn register_subscriber(series, conn):
    lock(subscribers_lock)
    subscribers[series].append(conn)
    unlock(subscribers_lock)

fn notify_subscribers(series, timestamp, value):   // called by Chunk 3 right after a successful write_point
    lock(subscribers_lock)
    for conn in subscribers[series]:
        try: conn.write(f"{timestamp},{value}\n")
        except: mark_for_removal(conn)
    remove_marked(subscribers[series])
    unlock(subscribers_lock)
```

### 7.5 CLI one-shot mode (for scripting / Track-A-style clean exit codes, optional but cheap)

```
./chronos write engine_temp 1735000000 87.3   -> exit code 0, prints nothing (or "OK" to stdout)
./chronos query engine_temp 1735000000 1735003600 -> prints rows to stdout, exit code 0
./chronos query bad_series 0 0                -> prints error to stderr, exit code 1
```

### 7.6 Full test list

1. Well-formed `WRITE`/`QUERY`/`TAIL` commands behave exactly per the grammar and response format above.
2. Malformed commands (missing fields, non-numeric timestamp/value, unknown command word) return `ERR <reason>` without crashing the connection or the server.
3. A `TAIL` subscriber connected before a write receives exactly that write, in the exact `timestamp,value` format.
4. A `TAIL` subscriber that disconnects mid-stream is cleanly removed from the subscriber list on the next notify attempt (no crash, no memory leak in a long-running demo).
5. Multiple simultaneous `TAIL` subscribers on the same series all receive the same writes.
6. CLI one-shot mode returns correct exit codes for success and failure cases.

---

## 8. Cross-Cutting Concerns

### 8.1 Interfaces frozen at Day 0 (do not change without re-syncing with the whole team)

```
Chunk 1 → Chunk 2:
    encode(points: [(u64, f64)]) -> Vec<u8>
    decode(bytes: &[u8]) -> Result<Vec<(u64, f64)>, DecodeError>

Chunk 2 → Chunk 3:
    write_point(series: String, timestamp: u64, value: f64) -> Result<(), WriteError>
    find_chunks(series: String, start: u64, end: u64) -> Vec<ChunkId>
    read_chunk(chunk_id: ChunkId) -> Result<Vec<(u64, f64)>, ReadError>

Chunk 3 → Chunk 4:
    write_point(series: String, timestamp: u64, value: f64) -> Result<(), WriteError>   // pass-through + rollup update
    query(series: String, start: u64, end: u64) -> QueryResult
```

### 8.2 Determinism requirements (Reproducible Build bonus)

- No iteration over unordered hash maps/sets in any path that produces output bytes (chunk headers, index files, STDLIB.md/README generation scripts if any are auto-generated).
- No embedded build timestamps unless explicitly stripped or fixed for reproducibility testing.
- No reliance on floating-point operations whose result depends on CPU-specific extended precision — stick to standard IEEE-754 double operations available uniformly in the stdlib.
- Verification step: build twice from a clean checkout (`git clean -fdx && build`), compute SHA-256 of both resulting binaries, confirm they match, publish both hashes in the README.

### 8.3 Configuration constants (centralize these, don't scatter magic numbers)

```
MAX_POINTS_PER_CHUNK = 10_000
MAX_CHUNK_DURATION_SECONDS = 7200        // 2 hours
HOURLY_THRESHOLD_SECONDS = 10_800        // 3 hours
DAILY_THRESHOLD_SECONDS = 172_800        // 2 days
CHECKSUM_ALGORITHM = "CRC32"             // or SHA-256 if you want the stronger STDLIB.md story
```

---

## 9. Error Handling Philosophy

- **Never silently return wrong data.** A checksum mismatch, a truncated file, or a malformed command must surface as an explicit error, not as zeroed/default values.
- **Distinguish recoverable from fatal.** A single corrupted chunk file should not crash the whole server — log it, return an error for queries touching that chunk, keep serving everything else.
- **Every error type gets a test.** If you defined `DecodeError::TruncatedInput`, there must be a test that triggers exactly that error.
- **User-facing errors (Chunk 4) are human-readable**, not raw internal error codes — `"ERR series not found"` not `"ERR 0x4"`.

---

## 10. Testing Strategy (full matrix)

| Test type | Scope | Owner | When |
|---|---|---|---|
| Unit — compression round-trip | Chunk 1 | Person A | Day 1 |
| Unit — bit-packing edge cases | Chunk 1 | Person A | Day 1 |
| Fuzz — compression | Chunk 1 | Person A | Day 1–2 |
| Unit — chunk file read/write | Chunk 2 | Person B | Day 1 |
| Unit — index rebuild from headers | Chunk 2 | Person B | Day 1–2 |
| Crash simulation — mid-write kill | Chunk 2 | Person B | Day 2 |
| Concurrency — readers vs. writer | Chunk 2 | Person B | Day 1 (against mock), Day 2 (real) |
| Unit — hourly/daily aggregation math | Chunk 3 | Person C | Day 1 |
| Unit — query planner tier selection | Chunk 3 | Person C | Day 2 |
| Integration — Chunk 1 → Chunk 2 | Cross | A + B | Day 1 evening |
| Integration — Chunk 2 → Chunk 3 | Cross | B + C | Day 2 |
| Protocol — malformed command handling | Chunk 4 | Person C | Day 2–3 |
| Integration — TAIL push correctness | Chunk 4 + Chunk 3 | Person C | Day 2–3 |
| End-to-end — full write/query/tail flow | Whole system | Whole team | Day 3 |
| Full-stack crash + concurrency | Whole system | Whole team | Day 3 |
| Reproducible build verification | Build process | Person B | Day 3 |

---

## 11. Language-Specific Standard Library Reference

Pick one language for the whole team (mixing languages across chunks defeats the "single command build" requirement unless you're prepared to orchestrate a multi-language build — not recommended for 72 hours).

| Need | Go | Rust | Python |
|---|---|---|---|
| Float bit reinterpret | `math.Float64bits` / `Float64frombits` | `f64::to_bits` / `from_bits` | `struct.pack('>d', x)` / `unpack` |
| Bitwise ops | native `&`, `\|`, `^`, `<<`, `>>` | native | native, but watch Python's arbitrary-precision int quirks with negative shifts |
| Binary file I/O | `encoding/binary`, `os` | `std::io`, `byteorder` is a crate (avoid; do it manually) | `struct`, built-in `open(..., 'rb')` |
| Checksums | `hash/crc32`, `crypto/sha256` | `std::hash` (basic) or hand-rolled CRC32 (recommended for STDLIB-craft points) | `zlib.crc32` (stdlib), `hashlib.sha256` |
| Atomic rename | `os.Rename` | `std::fs::rename` | `os.rename` |
| Sockets | `net` | `std::net` | `socket` |
| Threading | goroutines + `sync` | `std::thread` + `std::sync` (`Arc`, `Mutex`, `RwLock`) | `threading` (note: GIL affects true parallelism, but I/O-bound concurrency still works fine here) |
| Atomic pointer swap for snapshot-read | `atomic.Pointer[T]` (Go 1.19+) | `Arc<T>` swapped via `ArcSwap`-style pattern (hand-rolled: `Mutex<Arc<T>>` is simplest and stdlib-only) | reassigning a module-level variable is atomic enough under the GIL for this pattern |

**Recommendation given the team's background (Python/Java/JavaScript/Swift comfort, embedded systems experience):** Go is likely the best fit for a 72-hour zero-dependency systems project — simpler concurrency primitives than Rust, no GIL caveat like Python, and a very complete stdlib for exactly this kind of task (binary I/O, sockets, hashing all first-class).

---

## 12. STDLIB.md Template

```markdown
# Standard Library Substitutions — Chronos

| # | Normally you'd use | Instead, we used | Where |
|---|---|---|---|
| 1 | InfluxDB / Prometheus (entire storage engine) | Hand-rolled Gorilla-style time-series engine | Whole project |
| 2 | `zlib`/`gzip` | Hand-rolled delta-of-delta + XOR bit-packed compression | Chunk 1 |
| 3 | Protobuf/msgpack | Hand-rolled fixed-layout binary chunk format | Chunk 2 |
| 4 | An ORM / SQLite | Hand-rolled chunk files + flat index | Chunk 2 |
| 5 | A crypto/hash package | [Go: hash/crc32] for checksums | Chunk 2 |
| 6 | A CLI framework (cobra/click) | Hand-rolled line parser | Chunk 4 |
| 7 | An HTTP client/server framework | Raw TCP sockets, hand-rolled line protocol | Chunk 4 |
| 8 | A concurrency/actor library | [Go: goroutines + atomic.Pointer] snapshot-read pattern | Chunk 2 |
| 9 | A metrics/monitoring client library | The project itself | Whole project |
| 10 | A logging framework | [language]'s built-in logging | Whole project |

## Concurrency Threat Model
[Describe single-writer/snapshot-read here, the rejected alternative (global lock), and why.]

## Crash-Safety Design
[Describe temp-then-rename, checksum verification, and what a corrupted chunk causes downstream.]

## Compression Benchmark
[Insert your actual measured numbers from section 4.7 here.]

## Query Planner Design
[Insert your threshold reasoning from section 6.4 here.]
```

---

## 13. README.md Template

```markdown
# Chronos — Zero-Dependency Time-Series Database

## Problem
[1-2 sentences, from Executive Summary above.]

## Architecture
[Paste the component diagram from section 3.1.]

## Build & Run
```
[your single build command]
[your run command]
```

## Usage
```
WRITE engine_temp 1735000000 87.3
QUERY engine_temp 1735000000 1735003600
TAIL engine_temp
```

## Benchmarks
[Compression ratio table from section 4.7.]

## Zero-Dependency Proof
[Show your empty dependency manifest, e.g. Cargo.toml [dependencies] section, or go.mod with no require block.]

## Reproducible Build
[Two SHA-256 hashes, matching.]

## Team
[Names + which chunk each person owned.]
```

---

## 14. Hour-by-Hour Timeline

| Hours | Person A | Person B | Person C |
|---|---|---|---|
| Pre-kickoff | Read Gorilla paper §4.1–4.2, sketch BitWriter on paper | Sketch chunk file format + index + concurrency model on paper | Sketch rollup record format + protocol grammar on paper |
| 0–2 | Team sync: freeze all interface signatures (section 8.1), agree on language | | |
| 2–8 | Implement `BitWriter`/`BitReader`, unit tests | Implement chunk file header read/write against a mock encode/decode | Implement hourly accumulator + basic aggregation math against mock points |
| 8–14 | Implement delta-of-delta encode/decode, test against worked example (4.4) | Implement rotation logic + index structure | Implement daily record derivation from mock hourly records |
| 14–20 | Implement XOR float encode/decode, test against worked example (4.5) | Implement crash-safe write (temp+rename) | Implement query planner skeleton (tier thresholds, fallback logic) |
| 20–24 (end of Day 1) | **Freeze real `encode`/`decode`, hand off to Person B** | Start swapping mock for Person A's real encode/decode | Continue against mock; start sketching Chunk 4 protocol handler |
| 24–30 | Add compression benchmark (4.7) | Finish real integration; implement checksum verification | Finish query planner against mock; begin Chunk 4 command parser |
| 30–36 | Help debug integration issues surfaced by B | Implement concurrency model (snapshot-read); write concurrent-access test | Wire Chunk 4 `WRITE`/`QUERY` against Chunk 3 |
| 36–42 | Free to assist wherever needed | Own crash-safety test (kill-mid-write simulation) | Add daily tier to planner; implement `TAIL` subscriber mechanism |
| 42–48 (end of Day 2) | Start drafting STDLIB.md compression section | Finish crash-safety test; begin reproducible-build verification | Finish `TAIL`; run first full end-to-end manual test with the whole team |
| 48–54 | Continue STDLIB.md; help write worked examples | Full-stack concurrency + crash test against the complete system | Fix bugs found in end-to-end test |
| 54–60 | Finalize benchmark numbers for README | Finalize STDLIB.md persistence/concurrency sections | Write automated end-to-end test suite |
| 60–66 | Assist with demo video script | Verify reproducible build (two builds, matching hashes) | Record demo video (show `WRITE`, `QUERY`, and especially live `TAIL`) |
| 66–72 | Final review pass | Final review pass | Finalize README, submission checklist, publish repo |

---

## 15. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Bit-packing bug in Chunk 1 discovered late | Medium | High | Mandatory round-trip tests from hour one, worked numeric examples checked by hand |
| Integration cascade — bug in A's code breaks B and C simultaneously | Medium | High | Interface freeze at end of Day 1 (hour 24), not Day 2; mocks used until then so B and C aren't blocked |
| Concurrency bug (torn read/race) undetected until Day 3 | Medium | High | Concurrency test written and run against mocks by Day 1, not deferred |
| Scope creep on daily rollup tier or TAIL feature | Low–Medium | Medium | Both are explicitly scoped as small additions on top of existing logic (section 6.3, 7.4) — if time runs short, cut the daily tier before cutting core crash-safety or the hourly tier |
| Reproducible build fails due to unnoticed nondeterminism | Medium | Low–Medium (bonus only) | Verify early (Day 2), not only at the final hour, so there's time to find and fix the source |
| Team member's language of choice lacks a needed stdlib primitive | Low | High if it happens | Confirm language choice and verify all primitives in section 11 exist in that language during Day 0, before writing real code |
| Demo video/README rushed at the very end | Medium | Medium | Explicitly scheduled hours (60–72) rather than left implicit |

---

## 16. Judging Rubric Mapping

| Criterion | Weight | Primary evidence in this project |
|---|---|---|
| Functionality & Usefulness | 35% | Working `WRITE`/`QUERY`/`TAIL` over a real CLI/TCP interface; live-streaming demo |
| Zero-Dependency Craft | 30% | 10-line STDLIB.md substitution table (section 12); hand-rolled compression, storage, and networking |
| Code Quality & Idiom | 25% | Four cleanly separated modules with frozen interfaces (section 8.1), each independently tested |
| Innovation | 10% | Multi-tier rollup planner with documented thresholds (section 6.4); live `TAIL` streaming; compression benchmark with real numbers |

---

## 17. Bonus Challenge Execution Plan

| Bonus | Points | Concrete action |
|---|---|---|
| Package Killer | +3 | State explicitly in README: "Normally: InfluxDB/Prometheus → Instead: Chronos" |
| STDLIB Log | +3 | Ship the 10-row table from section 12, verified accurate against actual code, not aspirational |
| Reproducible Build | +5 | Two clean builds, matching SHA-256 hashes, both published in README (section 8.2 has the determinism checklist) |
| Single File | Not targeted | Deliberately skipped — four-module architecture scores better under Code Quality; state this trade-off explicitly in README so it reads as a decision, not an oversight |

---

## 18. Submission Checklist

- [ ] Public GitHub repository, public at submission time
- [ ] Single documented build command
- [ ] Empty dependency manifest verified (`go.mod` no `require` block / `Cargo.toml` empty `[dependencies]` / no non-stdlib imports anywhere)
- [ ] README.md complete (section 13 template)
- [ ] STDLIB.md complete with 10+ real substitutions (section 12 template)
- [ ] Full test suite passing: unit, integration, concurrency, crash-safety, end-to-end (section 10)
- [ ] 5-minute demo video recorded, shows `WRITE`, `QUERY`, and live `TAIL`
- [ ] Reproducible build hashes published and verified matching
- [ ] Track confirmed as D — Data & Storage in submission form
- [ ] All code committed within the 72-hour window only (verify git log timestamps against kickoff time)

---

## 19. Post-Submission / Demo Script

A suggested 5-minute demo video structure:

1. **0:00–0:30** — State the problem (time-series data is usually stored inefficiently) and the constraint (zero dependencies).
2. **0:30–1:30** — Show the architecture diagram, briefly walk through the four chunks and who owns what.
3. **1:30–2:30** — Live terminal demo: `WRITE` a few points, `QUERY` them back, show the compression benchmark numbers.
4. **2:30–3:30** — Live terminal demo: open a `TAIL` connection in one window, `WRITE` points in another, show them streaming in real time.
5. **3:30–4:15** — Show the crash-safety test running (kill the process mid-write, restart, show recovery) and the concurrency test passing.
6. **4:15–5:00** — Show STDLIB.md briefly, mention the bonus challenges claimed, close with the reproducible build hashes matching.

# Chronos — Operator Dashboard

> **Repo:** `guysitsme/zerodependancy`  
> **Branch:** `frontend-ui/ux`  
> **Track:** D — Data & Storage | Zero Dependency 2026 Hackathon  
> **Stack:** Go (backend) · Vanilla HTML/CSS/JS (frontend) — zero external dependencies

---

## Repository layout

```
/
├── README.md                  ← you are here
│
├── index.html                 ← Dashboard UI — main page
├── style.css                  ← Monochrome design system
├── app.js                     ← All UI interactivity + demo simulation
│
└── backend/                   ← Chronos database server (Go)
    ├── go.mod                 ← module chronos, go 1.21
    ├── main.go                ← Entry point — starts TCP + WebSocket servers
    ├── config/
    │   └── config.go          ← All tuning constants (ports, thresholds, chunk sizes)
    ├── compression/           ← CHUNK 1: Gorilla compression
    │   ├── compression.go     ← BitWriter/Reader, delta-of-delta, XOR floats, Benchmark
    │   └── compression_test.go← 9 spec tests + 10 000-iteration fuzz test
    ├── persistence/           ← CHUNK 2: Crash-safe disk storage
    │   └── store.go           ← Chunk files, CRC32, atomic index, crash recovery
    ├── rollup/                ← CHUNK 3: Rollup engine + query planner
    │   └── engine.go          ← Hourly/daily accumulators, tier-selection query planner
    └── server/                ← CHUNK 4: Network interface
        ├── server.go          ← TCP line protocol (WRITE / QUERY / TAIL / BENCHMARK)
        ├── websocket.go       ← stdlib-only RFC 6455 WebSocket handshake
        └── benchmark.go       ← Thin shim to expose compression.Benchmark() via server
```

---

## Quick start — Frontend (UI)

No build step required. Open in a browser:

```bash
# Option A — open directly
start index.html            # Windows
open index.html             # macOS

# Option B — serve over HTTP
python -m http.server 8080
# visit http://localhost:8080
```

The UI ships with a **demo simulation mode** in `app.js`. All panels (Write, Query, Tail, Benchmark) work immediately without a backend.

---

## Quick start — Backend (Go server)

```bash
cd backend

# Run all tests first
go test ./...

# Start the server (defaults: TCP :9000, WebSocket :9001)
go run main.go

# Or specify a custom data directory
go run main.go -data /var/chronos/data
```

The server exposes:
- **TCP port 9000** — raw line-protocol server (for CLI / telemetry ingest)
- **WebSocket port 9001** — same protocol over WS (for the browser UI)

---

## Protocol reference

### Commands (client → server)

| Command | Format | Example |
|---|---|---|
| `WRITE` | `WRITE <series> <unix_ts> <value>\n` | `WRITE engine_temp 1735000000 87.3` |
| `QUERY` | `QUERY <series> <start_ts> <end_ts>\n` | `QUERY engine_temp 1735000000 1735086400` |
| `TAIL`  | `TAIL <series>\n` | `TAIL engine_temp` |
| `BENCHMARK` | `BENCHMARK\n` | `BENCHMARK` |

### Responses (server → client)

| Situation | Response |
|---|---|
| Write success | `OK\n` |
| Write failure | `ERR <reason>\n` |
| Query — raw points | `<ts>,<value>\n` × N, then `END\n` |
| Query — hourly rollup | `<window_start>,<avg>,<min>,<max>,<count>\n` × N, then `END\n` |
| Query — daily rollup | same format as hourly |
| Query failure | `ERR <reason>\n` |
| Tail push | connection stays open; each new point as `<ts>,<value>\n` |
| Benchmark | `<raw_bytes>,<compressed_bytes>,<ratio>\n` |

---

## Connecting the UI to the backend

Open [`app.js`](./app.js) and look for the `connectBtn` handler (≈ line 120). Replace the demo simulation with a real WebSocket:

```js
// Replace this block:
connectBtn.addEventListener('click', () => {
  // ... demo timeout ...
});

// With this:
let ws = null;
connectBtn.addEventListener('click', () => {
  const host = serverHostInput.value.trim() || 'localhost:9001';
  if (state.connected) { ws?.close(); setConnectionState(false); return; }
  connectBtn.textContent = 'Connecting…'; connectBtn.disabled = true;
  ws = new WebSocket(`ws://${host}/ws`);
  ws.onopen  = () => { connectBtn.disabled = false; setConnectionState(true); };
  ws.onerror = () => { connectBtn.disabled = false; showError('Connection failed.'); };
  ws.onclose = () => setConnectionState(false);
  ws.onmessage = (e) => handleServerMessage(e.data);
});
```

See [README (UI section)](#ui-panels--features) below for full wiring instructions.

---

## Backend developer — audit guide

This section is for the backend developer who wants to verify the implementation,
run tests, and push changes back to this branch.

### 1. Clone and set up

```bash
git clone https://github.com/guysitsme/zerodependancy.git
cd zerodependancy
git checkout frontend-ui/ux
cd backend
go test ./...    # all tests should pass
```

### 2. What to audit — file by file

#### [`config/config.go`](./backend/config/config.go)
All system-wide constants. **Review and adjust:**
- `MaxPointsPerChunk` (default 10 000) — rotation frequency
- `MaxChunkDurationSecs` (default 7 200s = 2h) — time-based rotation
- `HourlyThresholdSecs` (3h) — minimum range to use hourly rollups instead of raw
- `DailyThresholdSecs` (2d) — minimum range to use daily rollups
- `TCPPort` / `WSPort` — network ports

#### [`compression/compression.go`](./backend/compression/compression.go)
Gorilla-style compression. Key things to verify:
- `encodeXOR` / `decodeXOR` — XOR float encoding with window reuse
- `encodeDoD` / `decodeDoD` — delta-of-delta timestamp encoding
- The `mbc-1` wire encoding (values 1..64 stored as 0..63 in 6 bits) — critical correctness invariant
- `Benchmark()` — run it and log the ratio; tune the benchmark data if needed

Run the full test suite including fuzz:
```bash
go test ./compression/... -v -count=1
# Expected: 9/9 PASS, fuzz runs 10 000 iterations
```

#### [`persistence/store.go`](./backend/persistence/store.go)
Crash-safe storage. Verify:
- `rotate()` — temp-then-rename write sequence and CRC32 checksum
- `rebuildIndex()` / `scanChunkHeaders()` — startup recovery logic
- `FindChunks()` — snapshot-read (no locks on read path)
- Chunk file format header (magic `CHRN`, 1-byte version, big-endian fields)

**Crash test:** kill the process mid-write and restart — the `.tmp` file must be ignored and the previous chunk must be intact.

#### [`rollup/engine.go`](./backend/rollup/engine.go)
Hourly/daily rollup accumulators + query planner. Verify:
- `updateAccumulator()` — hour-boundary detection and finalization
- `maybeFinalizeDailyRecord()` — only fires when all 24 hourly records for a day are present
- `Query()` — tier selection order: daily → hourly → raw
- Rollup binary format: fixed 36 bytes per record (8+8+8+8+4, big-endian)

#### [`server/server.go`](./backend/server/server.go)
TCP handler. Verify:
- `handleWrite` — validates series name (`[a-zA-Z0-9_]+`), parses ts and value
- `handleQuery` — calls engine, formats raw vs rollup responses differently
- `handleTail` — subscriber broker, goroutine leak check (broker cleans dead conns)
- `handleBenchmark` — returns `raw_bytes,compressed_bytes,ratio\n`

#### [`server/websocket.go`](./backend/server/websocket.go)
RFC 6455 WebSocket handshake — stdlib only. Verify:
- The `Sec-WebSocket-Accept` computation (SHA-1 + base64)
- `wsConn` satisfies `net.Conn` — compile-time check: `var _ net.Conn = (*wsConn)(nil)`
- `Access-Control-Allow-Origin: *` is set in the 101 response (needed for browser)

### 3. Running the server manually

```bash
cd backend
go run main.go
```

Then test with `nc` (netcat):
```bash
# Write a point
echo "WRITE engine_temp 1735000000 87.3" | nc localhost 9000

# Query back
echo -e "QUERY engine_temp 1730000000 1740000000\n" | nc localhost 9000

# Run benchmark
echo "BENCHMARK" | nc localhost 9000
```

Or connect the browser UI to `localhost:9001` (WebSocket port) by entering `localhost:9001` in the server host field and clicking Connect.

### 4. Data directory layout

```
data/
├── engine_temp/
│   ├── index.dat          ← newline-delimited chunk metadata (rebuilt on startup if missing)
│   ├── chunk_0001.dat     ← CHRN header + compressed payload + CRC32
│   ├── chunk_0002.dat
│   ├── hourly_rollup.dat  ← fixed-width 36-byte records
│   └── daily_rollup.dat
└── cpu_usage/
    └── ...
```

### 5. Known limitations (documented, not oversights)

| Limitation | Reason |
|---|---|
| Single writer per series | Write throughput doesn't need to scale for single-host telemetry |
| No WAL / journal | Crash safety is handled by temp-then-rename + index rebuild on startup |
| WebSocket framing is pass-through | The `wsConn` adapter passes raw bytes; does not implement full WS frame parsing — works for text frames only |
| No authentication | Scope is a local hackathon demo |

### 6. How to make changes and push back

```bash
# You're already on the right branch:
git status  # should say: On branch frontend-ui/ux

# Edit files, then:
git add -p                              # stage changes interactively
git commit -m "fix(compression): ..."   # use conventional commits
git push origin frontend-ui/ux         # push to same branch
```

**Branch rules:**
- Work on `frontend-ui/ux` — do not push to `main` directly
- Keep zero external dependencies — no `go get`, no npm installs
- Run `go vet ./...` and `go test ./...` before pushing
- If you change the wire format of compression or chunk files, bump `ChunkFormatVersion` in `config/config.go`

---

## UI panels & features

| Panel | What it does |
|---|---|
| **Sidebar** | Global series selector, server host input (`host:port`), section nav, Connect/Disconnect with live status |
| **Write** | Sends `WRITE <series> <ts> <value>` — shows `OK` / `ERR` inline |
| **Query** | Sends `QUERY <series> <start> <end>` — renders canvas chart + results table. Auto-badges tier: `RAW` / `HOURLY ROLLUP` / `DAILY ROLLUP` |
| **Tail** | Live push terminal — streams new points as they arrive |
| **Benchmark** | Sends `BENCHMARK` — displays raw size, compressed size, ratio |

---

## Design system

| Token | Value |
|---|---|
| Primary font | Hanken Grotesk |
| Mono font | JetBrains Mono |
| Background | `#F7F7F7` |
| Surface | `#FFFFFF` |
| Border | `#EBEBEB` |
| Dark | `#2B2B2B` |
| Terminal bg | `#2B2B2B` |

All tokens are CSS custom properties in `style.css` — edit `:root` to retheme.

---

## Team

| Role | Files |
|---|---|
| Frontend | `index.html`, `style.css`, `app.js` |
| Backend — Chunk 1 | `backend/compression/` |
| Backend — Chunk 2 | `backend/persistence/` |
| Backend — Chunk 3 | `backend/rollup/` |
| Backend — Chunk 4 | `backend/server/`, `backend/main.go` |
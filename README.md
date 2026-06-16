# Telemetry Client

Rust application that connects to the UDP multicast telemetry feed, classifies each record, and writes parsed output as JSON lines - one file per record type.

## Requirements

- Joins multicast group `224.0.0.1` on port `8904`
- Parses `key=value, key=value` datagrams
- Classifies records into `gps`, `door`, `vehicle`, or `passenger`
- Appends JSON lines to `gps.jsonl`, `door.jsonl`, `vehicle.jsonl`, `passenger.jsonl`

## Build

```bash
cargo build --release
```

## Run

1. Start the simulator (from the repo https://github.com/rust-fun-test/telemetry-sim):


Use the prebuilt binary:

```bash
git clone https://github.com/rust-fun-test/telemetry-sim -b rust-for-fun/telemetry-sim
./rust-for-fun/telemetry-sim/macos-arm64/telemetry-sim
```

2. In another terminal, start the client:

```bash
cargo run --release -- --output-dir ./out
```

3. Let both run for a few seconds, then inspect the output:

```bash
head ./out/gps.jsonl
head ./out/door.jsonl
head ./out/vehicle.jsonl
head ./out/passenger.jsonl
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | `./out` | Directory for `.jsonl` output files |
| `--multicast-addr` | `224.0.0.1` | Multicast group address |
| `--port` | `8904` | UDP port |

## Logging

Uses `tracing`. Set log level with `RUST_LOG`:

```bash
RUST_LOG=info cargo run --release
RUST_LOG=telemetry_client=debug cargo run --release
```

---

## Findings and design notes

This section covers what showed up when running the client against the live feed, and why the code is structured the way it is.

### The feed is noisier than the README suggests

On a short test run (~10 seconds), the client received hundreds of datagrams but only wrote valid JSON for a subset of them. The rest were skipped. Looking at the logs, the noise fell into a few patterns:

- **Typos in keys** - e.g. `dor=open` instead of `door=open`, `latitude` instead of `latitude`, `odomete` instead of `odometer`. These don't match any known record type and get rejected at classification time.
- **Typos in values** - e.g. `door=clse` or `door=opn`. The key is right but the value isn't `open` or `close`, so the record is rejected during typed parsing.
- **Concatenation glitches** - pairs run together when the `, ` separator is missing, e.g. `longitude=2.0speed=3.0` or `passengers_in=3passengers_out=1`. Sometimes this produces a segment with no `=` at all; other times it produces a value that contains stray text. Either way, the record doesn't classify cleanly.

Roughly 10-15% of datagrams were skipped in testing. The valid ones landed in the right `.jsonl` files with correct JSON structure. The client never crashed on a bad line - it logs a warning with the raw input and moves on.

### How records are classified

Classification is strict: each record type is identified by its **exact** set of keys, nothing more and nothing less.

| Type | Required keys |
|------|---------------|
| gps | `latitude`, `longitude`, `speed`, `altitude` |
| door | `door` |
| vehicle | `odometer`, `fuel_consumption`, `turn_direction` |
| passenger | `passengers_in`, `passengers_out`, `total_load` |

A single typo in any key means the record won't match. That's intentional - it's better to skip a bad line than write garbage JSON. Numeric fields are parsed strictly (`f64` for floats, `u32` for counts). Enum-like fields (`door`, `turn_direction`) are checked against the allowed values.

### Concurrency model

The client is split into separate Tokio tasks connected by channels:

```
UDP socket  ->  receiver  ->  classifier  ->  writer (x4)
                 mpsc          mpsc
```

- **Receiver** - blocks on `recv_from`, forwards raw datagram strings to the classifier. Uses `tokio::select!` to also watch for a shutdown signal.
- **Classifier** - parses, classifies, serializes to JSON, and routes each line to the correct writer channel. Parse failures are handled here without affecting the rest of the pipeline.
- **Writers (x4)** - one task per record type, each owning a single output file. They only receive pre-serialized JSON strings.

`main` spawns all tasks, keeps their `JoinHandle`s, and uses `tokio::select!` to wait for either Ctrl-C or a periodic stats tick. On shutdown it signals all tasks via a `watch` channel, drops the classifier's sender ends (so writers drain and exit), then joins every handle.

This separation keeps UDP I/O, CPU-bound parsing, and disk I/O from blocking each other. At ~100 messages/second the channel buffer (1024) is more than enough headroom.

### Why channels instead of shared state

Each stage communicates through `tokio::sync::mpsc` channels rather than shared mutable state. The receiver doesn't know about file paths. The writers don't know about UDP or parsing. The classifier is the only place that understands record types.

On shutdown, dropping the classifier's sender handles closes the writer channels cleanly - each writer drains whatever is left in its queue, flushes, and exits.

### Atomic append writes

Each writer opens its `.jsonl` file in append mode (`create + append`). Every record is written as a single `write_all` call - the JSON string plus a newline - before moving on to the next. On Linux/macOS, append-mode writes below `PIPE_BUF` (~4 KB on most systems) are atomic at the OS level, so even if something else were writing to the same file, individual lines wouldn't get interleaved.

Our JSON lines are well under that limit. Writers also flush on a 1-second interval and once more on shutdown, so a crash mid-run might lose buffered data but won't corrupt existing lines.

We don't use a temp-file-rename pattern here because the requirement is append-only JSONL, not full-file replacement. Append + single-write-per-line is the right tradeoff for this workload.

### Error handling

Errors are split into layers:

| Layer | What goes wrong | What happens |
|-------|-----------------|--------------|
| Parse | Bad format, missing `=`, empty fields | `warn`, skip record |
| Classify | Unknown key set (typos, partial records) | `warn`, skip record |
| Validate | Bad numeric value, invalid enum | `warn`, skip record |
| Serialize | `serde_json` failure (shouldn't happen on valid records) | `error`, skip record |
| I/O | File open/write/flush failure | `error`, increment io_errors counter |

Nothing propagates up to crash `main` during normal operation. The pipeline stats (logged every 5 seconds) show received / parsed / skipped / serialized counts so you can see the skip rate at a glance.

### Logging

`tracing` with `RUST_LOG` filtering. Startup logs the multicast address, port, and output directory. Each writer logs when it opens its file and how many lines it wrote on shutdown. Skipped records include the reason and the raw line, which is useful when trying to understand what the feed is actually sending vs what you expect.

Example skip log:

```
WARN unknown record type keys=dor raw=dor=open
WARN parse failed reason=invalid door value 'clse' (expected open or close) raw=door=clse
```

### Sample output

After a few seconds of running, the output files look like:

**gps.jsonl**
```json
{"latitude":87.7,"longitude":-12.8,"speed":43.6,"altitude":3799.9}
```

**door.jsonl**
```json
{"door":"close"}
```

**vehicle.jsonl**
```json
{"odometer":5164.0,"fuel_consumption":10.4,"turn_direction":"left"}
```

**passenger.jsonl**
```json
{"passengers_in":0,"passengers_out":2,"total_load":8}
```

Every line in the output files is valid JSON. The noise stays in the logs, not on disk.

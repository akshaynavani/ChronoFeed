# ChronoFeed — Implementation Plan

**Deadline-Guaranteed Sensor Ingestion over MQTT**

Tech: C++17, POSIX threads, MQTT 3.1.1, Linux (PREEMPT_RT friendly), NTP/SNTP.

## Goal

A multi-threaded C++ pipeline that ingests sensor readings published over MQTT, timestamps
them against an NTP-disciplined clock, and processes each sample within a hard end-to-end
deadline (default 5 ms from socket receipt to sink). Every deadline miss is detected,
counted, and reported. Threads are pinned to dedicated cores and (when permitted) run
under `SCHED_FIFO` with locked memory to keep latency deterministic.

## Key design decisions

- **No heavyweight dependencies.** A minimal MQTT 3.1.1 client (CONNECT / SUBSCRIBE /
  PUBLISH QoS 0 / PINGREQ) is implemented directly over POSIX sockets, so the hot path
  has no hidden allocations or locks from a third-party library.
- **Lock-free SPSC ring buffers** between stages; pre-allocated, fixed-size sample structs,
  no heap allocation on the hot path.
- **Stage-per-core pipeline:** `ingest` (MQTT socket reader) → `process` (decode,
  validate, filter, window stats) → `sink` (CSV/stdout writer). Each stage on its own
  pinned core.
- **Clock model:** `CLOCK_MONOTONIC` for deadline math; wall time derived from monotonic
  plus an offset/frequency correction maintained by an SNTP client with a PI discipline
  loop (no `settimeofday`, no root needed).
- **Deadline monitor:** each sample carries its receive timestamp; every stage checks
  remaining budget, and a watchdog thread reports per-stage latency histograms and miss
  counts.
- **Graceful degradation:** if RT scheduling or affinity is not permitted, log a warning
  and continue with best-effort scheduling.
- **RTOS portability:** OS-specific calls (affinity, priority, memory locking) are
  isolated in `rt/` so a port to an RTOS (e.g. a POSIX-compliant one like QNX/NuttX)
  only touches that layer.

## Phases

### Phase 1 — Core foundation
Deliverables: CMake build, config loading, time utilities, sample types, SPSC ring buffer,
logging.
Files: `CMakeLists.txt`, `include/chronofeed/{time.hpp,sample.hpp,spsc_ring.hpp,config.hpp,log.hpp}`,
`src/config.cpp`, `src/log.cpp`, `config/chronofeed.conf`.

### Phase 2 — MQTT ingestion
Deliverables: MQTT 3.1.1 packet encoder/decoder, socket client with reconnect and
keep-alive, sensor payload parser (compact `id,seq,ts_ns,value` text and JSON-lite).
Files: `include/chronofeed/mqtt/{packet.hpp,client.hpp}`, `src/mqtt/{packet.cpp,client.cpp}`,
`include/chronofeed/payload.hpp`, `src/payload.cpp`.

### Phase 3 — Real-time pipeline & deadline enforcement
Deliverables: RT thread helper (affinity, SCHED_FIFO, mlockall, stack prefault),
ingest/process/sink stages, deadline monitor with latency histogram, pipeline
orchestration and `chronofeed` main binary with signal handling.
Files: `include/chronofeed/rt/thread.hpp`, `src/rt/thread.cpp`,
`include/chronofeed/{histogram.hpp,deadline.hpp,pipeline.hpp,stages.hpp}`,
`src/{deadline.cpp,pipeline.cpp,stages.cpp,main.cpp}`.

### Phase 4 — NTP clock discipline
Deliverables: SNTP client (RFC 4330), offset/delay computation, PI-loop clock discipline
with outlier rejection, background sync thread, disciplined wall clock used to stamp
samples and measure publisher→ingest transit.
Files: `include/chronofeed/ntp/{sntp.hpp,discipline.hpp}`, `src/ntp/{sntp.cpp,discipline.cpp}`,
wiring in `src/pipeline.cpp`.

### Phase 5 — Tooling and tests
Deliverables: load-generating MQTT publisher (`chronofeed_pub`), Google Test suite
(ring buffer, MQTT packets, payload parsing, histogram, discipline loop, deadline monitor),
`.gitignore`, README with architecture diagram.
Files: `tools/publisher.cpp`, `tests/*.cpp`, `.gitignore`, `README.md`, `.env.example`.

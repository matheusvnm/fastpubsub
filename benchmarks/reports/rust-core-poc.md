# Rust core PoC: results, findings and reproduction

This report covers the proof of concept for a Rust transport core in FastPubSub. It gives the results, the problems that we found, how we fixed them and how to reproduce them.

Branch: `poc/rust-core`. All numbers are medians of 5 rounds unless the text says otherwise.

## 1. Summary

- On real Pub/Sub in the same region, the Rust transport passes all six go and no-go thresholds against the optimized Python transport.
- The Rust transport uses 76% to 77% less CPU for each event in the I/O scenarios, and 32% less in the CPU-bound scenario.
- It uses 37% to 40% less peak memory.
- With the fix for server flow control, it has the same p99 latency in echo. In the scenarios with a 100 ms wait, its p99 latency is 30% to 45% lower.
- Echo still has 8% fewer events per second than the optimized Python transport.
- The first official run on the emulator gave a no-go. Two defects in the PoC code caused most of the gap. The first was a race in the publisher cache. The second was a server flow-control limit that was too low. We fixed both.
- Phase 0, the Python-only work, gives 35% to 38% less CPU than `main` without a new dependency. Phase 0 is valid without Rust.
- Verdict: make the Rust transport the default. Keep the Python transport as a fallback until the gates in section 13 pass.

## 2. Versions under test

| Label | Code |
|---|---|
| `python-current` | `main` at `3fc5449`, through a worktree in `.worktrees/bench-main` |
| `python-optimized` | This branch with `FASTPUBSUB_TRANSPORT=python` |
| `rust` | This branch with `FASTPUBSUB_TRANSPORT=rust` |
| `google-nonblocking` | The Google Python client alone, as a reference |

The transport is selected once, when `fastpubsub/transport.py` is imported. With `rust`, no Google Python module is imported.

## 3. Scenarios and thresholds

| Scenario | Messages in flight | Handler work |
|---|---|---|
| `echo` | 100 | Receive, then publish again. No other work. |
| `wait-only-100ms` | 1,000 | A 100 ms asynchronous wait. |
| `cpu-and-wait-100ms` | 1,000 | 1 ms of pure Python CPU work, then a 100 ms wait. |

We set the thresholds before the first official run. Each threshold compares `rust` with `python-optimized`.

| # | Threshold |
|---|---|
| 1 | Echo CPU per event is at least 40% lower. |
| 2 | Wait-only CPU per event is at least 25% lower. |
| 3 | p99 latency is at most 10% higher in each scenario. |
| 4 | cpu-and-wait events per second are not lower. |
| 5 | Peak memory is at most 20% higher. |
| 6 | The test suite passes with the Rust transport. |

## 4. Benchmark method

- Each run starts a new worker process. The worker creates a new topic and a new subscription, and deletes them at the end.
- A run has a 10 s warm-up and a 30 s measurement.
- A round runs each version once. The version order rotates in each round, so no version always runs first or last.
- The worker measures its own CPU time between the end of the warm-up and the end of the run.
- The CPU work is a pure Python integer loop. `hashlib` does not scale on free-threaded Python, so the loop does not use it.
- The CPU iterations are calibrated once and fixed for all versions: 17,470 in Docker on the Mac, 12,165 on the Google Cloud VM.
- No profiler runs during an official run.

We measured the tools that could add cost, with 3 interleaved rounds of echo in Docker:

| Condition | Rust µs/event | Python µs/event |
|---|---|---|
| Plain worker, log level DEBUG (the official setup) | 100.4 | 164.4 |
| Log level WARNING | 100.1 | 166.6 |
| Timeline sampler attached | 100.0 | 165.8 |

All differences are inside the normal spread of about ±1.5%. py-spy and memray slow the worker, so we used them only to find where time and memory go.

## 5. Results on the emulator (Docker, 2 CPUs for each container)

The Docker VM is linux/arm64 with 4 CPUs. Each container has a limit of 2 CPUs.

### 5.1 Phase 0 and the first Rust core

First official run, `benchmarks/results/official.csv`:

| Scenario | Metric | python-current | python-optimized | rust |
|---|---|---|---|---|
| echo | CPU µs/event | 270.4 | 166.6 | 109.8 |
| echo | Peak RSS MB | 100.0 | 93.1 | 67.4 |
| wait-only-100ms | CPU µs/event | 282.7 | 183.3 | 124.5 |
| wait-only-100ms | Peak RSS MB | 102.8 | 96.4 | 128.5 |
| cpu-and-wait-100ms | Events per second | 808.6 | 919.2 | 997.2 |
| cpu-and-wait-100ms | p99 ms | 1,258 | 993 | 662 |
| cpu-and-wait-100ms | Peak RSS MB | 116.1 | 100.7 | 128.4 |

Result: no-go. Echo CPU was -34.1% (threshold -40%). Peak memory was +27.5% to +33.3% with 1,000 messages in flight.

### 5.2 After the publisher cache fix

`benchmarks/results/official-fixed.csv`. Only the Rust memory changed:

| Scenario | Rust peak RSS MB before | Rust peak RSS MB after | python-optimized |
|---|---|---|---|
| echo | 67.4 | 60.0 | 94.6 |
| wait-only-100ms | 128.5 | 61.3 | 96.9 |
| cpu-and-wait-100ms | 128.4 | 64.0 | 99.7 |

Five of six thresholds passed. Echo CPU stayed at -34.2%.

### 5.3 After the bottleneck changes

`benchmarks/results/official-bottlenecks.csv`:

| Scenario | Metric | python-current | python-optimized | rust | Rust against optimized |
|---|---|---|---|---|---|
| echo | CPU µs/event | 270.3 | 167.2 | 100.7 | -39.8% |
| echo | Peak RSS MB | 100.2 | 93.1 | 59.7 | -35.9% |
| wait-only-100ms | CPU µs/event | 281.6 | 181.8 | 117.5 | -35.4% |
| wait-only-100ms | p99 ms | 528.9 | 527.1 | 570.5 | +8.2% |
| cpu-and-wait-100ms | Events per second | 813.7 | 915.8 | 1,010.7 | +10.4% |
| cpu-and-wait-100ms | p99 ms | 1,235 | 1,005 | 573 | -42.9% |

Echo CPU missed the threshold by 0.4 µs. The spread between rounds was ±1.1 µs.

### 5.4 Python versions and free threading (Mac, not official)

These runs used the Mac host, not Docker, so they only show a direction.

- Rust lowers the async CPU by 28% to 35%.
- Free threading gives no gain for async handlers.
- On Python 3.15t, sync CPU-bound handlers double their events per second (1,785).
- On Python 3.14t, sync handlers collapse: 186 events per second and 36 ms CPU for each event. We did not investigate this.
- Sync handlers that wait are limited by the 40-thread pool.
- Batch sizes of 16, 64 and 256 for the Rust dispatch give no difference. The default stays 256.

## 6. Results on real Pub/Sub

### 6.1 From the Mac over the internet

One round, Mac to Pub/Sub, about 100 ms network round trip:

| Scenario | Transport | Events per second | p50 ms | p99 ms |
|---|---|---|---|---|
| echo | rust | 819 | 78 | 444 |
| echo | python | 86 | 1,122 | 1,575 |
| wait-only-100ms | rust | 833 | 900 | 3,910 |
| wait-only-100ms | python | 113 | 8,448 | 11,339 |

The network round trip dominates this result. Section 7.3 explains the Python delay.

### 6.2 In the same region, before the flow-control fix

Google Compute Engine `n2-standard-4` in `us-central1-a`, Debian 12, Python 3.12, Rust 1.94.0. The benchmark is pinned to 2 CPUs with `taskset`. The VM uses its own service account. `benchmarks/results/official-gcp.csv`:

| Scenario | Metric | python-current | python-optimized | rust | Rust against optimized |
|---|---|---|---|---|---|
| echo | Events per second | 1,081 | 1,130 | 654 | -42.1% |
| echo | CPU µs/event | 582.8 | 349.5 | 91.2 | -73.9% |
| echo | p99 ms | 398 | 439 | 1,280 | +191.5% |
| wait-only-100ms | CPU µs/event | 641.7 | 425.2 | 99.0 | -76.7% |
| cpu-and-wait-100ms | p99 ms | 3,254 | 3,419 | 3,783 | +10.7% |

Threshold 3 failed. Section 7.4 explains the cause.

### 6.3 In the same region, final result

Same setup. `benchmarks/results/official-gcp-flow.csv`, 60 runs, no redeliveries:

| Scenario | Metric | python-current | python-optimized | rust | Rust against optimized |
|---|---|---|---|---|---|
| echo | Events per second | 1,125 | 1,090 | 1,001 | -8.2% |
| echo | CPU µs/event | 522.0 | 323.5 | 78.3 | -75.8% |
| echo | p50 ms | 43.6 | 33.9 | 17.7 | -47.6% |
| echo | p99 ms | 425 | 468 | 486 | +3.9% |
| echo | Peak RSS MB | 115.0 | 104.1 | 62.5 | -40.0% |
| wait-only-100ms | Events per second | 978 | 1,104 | 1,602 | +45.2% |
| wait-only-100ms | CPU µs/event | 601.1 | 382.8 | 87.0 | -77.3% |
| wait-only-100ms | p99 ms | 3,283 | 3,344 | 1,842 | -44.9% |
| wait-only-100ms | Peak RSS MB | 118.1 | 106.8 | 67.3 | -37.0% |
| cpu-and-wait-100ms | Events per second | 561 | 589 | 866 | +47.0% |
| cpu-and-wait-100ms | CPU µs/event | 1,806 | 1,579 | 1,068 | -32.4% |
| cpu-and-wait-100ms | p99 ms | 3,633 | 3,501 | 2,441 | -30.3% |
| cpu-and-wait-100ms | Peak RSS MB | 114.2 | 106.8 | 67.2 | -37.1% |

| # | Threshold | Result |
|---|---|---|
| 1 | Echo CPU at least -40% | Pass, -75.8% |
| 2 | Wait-only CPU at least -25% | Pass, -77.3% |
| 3 | p99 at most +10% | Pass, +3.9%, -44.9%, -30.3% |
| 4 | cpu-and-wait events per second not lower | Pass, +47.0% |
| 5 | Peak memory at most +20% | Pass, -37.0% to -40.0% |
| 6 | Tests pass with Rust | Pass, 471 of 472 (see section 9) |

The CPU gain is almost twice as large as on the emulator. Real TLS, authentication and network handling cost the Python stack more than the Rust stack.

## 7. Findings

### 7.1 Race in the Rust publisher cache (memory)

- **Symptom:** Rust peak memory grew with the number of messages in flight, about 68 KB for each extra message. The Python transport stayed near 95 MB.
- **When:** the timeline sampler showed that the peak came in the setup phase, at 0.8 s to 1.0 s, before the warm-up.
- **Where:** memray with native stacks read the heap at its peak. It showed 39 MB under `tonic MakeSendRequestService::call`, with 966 calls, one for each HTTP/2 connection. It also showed 31 MB under `google_cloud_gax_internal::grpc::Client::make_inner` and 8 MB under `Publisher::new`.
- **Cause:** `PubSubClientFactory.get_publisher` awaited `asyncio.to_thread(Publisher, ...)` before it wrote the cache. The benchmark sends its first 1,000 publishes at the same time. Each one missed the cache and created a publisher, a gRPC channel, an HTTP/2 connection and an OS thread. The Python factory uses an `asyncio.Lock`, so it does not have this race.
- **Rejected causes:** the GIL wait on tokio threads and glibc arenas. An A/B run with Python calls on separate threads gave the same memory. `MALLOC_ARENA_MAX=2` saved only about 5 MB.
- **Fix:** the factory caches the creation task, and concurrent callers await the same task. A second cache returns a ready publisher without an await.
- **Effect:** wait-only peak memory went from 128.5 MB to 61.3 MB. Before the fix, macOS malloc kept 208 MB and glibc kept 123 MB. The size of the problem depends on the environment, but the cause is in the code.

### 7.2 Bottlenecks in the Rust path (CPU)

We profiled Rust echo in Docker. It used about 64 µs per event on Python threads, 43 µs on tokio threads and 6 µs on the bridge threads. We changed five things:

1. The publish middleware keeps one `PubSubClient`, and `get_publisher` has a cache path without an await. Rust `Publisher.publish` sends to one forwarding task (`FuturesUnordered`) instead of one tokio task for each message.
2. One `Bridge` thread for each event loop carries both received messages and publish results, with one `call_soon_threadsafe` for each batch.
3. `Publisher.publish` checks exact built-in types first and uses strict `validate_call` only for other values.
4. We tested 1 tokio worker against 2. One worker saved 4 to 6 µs per event but gave one p99 outlier of 192 ms, so we kept 2.
5. Received messages start as eager tasks (`asyncio.eager_task_factory`).

Effect: Rust echo went from 110.3 to 100.7 µs per event. The gain came from the Python threads (61 to 51 µs). The tokio threads stayed at about 43 µs. Our estimate before the change was 20 to 35 µs, and the real gain was about 10 µs. The largest error was item 3: `validate_call` costs 0.83 µs for each call, not the 4 to 5 µs that py-spy suggested.

### 7.3 Blocking receipt modack in the Google Python client (latency over a network)

- **Symptom:** from the Mac, the Python transport handled 86 events per second in echo, with a p50 of 1.1 s. Its CPU was not saturated.
- **Test:** the Google Python client alone published fast (96 ms round trip, 500 messages in 245 ms). After a burst of 500 messages, delivery took 1,154 ms at p50. At one message every 200 ms, delivery took 144 ms.
- **Cause:** in google-cloud-pubsub 2.43.0, `StreamingPullManager._on_response` calls `_send_lease_modacks(..., receipt_modack=True)`. That calls `Dispatcher.modify_ack_deadline`, which calls `send_unary_modack`, a blocking unary RPC. It runs on the thread that reads the stream, before the messages go to the callbacks. Each received batch waits one network round trip, and the batches wait in a line. We found this from the code and from the burst and steady test. We did not instrument the client.
- **Effect in the same region:** the round trip is a few milliseconds, so the penalty is small. This is a property of the Google client, not of fastpubsub.

### 7.4 Server flow control stalls the Rust stream (tail latency)

- **Symptom:** in the same region, the Rust echo p50 was lower than the Python p50 (20 ms against 36 ms). The Rust p99 was 1,280 ms, which is a long tail. In the closed echo loop, the long tail lowered the events per second to 654.
- **Split:** the probe in `benchmarks/probes/rust_tail.py` split each latency. The publish part had a p99 of 41 ms. The delivery part had a p99 of 970 ms to 1,360 ms.
- **Pattern:** delivery stopped for the whole stream for 460 ms to 520 ms, many times. The slow messages came in bursts that lasted several seconds. The Google Python client had the same server pauses, but they did not chain:

  | Client | Events per second | Quiet 250 ms windows | Largest gap | Slowest message | Slower than 500 ms |
  |---|---|---|---|---|---|
  | Rust, limit 1,000 | 662 | 41 of 120 | 524 ms | 1,814 ms | 792 |
  | Google Python | 1,048 | 21 of 120 | 449 ms | 509 ms | 26 |

- **Cause:** the `max_outstanding_messages` value that the client sends to the server. At 1,000, the server throttled the stream, although only about 170 messages waited for an acknowledgment at a time.

  | Server limit | Events per second | p99 ms | Slower than 500 ms |
  |---|---|---|---|
  | 1,000 | 653 | 1,004 | 794 |
  | 2,000 | 1,155 | 487 | 0 |
  | 5,000 | 1,193 | 476 | 0 |

- **A missing guarantee:** the Rust crate uses `max_outstanding_messages` only for the server. Nothing enforced fastpubsub's `max_messages` on the client.
- **Fix:** the Rust task sends `max_messages * 2` to the server (`SERVER_OUTSTANDING_FACTOR`). The task runs at most `max_messages` handlers at the same time, and the other messages wait in a queue. At shutdown, the task stops starting handlers. Then it nacks the queued messages and the messages that arrive later. Then it waits for the stream. Two unit tests cover the limit and the shutdown.
- **Tried and dropped:** several streams for each subscription helped only because each stream got its own limit. One client for each stream had no effect.

### 7.5 Other findings

- `hashlib.sha256` does not scale on free-threaded CPython: about 10 times the CPU for each call with 8 threads. The benchmark CPU work changed to a pure Python loop.
- The official Rust Pub/Sub crate has no `PUBSUB_EMULATOR_HOST` support. The core uses `with_endpoint("http://host")` and anonymous credentials for the emulator.
- perf does not work in Docker Desktop unless `perf_event_paranoid` changes. We did not change it. The tokio profile comes from the macOS `sample` tool instead.

## 8. Where the Rust CPU goes now

Rust echo in Docker, measured phase, by thread group:

| Thread group | µs per event |
|---|---|
| Python threads (event loop, handler, framework) | 50.6 |
| tokio threads (Google crate: HTTP/2, lease loop, batch actor, wake-ups) | 42.7 |
| Bridge thread (Python objects for messages and results) | 5.3 |

Most of the tokio cost is inside the Google crate.

## 9. Known gaps

- Echo has 8% fewer events per second than python-optimized in the same region. Its p99 is equal and its p50 is half.
- `tests/integration/test_graceful_shutdown.py::test_factory_clients_closed_on_shutdown` fails with the Rust transport. It reads the private cache of the Python client factory, so it applies only to the Python transport.
- The Python 3.14t collapse of sync handlers (section 5.4) is not investigated.
- `SERVER_OUTSTANDING_FACTOR = 2` comes from the echo tests. It is not tested with other loads.
- The Rust transport never ran with exactly-once delivery, ordering keys or dead-letter topics on real Pub/Sub.
- The Rust core has no packaging or CI build yet.

## 10. How to reproduce

### 10.1 Local setup

```bash
BENCHMARK_CPUS=2 docker compose up -d --wait
just core-build
just core-build-container
```

`just core-build` builds the abi3 wheel for `.venv`, and the 3.14t and 3.15t wheels when `.venv-3.14t` and `.venv-3.15t` exist.

### 10.2 One official run on the emulator

```bash
docker compose exec -T fastpubsub python -m benchmarks.bench --scenario echo wait-only-100ms cpu-and-wait-100ms --duration 30 --warmup 10 --repeats 1 --cpu-iterations 17470 --results-file benchmarks/results/official.csv --case fastpubsub-async --transport rust --label rust
```

Run the same command with `--transport python --label python-optimized`, and with `--transport python --label python-current --fastpubsub-source .worktrees/bench-main` for `main`. Rotate the order in each round. Show the results:

```bash
docker compose exec -T fastpubsub python -m benchmarks.report --results-file benchmarks/results/official.csv --by-scenario
```

### 10.3 The publisher cache race (section 7.1)

Check out the commit before the fix, then record a timeline and a memory profile:

```bash
git checkout f12078d
```

```bash
docker compose exec -T fastpubsub python -m benchmarks.timeline --label race --output /src/benchmarks/results/race.csv -- --case fastpubsub-async --scenario wait-only-100ms --duration 20 --warmup 10 --cpu-iterations 17470 --transport rust
```

```bash
docker compose exec -T fastpubsub memray run --native --force -o /src/benchmarks/results/race.bin -m benchmarks.worker --case fastpubsub-async --scenario wait-only-100ms --duration 10 --warmup 5 --cpu-iterations 17470 --transport rust
```

The RSS in `race.csv` jumps in the setup phase. `memray stats` and the high-water-mark records show the allocations under `tonic MakeSendRequestService::call`, one for each connection. memray and py-spy must be installed in the container first (`uv pip install memray py-spy`).

### 10.4 Real Pub/Sub project with cost guards

1. Create a separate Google Cloud project and link a billing account.
2. Enable `pubsub.googleapis.com`.
3. Lower the Pub/Sub quotas `regionalpublisher` and `regionalstreamingpullsubscriber` to 300,000 per minute in each region. This is a hard cap. The Service Usage API `consumerOverrides` endpoint can set it with `force=true`.
4. Create a budget with email alerts. A budget only sends alerts. It does not stop spending.

Run a worker against the project from any machine with credentials:

```bash
env -u PUBSUB_EMULATOR_HOST BENCHMARK_PROJECT_ID=<project> .venv/bin/python -m benchmarks.worker --case fastpubsub-async --scenario echo --duration 30 --warmup 10 --cpu-iterations 17470 --transport rust
```

For the in-region runs, create an `n2-standard-4` VM in the region of the test, with a standard 30 GB disk and the `cloud-platform` scope. Install `build-essential`, uv, Python 3.12 and Rust 1.94.0. Copy the code, run `uv sync --group dev`, build the wheel with `uvx maturin build --release -m core/Cargo.toml` and install it. Calibrate the CPU work on the VM and pin each run with `taskset -c 0,1`. The VM list price was about R$1.19 per hour on 2026-11-08. The full matrix took about 50 minutes.

### 10.5 The blocking receipt modack (section 7.3)

Run from outside Google Cloud, then from a VM in the region:

```bash
BENCHMARK_PROJECT_ID=<project> .venv/bin/python -m benchmarks.probes.python_delivery
```

Outside Google Cloud, the burst delivery p50 is much higher than the steady delivery p50. In the region, the difference is small.

### 10.6 The flow-control stall (section 7.4)

Run on a VM in the region, with the Rust transport installed:

```bash
BENCHMARK_PROJECT_ID=<project> OUTSTANDING_LIMIT=1000 ACK_AFTER_PUBLISH=1 taskset -c 0,1 .venv/bin/python -m benchmarks.probes.rust_tail
```

```bash
BENCHMARK_PROJECT_ID=<project> OUTSTANDING_LIMIT=2000 ACK_AFTER_PUBLISH=1 taskset -c 0,1 .venv/bin/python -m benchmarks.probes.rust_tail
```

With a limit of 1,000, the probe shows gaps of about 500 ms without arrivals. Hundreds of messages are slower than 500 ms. With 2,000, no message is slower than 500 ms.

## 11. Tools

| Tool | Use |
|---|---|
| `benchmarks/timeline.py` | RSS and CPU for each thread group, 4 samples each second, with run phases. macOS gives only the total CPU of another process. |
| memray `--native` | Allocation sites at the heap peak, Python and native frames together. |
| py-spy `--gil` | Where the thread that holds the GIL spends time. It does not see Rust threads. |
| macOS `sample` | Native stacks of the tokio threads. |
| `benchmarks/probes/rust_tail.py` | Publish and delivery latency split, and arrival gaps. |
| `benchmarks/probes/python_delivery.py` | Delivery delay of the Google Python client for a burst and for a steady rate. |

## 12. Result files

| File | Content |
|---|---|
| `benchmarks/results/official.csv` | First official run on the emulator |
| `benchmarks/results/official-threads.csv` | A/B run with Python calls on separate threads |
| `benchmarks/results/memory-arenas.csv` | Run with `MALLOC_ARENA_MAX=2` |
| `benchmarks/results/official-fixed.csv` | Emulator, after the publisher cache fix |
| `benchmarks/results/official-bottlenecks.csv` | Emulator, after the bottleneck changes |
| `benchmarks/results/three-way.csv`, `three-way-cpu.csv` | Python, Python with Rust and free-threaded builds on the Mac |
| `benchmarks/results/runs.csv` | Dispatch and batch size tests |
| `benchmarks/results/official-gcp.csv` | Real Pub/Sub in the region, before the flow-control fix |
| `benchmarks/results/official-gcp-flow.csv` | Real Pub/Sub in the region, final |

## 13. Verdict

**Go. Make the Rust transport the default transport.**

FastPubSub is public, but nobody uses it yet. A change of the default transport breaks no user, so the usual reason to keep a new transport opt-in does not apply.

### Why

- In a real deployment, the Rust transport uses 76% to 77% less CPU for each event in the I/O scenarios. In the CPU-bound scenario, it uses 32% less.
- It uses about 40% less peak memory.
- Its p99 latency is equal in echo and 30% to 45% lower in the scenarios with a wait.
- It has 45% to 47% more events per second with 1,000 messages in flight.
- The gain on real Pub/Sub is almost twice the gain on the emulator. Real TLS and authentication cost the Python stack much more than the Rust stack.
- With Rust as the default, the library does not import the Google Python client on the main path. It also gets a path to free-threaded Python.

### Risks

- The Google Rust crate is young (version 1.7). It has no emulator support. It applies `max_outstanding_messages` only on the server. We found that by accident, so other surprises can exist.
- Exactly-once delivery, ordering keys and dead-letter topics did not run with the Rust transport on real Pub/Sub.
- Each run in this PoC lasted 40 seconds. No soak test shows the memory, the lease extension and the reconnects over hours.
- A Rust core needs a Rust toolchain, wheels for each platform and Python version, and PyO3 upgrades.
- Echo has 8% fewer events per second. The Python 3.14t collapse of sync handlers is not investigated.

### Steps

1. Merge Phase 0 as a separate change. It makes the Python transport 35% to 38% cheaper than `main`, and the Python transport stays as the fallback.
2. Make `rust` the default value of `FASTPUBSUB_TRANSPORT`. Keep `python` as the fallback for platforms without a wheel and for problems in the Rust path.
3. Before the next release, pass these gates:
   - Feature parity tests on real Pub/Sub: exactly-once delivery, ordering keys, dead-letter topics, filters, retry policies and graceful shutdown under load.
   - CI wheels for Linux x86_64 and arm64 and for macOS arm64, abi3 for Python 3.12 and later. Free-threaded wheels are optional.
   - A soak test of several hours in a Google Cloud region, with the memory and the redeliveries recorded.
   - A transport-independent version of `test_factory_clients_closed_on_shutdown`.
4. After the gates pass and one release runs, decide if the Python transport can go. One transport halves the work to keep features in parity.

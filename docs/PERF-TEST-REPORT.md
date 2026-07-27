# Test report — File Connector performance, in-cloud (GCS source → SFTP sinks)

**Date:** 2026-07-23
**Scope:** throughput and latency of the Solace PubSub+ File Connector (Micro-Integration) with a
**GCS** source, folder-based routing (recursive `file_type: 4`), **SFTP** sinks, over a **Solace Cloud**
broker — measured with the **entire stack deployed in the cloud**, colocated near the broker (no
home/office WAN link in the path). Setup and routing model: see the repo `README.md`.

---

## 1. Environment & setup

| Item | Location / value |
|---|---|
| Source GCS bucket | GCP **europe-west1** (Belgium) |
| Archive bucket | separate bucket, same region (for dedup) |
| Solace Cloud broker | AWS **eu-west-1** (Ireland) — RTT from VM ~25 ms |
| Connector stack | GCE VM `e2-standard-4`, europe-west1 (deleted after test) |
| Deployed on the VM | `fc-source` + `fc-sink-a/b/c` + `sftp-dest-a/b/c` (this repo's `docker-compose.yml`) |
| Publish chunk | ~1 MB default (`MAX_CONFIGURED_CHUNK_BUFFER_SIZE = 999936`) |
| Source scan | `restart_time_sec: 15` |

Rationale: an earlier iteration (connector on a laptop) showed the dominant cost was the
connector↔broker WAN link (~10 MB/s @100 MB, degrading to ~3 MB/s @400 MB). This report covers the
final setup — connector, sinks and SFTP destinations all in the cloud near the broker — to measure the
connector's actual ceiling and its behaviour under fanout and multi-file load.

**Method:** single clock (UTC) via `docker logs -t`. Segments — S3b: first publish → `FA_COMPLETE`
(source→broker); E2E: run START → last file written on the SFTP dest (`stat` mtime). Files co-scheduled
in one run by staging (stop source → upload objects → start source → single scan). Uploads done over
LAN from the VM with `gcloud`, parallel composite uploads disabled (see §7).

---

## 2. Scenario A — single-file throughput / size scaling

Source→broker publish segment (S3b), default ~1 MB chunk.

| Size | S3b (publish→complete) | Throughput |
|---|---|---|
| 100 MB | ~3.5 s | **~28 MB/s** |
| 400 MB | ~7.2 s | **~56 MB/s** |

Throughput **increases** with file size: larger files fill the publish pipeline and amortize the
per-chunk persistent-ack overhead. GCS read is not the bottleneck (first chunk out in ~0.5 s, streaming
`blob.reader()`).

---

## 3. Scenario B — fanout to 3 destinations

3× 10 MB, one file per destination (destA/B/C), processed in a single run.

| Milestone | Time (rel. run START) |
|---|---|
| destA first publish | +0.96 s |
| destB first publish | +4.23 s |
| destC first publish | +6.34 s |
| Last file written (dest-c) | **+10.1 s** |

Destinations are handled **one after another** (destA → destB → destC), not concurrently. Total fanout
time is the sum of the per-destination times (~30 MB in ~10 s).

---

## 4. Scenario C — multiple files per destination

9× 10 MB, three files per destination (3 dests × 3 files), single run (9 files, 99 multiparts, 90 MB).

| Metric | Value |
|---|---|
| Total (START → last file written) | **~22.3 s** |
| Per file | ~2.48 s |
| Aggregate throughput | ~4 MB/s |

All 9 files processed serially by one worker. Small files (10 MB ≈ 11 chunks) are **overhead-bound**
(~2.5 s/file) even with no WAN — the pipeline never reaches steady state.

---

## 5. Scenario D — file-count sweep, 100 KB files (single destination)

100 KB files staged into a single destination (destA), sweeping the file count **n** from 1 to 300;
each file is one chunk, so this isolates the **per-file processing overhead** at scale.

| n | delivered | source runs | total (START→last write) | ms/file | aggregate |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 0.8 s | 823 ms | 0.12 MB/s |
| 10 | 10 | 1 | 2.9 s | 289 ms | 0.34 MB/s |
| 50 | 50 | 1 | 12.6 s | 253 ms | 0.39 MB/s |
| 100 | 100 | 1 | 24.6 s | 246 ms | 0.40 MB/s |
| 200 | 200 | 2 | 55.7 s | 278 ms | 0.35 MB/s |
| 300 | 300 | 2 | 75.0 s | 250 ms | 0.39 MB/s |

Total time scales **linearly** with file count: a steady ~**250 ms per file** (n ≥ 10), independent of
n. The cost is pure per-file overhead (single 100 KB chunk), so aggregate throughput sits at ~0.4 MB/s
regardless of count — 300 files take ~75 s. (Batches of 200+ files were split across two source runs;
this did not change the linear per-file cost.)

---

## 6. Key findings

1. **The connector does not parallelize.** A single worker (`pool-4-thread-1`) processes every file in a
   run serially, regardless of destination. Fanout across N destinations and multiple files per
   destination both cost the **sum** of per-file times — there is no concurrency.
2. **Throughput grows with file size** (28 MB/s @100 MB, 56 MB/s @400 MB); small files are
   overhead-bound (~2.5 s per 10 MB file, ~250 ms per 100 KB file).
3. **Per-file overhead dominates for small files** — a steady ~250 ms/file floor, linear in file count
   (300× 100 KB = ~75 s, ~0.4 MB/s aggregate). File count, not size, drives the cost here.
4. **GCS read is fast** and never the limiting segment; time is spent in the persistent chunk publish.
5. Colocating the stack near the broker removes the WAN link; the remaining ceiling is the serial,
   single-worker processing model.

---

## 7. Robustness note (found during testing)

**Crash on an enumerated-then-deleted object.** `gcloud storage cp` (>~150 MB) uses *parallel composite
uploads*: temporary objects `gs://<bucket>/gcloud/tmp/…` are created then **deleted**. The connector
scans them and tries to archive them after deletion → `FA_POST_PROCESS_FILE_MOVE | Object does not
exist` → `error.handle: stop_all` → **fatal exit (Exit 1) + reboot**. Any uploader creating transient
objects crashes the source. Workaround: `CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED=False`
(also noted in the repo README "Known caveats").

---

## 8. Recommendations

1. **Colocate the connector (and sinks) with the broker** — same cloud/region ideally. This removed the
   dominant WAN cost and is the main throughput lever.
2. **Keep the default ~1 MB chunk.** (A separate 8 MB test degraded throughput — less pipeline overlap.)
3. For **high-volume or many small files**: batch into large files, or run **multiple source instances**
   partitioned by prefix (one per destX folder) to work around the single-worker serialization. **To be
   tested.**
4. Lower `restart_time_sec` if small-file latency matters (scan floor is 0–15 s).

**Study limitations:** 2–3 repetitions per point (higher variance at 400 MB); one run per fanout/multi-file
scenario; no SEMP queue-depth measurement under load; VM in GCP while the broker is in AWS (cross-cloud,
~25 ms) — a strict same-cloud colocation would likely push throughput higher still.

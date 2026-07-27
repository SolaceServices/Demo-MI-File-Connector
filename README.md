# Solace File Connector — GCS source → SFTP destinations (folder routing)

A self-contained test framework: the **Solace PubSub+ File Connector** pulls files from a **Google
Cloud Storage** bucket and routes each one to one of three **SFTP** destinations, based on the
object's top-level folder (prefix), over your Solace broker.

```
  gs://<bucket>/destA/*  ─┐                              ┌─►  sftp-dest-a:/outbound   (port 2222)
  gs://<bucket>/destB/*  ─┤  fc-source ──► Solace broker ─┤─►  sftp-dest-b:/outbound   (port 2223)
  gs://<bucket>/destC/*  ─┘   (1 flow)     (topics/queues) └─►  sftp-dest-c:/outbound   (port 2224)
        source bucket                                          3 destination SFTP servers
```

- One recursive source flow watches the whole bucket. The **prefix** (`destA`/`destB`/`destC`) is the
  routing key: it becomes the topic suffix, and each destination queue subscribes to its own prefix.
- After a file is replicated it is **moved to a separate archive bucket** so it isn't sent again.
- Add a destination by adding a `destD/` prefix, a `destD` cfg line, a `q.fc.dest-d` queue, and a 4th sink.

---

## 1. Prerequisites

- **Docker** + Docker Compose.
- The **Solace File Connector image** (obtain from Solace; this repo does not ship it). Load it and set
  `MI_IMAGE` in `.env`, e.g. `docker load -i pubsubplus-connector-file-<ver>-image.tar`.
- A **Solace broker** (PubSub+ Cloud or software) with client + management (SEMP) credentials.
- A **Google Cloud project** with two buckets and a service-account key (section 2).
- `gcloud` CLI (for setup/verification) — optional but recommended.

## 2. Google Cloud setup (once)

You need **two buckets** (source + archive — they must be different) and a **service account** whose
JSON key the connector uses.

```bash
PROJECT=your-gcp-project-id
SRC=your-source-bucket
ARCHIVE=your-archive-bucket
SA=mi-file-connector

gcloud config set project "$PROJECT"
gcloud storage buckets create gs://$SRC     --location <region> --uniform-bucket-level-access
gcloud storage buckets create gs://$ARCHIVE --location <region> --uniform-bucket-level-access

# service account + key
gcloud iam service-accounts create "$SA"
gcloud iam service-accounts keys create ./gcs-key.json \
  --iam-account="$SA@$PROJECT.iam.gserviceaccount.com"

# IAM — grant BOTH roles on BOTH buckets (see note):
for B in "$SRC" "$ARCHIVE"; do
  gcloud storage buckets add-iam-policy-binding gs://$B \
    --member="serviceAccount:$SA@$PROJECT.iam.gserviceaccount.com" --role="roles/storage.objectAdmin"
  gcloud storage buckets add-iam-policy-binding gs://$B \
    --member="serviceAccount:$SA@$PROJECT.iam.gserviceaccount.com" --role="roles/storage.legacyBucketReader"
done
```

> **Why two roles?** `objectAdmin` covers read/write/delete of objects (needed to replicate and to
> archive = copy+delete). But the connector also calls `storage.buckets.get` before listing, which
> `objectAdmin` does **not** grant — add **`roles/storage.legacyBucketReader`** on both buckets, or
> you'll get `storage.buckets.get denied` and the source won't start.

Seed a test object:
```bash
echo "hello A" | gcloud storage cp - gs://$SRC/destA/hello.txt
```

Validate the key locally:
```bash
gcloud auth activate-service-account --key-file=./gcs-key.json
gcloud storage ls gs://$SRC/destA/      # should list hello.txt
gcloud config set account you@yourorg   # switch back to your user
```

## 3. Configure (3 things to edit)

1. **`.env`** — `cp .env.example .env` and fill broker credentials, `MI_IMAGE`, `GCS_PROJECT_ID`,
   and `GCS_KEY_FILE` (path to the JSON key, e.g. `./gcs-key.json`).
2. **`config/source-gcs/file_paths.cfg`** — replace `REPLACE-WITH-YOUR-SOURCE-BUCKET` with your
   source bucket name. **Keep this file pure ASCII** (no accents/em-dash — a non-ASCII byte makes the
   connector fail to read it).
3. **`config/source-gcs/application.yml`** — in `postProcessFileMovePathExpr`, replace
   `REPLACE-WITH-YOUR-ARCHIVE-BUCKET` with your archive bucket name.

## 4. Run

```bash
# 1) create the broker queues + subscriptions (idempotent)
./broker/semp-setup.sh
./broker/semp-setup.sh show          # each q.fc.dest-* should show 2 subscriptions

# 2) start the stack (3 SFTP servers + source + 3 sinks)
docker compose up -d

# 3) check health
./scripts/verify.sh                  # or: curl -s localhost:8090/actuator/health
```

## 5. Test the routing

Upload objects to the source bucket under `destA/ destB/ destC/`, then watch them land:

```bash
echo "A" | gcloud storage cp - gs://<source-bucket>/destA/file-a.txt
echo "B" | gcloud storage cp - gs://<source-bucket>/destB/file-b.txt

# source picks up within ~15s (scan interval):
docker logs -f fc-source 2>&1 | grep -E "Blob Found|custom topic:.*/dest|Moved GCS|files processed"
```

Verify arrival:
- **FileZilla** (SFTP): host `127.0.0.1`, ports `2222`/`2223`/`2224`, user/pass from `.env`, folder
  `/outbound` — the file appears as `/outbound/<basename>`.
- **Archive**: `gcloud storage ls gs://<archive-bucket>/**` — the source object was moved there
  (the source bucket empties out).

### 5.1 Sending test files of different sizes (1 KB → 200 MB)

Generate a set of test files locally (random, incompressible content — the `dd` commands are portable
on macOS and Linux), then upload them to the source bucket.

```bash
BUCKET=your-source-bucket

# Avoid gcloud "parallel composite" uploads: their temporary objects can be scanned by the
# source and then vanish, crashing it. Disable once (see "Known caveats").
gcloud config set storage/parallel_composite_upload_enabled False

# 1) generate the files (1 KB, 64 KB, 1 MB, 10 MB, 50 MB, 100 MB, 200 MB)
mkdir -p /tmp/fc-test
dd if=/dev/urandom of=/tmp/fc-test/blob-1kb.bin   bs=1024    count=1   2>/dev/null
dd if=/dev/urandom of=/tmp/fc-test/blob-64kb.bin  bs=1024    count=64  2>/dev/null
dd if=/dev/urandom of=/tmp/fc-test/blob-1mb.bin   bs=1048576 count=1   2>/dev/null
dd if=/dev/urandom of=/tmp/fc-test/blob-10mb.bin  bs=1048576 count=10  2>/dev/null
dd if=/dev/urandom of=/tmp/fc-test/blob-50mb.bin  bs=1048576 count=50  2>/dev/null
dd if=/dev/urandom of=/tmp/fc-test/blob-100mb.bin bs=1048576 count=100 2>/dev/null
dd if=/dev/urandom of=/tmp/fc-test/blob-200mb.bin bs=1048576 count=200 2>/dev/null

# 2) upload them, spread across the three destinations (prefix = routing key)
gcloud storage cp /tmp/fc-test/blob-1kb.bin   gs://$BUCKET/destA/
gcloud storage cp /tmp/fc-test/blob-64kb.bin  gs://$BUCKET/destB/
gcloud storage cp /tmp/fc-test/blob-1mb.bin   gs://$BUCKET/destC/
gcloud storage cp /tmp/fc-test/blob-10mb.bin  gs://$BUCKET/destA/
gcloud storage cp /tmp/fc-test/blob-50mb.bin  gs://$BUCKET/destB/
gcloud storage cp /tmp/fc-test/blob-100mb.bin gs://$BUCKET/destC/
gcloud storage cp /tmp/fc-test/blob-200mb.bin gs://$BUCKET/destA/
```

To send them all to a single destination instead, upload every file to the same prefix:
```bash
gcloud storage cp /tmp/fc-test/*.bin gs://$BUCKET/destA/
```

Verify integrity at the destination (size + checksum) — e.g. for the 200 MB file on dest-a:
```bash
shasum -a 256 /tmp/fc-test/blob-200mb.bin
sftp -P 2222 demo@127.0.0.1:/outbound/blob-200mb.bin /tmp/ && shasum -a 256 /tmp/blob-200mb.bin
```

> Larger files take longer to appear (each is split into ~1 MB persistent messages published to the
> broker). Watch progress with the `docker logs fc-source` command above.

## 6. How it works — configuration reference

The settings below are deliberate; changing them will break routing.

| Setting | Where | Why |
|---|---|---|
| Dest queue subscribes to **`<base>/<destId>` AND `<base>`** | `broker/semp-setup.sh` | The data multiparts go to `<base>/<destId>`; the run **START** event (which initialises the sink) goes to the **base** topic. A queue missing the base sub crashes with `FA_ERROR_MISMATCH_IN_RUNID`. |
| `dest_file_name_type: 8` + `output-0.destination: /outbound/${FILE(FNAME)}` | `config/sink-*-sftp` | Writes the file **flat** as `/outbound/<basename>`. Otherwise the source tree is recreated and the write fails on a missing nested dir. |
| `alwaysMatchCompleteEvent: false` | `config/sink-*-sftp` | With multiple destinations, every sink receives the shared run COMPLETE event; non-target sinks must ignore it instead of failing. |
| Archive = **separate bucket** | `config/source-gcs/application.yml` | The source lists the whole source bucket; an in-bucket `archive/` prefix would be re-scanned forever. |
| Default chunk size (no `maxFileTransferSize`) | `config/source-gcs/application.yml` | Larger chunks measured **slower** against a cloud broker. Leave the default. |
| `file_paths.cfg` is **pure ASCII** | `config/source-gcs/file_paths.cfg` | The connector reads it with a strict charset; a non-ASCII byte aborts startup. |

Routing key: `${FILE(FNAME_P)}` = the object's parent folder → topic `solace/fc/source/demo/<destId>`.

## 7. Known caveats

- **Large uploads to GCS via `gcloud`**: parallel *composite* uploads create transient temp objects
  that the source may scan then find already deleted, causing the source to exit. Upload without
  composite (e.g. set `gcloud storage cp` without composite, or raise
  `GSUtil:parallel_composite_upload_threshold` above your file size).
- **Detection latency**: the source scans every 15s (`scheduler.restart_time_sec`), so expect up to
  ~15s before a new object is picked up.

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `storage.buckets.get denied` / source won't start | Add `roles/storage.legacyBucketReader` to the SA on **both** buckets (section 2). |
| Sink exits with `FA_ERROR_MISMATCH_IN_RUNID` | Dest queue missing the base-topic sub — re-run `./broker/semp-setup.sh` and check `show`. |
| Source exits reading the cfg (`MalformedInputException`) | Non-ASCII character in `file_paths.cfg` — retype it in plain ASCII. |
| Nothing routes | Confirm objects are under a `destX/` prefix; `docker logs fc-source`; queues exist (`semp-setup.sh show`). |

## 9. Layout

```
docker-compose.yml            # 3 SFTP dest servers + fc-source (GCS) + 3 sinks
.env.example                  # copy to .env and fill in
broker/semp-setup.sh          # create/show/teardown queues + subscriptions
config/source-gcs/            # source: application.yml + file_paths.cfg
config/sink-{a,b,c}-sftp/     # sinks (flat output to /outbound)
config/sftp/dest.sftp.json    # SFTP server users/dirs (demo/demo, /outbound)
scripts/verify.sh             # health + queue check
docs/PERF-TEST-REPORT.md      # in-cloud performance results (throughput, fanout, file-count sweep)
```

## 10. Stop / clean up

```bash
docker compose down            # add -v to also drop the SFTP data volumes
./broker/semp-setup.sh teardown
```

## 11. Target architecture — edge brokers + VPN bridges (design note)

This section describes the **target multi-site deployment**, not the single-broker test stack above.
No config for it ships in this package; it documents the intended design so it can be built later.

### Goal
Keep the **GCS source and the source connector centralised** on the current **cloud broker**, but
move each **destination close to where its files must land**: one **local/edge Solace broker per
SFTP destination**, co-located with that SFTP server and its **sink connector**. Each destination's
**sink queue is deported to its edge broker**; messages travel from the central broker to each edge
over a **Message VPN bridge**.

```
                 ┌───────────────────────── central (cloud) broker ─────────────────────────┐
   GCS  ──►  fc-source  ──►  publishes solace/fc/source/demo[/destX]                          │
                 │            bridge queue per site (durable, exclusive) on the CLOUD:         │
                 │              q.bridge.siteA  ⟵ subs: …/demo/destA  + …/demo                 │
                 └───────────────────────────────┬───────────────────────────────────────────┘
                         VPN bridge (one TCP conn, TLS 55443), INITIATED BY THE EDGE
                                                  │  (edge dials out to cloud)
              ┌───────────────────────────────────┼───────────────────────────────┐
   edge broker A (site A)                 edge broker B (site B)            edge broker C …
   q.fc.dest-a (durable) ⟵ subs …/destA,…/demo    q.fc.dest-b ⟵ …/destB,…/demo     …
   fc-sink-a  ──►  local SFTP  /outbound          fc-sink-b  ──►  local SFTP        …
```

### The bridge and its direction of initiation
A **Message VPN bridge** links two Message VPNs and forwards messages that match the bridge's
subscriptions (or its bridge queue) from a **remote** VPN to a **local** VPN. Crucially, you create
the bridge on the **local** broker; the broker then **opens a single outbound client connection to
the remote broker** (the bridge authenticates like a normal client).

Therefore the bridge is **defined on each edge broker**, with the **remote = the central cloud
broker**. The edge broker is the **initiator** — it dials **out** to the cloud (TLS, port 55443).
This fits networks where edge sites are behind NAT/firewalls and only **outbound** connectivity to
the cloud is permitted; no inbound port needs to be opened at the edge.

### Deporting a sink queue via bridge subscriptions (Guaranteed messaging)
The sink's data is **persistent**, so the bridge must carry **Guaranteed** messages. Per Solace,
that requires a **bridge queue on the REMOTE (cloud) VPN** plus a matching **durable endpoint on the
LOCAL (edge) VPN**:

1. **On the cloud broker (remote), per site** — create a durable, **exclusive** *bridge queue*
   (e.g. `q.bridge.siteA`) and give it the **same subscriptions the sink needs**:
   `solace/fc/source/demo/destA` (data multiparts) **and** `solace/fc/source/demo` (the run START
   event). This queue attracts exactly site A's traffic.
2. **On the edge broker (local)** — create the bridge, point it at the cloud VPN and specify that
   bridge queue as the **remote message spool queue**. When the bridge comes up, a **consumer flow
   binds to the remote queue** and transports its Guaranteed messages over the single connection to
   the edge.
3. **On the edge VPN** — the **sink queue `q.fc.dest-a` lives here** (this is the "deported" queue),
   as a **durable queue** subscribed to the same topics (`…/destA` + `…/demo`). Because a matching
   **durable** endpoint exists locally, the messages stay Guaranteed (spooled). The local
   `fc-sink-a` consumes from it and writes to the local SFTP server.

> If the edge VPN had only *client* (non-durable) subscriptions, the bridged messages would be
> downgraded to Direct (not persisted); with **no** matching subscription they are discarded. So the
> deported sink queue **must** be a durable queue with the matching subscriptions.

### Notes / guardrails (from Solace docs)
- **One bridge queue per inbound bridge** on the cloud (don't share a bridge queue across sites).
- Keep **`alwaysMatchCompleteEvent: false`** on every sink: the base-topic (`…/demo`) carries the
  shared run START/COMPLETE, so each edge still receives events for runs targeting other sites and
  must ignore the non-matching COMPLETE.
- **No message loss**: leave `reject-msg-to-sender-on-discard` enabled on the local durable queues
  (default for queues). A Guaranteed message only unspools from the cloud bridge queue once every
  matching edge endpoint has spooled it.
- The bridge connection needs **TLS to the cloud** (port 55443) and a client username whose **client
  profile allows bridge connections**.
- In Solace Cloud, each service is a single Message VPN, so bridges are always **service-to-service**
  (cloud service ↔ each edge service).
- Alternative: Solace **Dynamic Message Routing (DMR)** can replace a static VPN bridge for the same
  cloud↔edge delivery; evaluate it if you prefer dynamic subscription propagation over static bridge
  subscriptions.

### What changes vs. this package
- The **source side is unchanged** (GCS + `fc-source` stay on the cloud broker).
- Per site, run only the **sink half** against the **edge broker**: the edge `q.fc.dest-x` (deported),
  `fc-sink-x` pointed at the edge broker, and the local SFTP server. The edge broker's bridge (to the
  cloud) + the cloud-side bridge queue replace the direct cloud consumption used in the test stack.

Sources: Solace docs — [Message VPN Bridges](https://docs.solace.com/Features/VPN/Message-VPN-Bridges-Overview.htm),
[Configuring Message VPN Bridges](https://docs.solace.com/Features/VPN/Managing-Message-VPN-Bridges.htm).

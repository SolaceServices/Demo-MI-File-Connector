# CLAUDE.md — project orientation & cloud reproduction

Solace PubSub+ **File Connector** demo: pull files from a **GCS** bucket, route each by top-level
folder (`destA/destB/destC`) to one of three **SFTP** destinations, over a Solace broker. One recursive
source flow; prefix = routing key; replicated files are moved to a separate archive bucket (dedup).

- **Local run (Docker on your machine):** follow `README.md` — it is the authoritative setup guide.
- **Performance results:** `docs/PERF-TEST-REPORT.md`.
- **This file:** quick orientation + how to reproduce the **full-cloud-on-GCP** deployment (a colleague
  should be able to follow §"Reproduce in full cloud" end to end).

## Layout (what matters)
- `docker-compose.yml` — 3 SFTP servers + `fc-source` (GCS) + 3 sinks. Image via `MI_IMAGE`, SA key via
  `GCS_KEY_FILE`, broker creds + folder queues via `.env`.
- `config/source-gcs/` — source `application.yml` + `file_paths.cfg` (bucket, routing, archive move).
- `config/sink-{a,b,c}-sftp/` — sinks, flat output to `/outbound`.
- `broker/semp-setup.sh` — create/show/teardown queues + subscriptions.
- `scripts/verify.sh` — health + queue check.

## Non-obvious facts (don't break these)
1. **Each dest queue subscribes to TWO topics:** its own `…/destX` **and** the base `${FC_BASE_TOPIC}`
   (carries the RUN START event that initializes the sink assembler). `semp-setup.sh` does this. Missing
   the base sub → sink crash `FA_ERROR_MISMATCH_IN_RUNID`.
2. **Sinks write flat:** `dest_file_name_type: 8` + `output-0.destination: /outbound/${FILE(FNAME)}`.
   Recreating the source tree over SFTP fails (`FA_ERROR_SFTP_CANNOT_STAT_DIRS`).
3. **Archive = a SEPARATE bucket.** The GCS source lists the whole bucket; an `archive/` prefix in the
   same bucket would be re-scanned forever.
4. **`file_paths.cfg` must be ASCII-only.** A non-ASCII char (e.g. an em-dash in a comment) →
   `FA_FILEPATHS_FTP MalformedInputException` → fatal exit.
5. **The SA needs `storage.buckets.get`.** Object roles alone are not enough — the connector calls
   `buckets.get` before listing. Grant `roles/storage.legacyBucketReader` on **both** buckets.
6. **Disable gcloud parallel composite uploads** when staging test files (>~150 MB): they create then
   delete temp objects the source races on → `FA_POST_PROCESS_FILE_MOVE Object does not exist` → fatal
   exit. Set `CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED=False`.
7. **Processing is serial** (one worker): fanout / multi-file = sum of per-file times, not concurrent.
   Throughput grows with file size; small files are overhead-bound (~250 ms each). Keep the ~1 MB chunk.

---

## Reproduce in full cloud (GCP)

Goal: run the entire stack (source + sinks + SFTP servers) on a VM **colocated near the broker**, to
remove the WAN link. In our runs this gave 3–18× vs a laptop (see `docs/PERF-TEST-REPORT.md`).

**Why near the broker, not near GCS:** the bottleneck is the connector↔broker publish, not the GCS read.
Pick the VM region to match the **broker's** cloud/region.

### 0. Prerequisites (on your workstation)
- `gcloud` CLI authenticated with a project that has **Compute** rights.
- The File Connector image tar (from Solace), e.g. `pubsubplus-connector-file-2.5.1-image.tar`.
- The GCS service-account JSON key, and a filled `.env` (copy `.env.example`).
- Two GCS buckets (source + archive) and the SA granted (see fact #5):
  ```bash
  for B in gs://<source-bucket> gs://<archive-bucket>; do
    gcloud storage buckets add-iam-policy-binding $B \
      --member="serviceAccount:<sa>@<project>.iam.gserviceaccount.com" \
      --role="roles/storage.legacyBucketReader"
  done
  ```

### 1. Find the broker's region, pick a matching VM region
```bash
dig +short <your-broker-host>.messaging.solace.cloud     # resolve the SMF host
whois <ip> | grep -iE "OrgName|Country"                  # AWS/GCP/Azure + region
```
Example from our setup: broker on **AWS eu-west-1 (Ireland)** → we used GCP **europe-west1** (adjacent,
~25 ms). Ideally use the broker's own cloud/region for a true LAN.

### 2. Create the VM (Debian)
```bash
ZONE=europe-west1-b            # match the broker region
gcloud compute instances create fc-perf --zone=$ZONE \
  --machine-type=e2-standard-4 --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=30GB --labels=purpose=fc-perf
```

### 3. Install Docker (+ gcloud if you'll stage files from the VM)
```bash
gcloud compute ssh fc-perf --zone=$ZONE --command='
  curl -fsSL https://get.docker.com | sudo sh
  # optional: Google Cloud CLI for LAN-speed uploads from the VM
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/gcs.list
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
  sudo apt-get update -qq && sudo apt-get install -y -qq google-cloud-cli'
```

### 4. Ship the stack to the VM
```bash
# from the repo root: bundle the repo (no big blobs), plus the image tar and the SA key
tar czf /tmp/stack.tgz config broker docker-compose.yml .env
gcloud compute scp /tmp/stack.tgz            fc-perf:~/stack.tgz --zone=$ZONE
gcloud compute scp pubsubplus-...-image.tar  fc-perf:~/conn.tar  --zone=$ZONE
gcloud compute scp <sa-key>.json             fc-perf:~/gcs-key.json --zone=$ZONE
```
On the VM, make sure `.env` has `GCS_KEY_FILE=./gcs-key.json` and `MI_IMAGE` matching the loaded image.

### 5. Bring it up
```bash
gcloud compute ssh fc-perf --zone=$ZONE --command='
  mkdir -p ~/stack && tar xzf ~/stack.tgz -C ~/stack && cp ~/gcs-key.json ~/stack/
  sudo docker load -i ~/conn.tar
  cd ~/stack
  ./broker/semp-setup.sh                       # queues + 2 subs/queue
  sudo docker compose up -d
  sudo docker compose ps'
```
Health: `curl -s localhost:8090/actuator/health` (source), `8091/8092/8093` (sinks). Expect a first
scan within ~15 s: logs show `Blob Found`, `custom topic:…/destX`, `Moved GCS file …`, `FA_COMPLETE`.

### 6. Drive a test (LAN upload from the VM)
```bash
gcloud compute ssh fc-perf --zone=$ZONE --command='
  gcloud auth activate-service-account --key-file ~/stack/gcs-key.json
  export CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED=False   # fact #6
  head -c 104857600 /dev/zero > f.bin
  gcloud storage cp f.bin gs://<source-bucket>/destA/f.bin'
# watch it route + measure via: sudo docker logs -t fc-source | grep -E "custom topic:.*/dest|FA_COMPLETE"
```
For deterministic single-run measurements, stage files while the source is stopped, then start it (one
scan picks them all up). Per-file completion = mtime on the dest server: `sudo docker exec sftp-dest-a
stat -c %.Y /home/demo/outbound/<file>`.

### 7. Tear down (avoid ongoing cost)
```bash
gcloud compute instances delete fc-perf --zone=$ZONE --quiet
gcloud compute instances list --filter="name~fc-perf"   # expect 0
```
The `roles/storage.legacyBucketReader` grant on the SA is persistent — keep it for future runs or revoke
with `remove-iam-policy-binding`.

# Memory stress test — out-of-core ingestion

Experiment label: **memory-stress-test**

## Why this exists

The `watson_notes` bronze pipeline kept getting `OOMKilled` in production. This
experiment reproduced the failure locally, quantified it, drove the fix, and
proved the fix — first on synthetic data, then on the real collection.

## What's here

| File | Purpose |
|---|---|
| `measure_ingest.py` | Seeds a synthetic `watson.notes` collection of N docs into local MongoDB, runs the **real** `notes` bronze pipeline end to end, and reports peak RSS of the whole process tree (Python driver + Spark JVM). |
| `measure_curve.sh` | Runs `measure_ingest.py` across several sizes and prints the memory-vs-size curve. |

## How to run

```bash
# local MongoDB + PostgreSQL
docker-compose up -d

# one size
poetry run python experiments/memory-stress-test/measure_ingest.py \
    --rows 60000 --content-bytes 1500

# the full curve — pin to the worker pod's core count for a faithful figure
SIZES="20000 60000 120000 300000" taskset -c 0-1 \
    bash experiments/memory-stress-test/measure_curve.sh
```

A **rising** curve is the bug; a **flat** curve is the fixed, out-of-core
behaviour.

## What we tried

### 1. Baseline — reproduce the OOM

Ran the unmodified framework against synthetic `watson.notes` of growing size,
Spark driver 512m, pinned to 2 cores (the production worker pod is 2 cpu / 4 Gi):

| rows | raw data | peak RSS | outcome |
|---|---|---|---|
| 20 000 | ~30 MB | 1272 MB | OK |
| 60 000 | ~90 MB | 1852 MB | OK |
| 120 000 | ~180 MB | — | **`java.lang.OutOfMemoryError: Java heap space`** |

Peak RSS grew **linearly** — ~14.5 MB per 1000 rows. Extrapolated, the real
collection needs ~8–10 GB → the production `OOMKilled` on the 4 Gi pod.

### 2. Root cause

The framework collapsed whole datasets into the driver at three points, then
ran Spark in a config that could not recover:

- **Reads** — `list(find({}))` + `createDataFrame` pulled whole collections in.
- **Writes** — `df.toPandas()` collected the entire DataFrame into the heap.
- **Spark** — `local[1]`, 512m, AQE off, no spill disk.

### 3. The fix (framework changes, shipped in the same PR)

- Lazy `$bucketAuto` `_id`-range **partitioned** MongoDB read.
- `foreachPartition` **COPY-streamed** Postgres writer + atomic staging swap.
- Real Spark config: `local[*]`, AQE, `spark.local.dir` spill disk.
- Pipeline output **persisted DISK_ONLY** so upstreams are read once.
- `read_partitions` 16 → 64 — small chunks for large, size-skewed documents.

### 4. After the fix — flat curve

Same harness, same 2-core / 4 Gi envelope:

| rows | raw data | baseline | after |
|---|---|---|---|
| 20 000 | ~30 MB | 1272 MB | 1071 MB |
| 60 000 | ~90 MB | 1852 MB | 1213 MB |
| 120 000 | ~180 MB | **OOM** | 1220 MB |
| 300 000 | ~450 MB | (≈5 GB, OOM) | 1332 MB |

Peak RSS is **flat** — 15× the data costs +24% memory. Memory is bounded by one
partition, not the dataset size.

### 5. Production validation

Ran the **unmodified** Watson `notes` pipeline on the fixed framework against
the real `watson.notes` Atlas collection — **30,379 docs / 1.65 GB** (~54 KB
avg/doc). Result: **30,381 rows** landed in `poorbricks.bronze.watson_notes` +
the contract was pushed. Three issues surfaced and were fixed along the way:

1. **Slow** — the lazy DataFrame was re-read from Atlas ~10× (once per
   validation scan). Fixed by persisting the output DISK_ONLY → Atlas read once.
2. **OOM on real data** — real docs are large and an unpinned `local[*]` on a
   many-core dev box ran 16 partitions at once in a 2g heap. Fixed by smaller
   partitions (64) and pinning the local run to the pod's 2 cores.
3. **`kubectl port-forward` dropped** mid-run — a local-only connectivity
   issue; the in-cluster Airflow worker reaches Postgres directly.

## Outcome

`watson_notes` — which "kept failing" — now completes within the 4 Gi pod, and
peak memory no longer scales with dataset size.

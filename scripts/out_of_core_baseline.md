# Out-of-core refactor — Phase 0 baseline

Reproduced with `scripts/measure_ingest.py` against the watson `notes` bronze
pipeline (local MongoDB + PostgreSQL), Spark driver 512m, content ~1.5 KB/doc.

| rows | raw data | peak RSS | outcome |
|---|---|---|---|
| 20 000 | ~30 MB | 1272 MB | OK (32 s) |
| 60 000 | ~90 MB | 1852 MB | OK (42 s) |
| 120 000 | ~180 MB | — | **FAIL — `java.lang.OutOfMemoryError: Java heap space`** |

Peak RSS grows ~14.5 MB per 1000 rows above a ~1 GB floor — linear in dataset
size. Spark also warns of single tasks of 16–92 MB (the driver-side
`createDataFrame` collapse). Extrapolated, 1 M notes needs ~15 GB → the
production `OOMKilled` on the 4 Gi worker pod.

## After the refactor

Same harness, same 2-core / 4 Gi envelope, after Phases 1–3 (partitioned
MongoDB read, partition-streamed COPY writer + atomic swap, real Spark config
with disk spill, SQL-side profiling):

| rows | raw data | baseline | after |
|---|---|---|---|
| 20 000 | ~30 MB | 1272 MB | 1071 MB |
| 60 000 | ~90 MB | 1852 MB | 1213 MB |
| 120 000 | ~180 MB | **OOM** | 1220 MB |
| 300 000 | ~450 MB | (≈5 GB, OOM) | **1332 MB** |

Peak RSS is now **flat** — 15× the data costs +24% memory (vs. linear growth
to an OOM before). Memory is bounded by one partition, not the dataset size.
`watson_notes` completes well within the 4 Gi pod.

Re-measure with `SIZES="20000 60000 120000 300000" bash scripts/measure_curve.sh`
(pin to the pod's core count with `taskset -c 0-1` for a faithful figure).

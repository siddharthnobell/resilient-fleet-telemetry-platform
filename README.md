# Resilient Fleet Telemetry Platform

Architecture design and PySpark implementation for a fault-tolerant, horizontally-scaled
platform processing telemetry from a 500,000-vehicle logistics fleet — built for a Big Data
Platforms & Analytics coursework assignment.

Every claim below is backed by real, executed output in the notebook — not just theory.
Where a result came out messier than expected, that's reported honestly rather than smoothed
over (see **Known Limitations** at the bottom).

> **All Part 3 code was executed end-to-end on Google Colab.** The full execution logs — console
> output, physical plans, error messages, timing data — are preserved as real cell outputs
> inside `Trim3_Big_Data_Assignment.ipynb` (GitHub renders these inline; no need to re-run
> anything to see them). Nothing in this repo is a code listing without corresponding executed
> evidence.

## Repository Structure

```
.
├── README.md                          # this file
├── part1_2_written_analysis.md        # Parts 1 & 2 — architecture & MapReduce design (no code)
├── generate_telemetry_data.py         # synthetic dataset generator (fixed seed, reproducible)
└── Trim3_Big_Data_Assignment.ipynb    # Parts 3 & 4 — full PySpark implementation, executed
                                        # end-to-end on Google Colab, outputs included
```

## What's Covered

| Part | Topic | Format |
|---|---|---|
| 1 | Scaling strategy (the "Wall," horizontal vs. vertical, the Three Vs); CAP theorem, ACID vs. BASE | Written |
| 2 | MapReduce logical flow (Split/Map/Shuffle/Sort/Reduce); Hadoop vs. Spark for iterative ML | Written |
| 3.1 | Ingest + average engine temperature per vehicle model; narrow vs. wide dependencies proven via `.explain()` and `.toDebugString()` | PySpark, executed |
| 3.2 | Data-skew mitigation via salting; hash vs. range partitioning | PySpark, executed |
| 3.3 | Fault tolerance via RDD lineage — deliberately crashed a task and proved recovery via recomputation, not replication | PySpark, executed |
| 3.4 | Checkpointing to truncate excessive lineage depth; checkpointing vs. caching | PySpark, executed |
| 4 | Lazy evaluation & DAG/stage construction; data locality; the "Liability of Lineage" | Written, grounded in Part 3's real output |

## Key Results (Empirically Verified, Not Asserted)

- **500,000 synthetic telemetry rows**, with a deliberately injected ~630× skew (3 of 5,000
  vehicles generating the bulk of the traffic) — mirrors the assignment's "some trucks generate
  1000x more logs" scenario.
- **Narrow vs. wide dependencies** confirmed via real physical plans: narrow steps produced zero
  `Exchange` nodes; the `groupBy().avg()` and `orderBy()` each produced their own `Exchange`,
  confirming two separate shuffles, not one.
- **Salting correctness** verified to ~1e-13 precision against an unsalted baseline — the
  weighted (sum/count) recombination was checked, not assumed correct.
- **Fault tolerance** proven by deliberately crashing a partition's first task attempt (real
  `WARN TaskSetManager: Lost task...` log line) and confirming recovery matched baseline exactly.
- **Checkpointing** proven to truncate lineage from **22,760 → 399 characters** (~57×) at the
  moment of checkpointing — measured directly via `toDebugString()`, not inferred.
- **A genuinely non-obvious finding**: the OOM failures encountered while building this section
  lived in the Spark **driver's** JVM (plan analysis/serialization), not the executors' — proven
  by quadrupling executor memory with zero effect, then resolving it with `spark.driver.memory`.

## Environment

- PySpark 3.5.6, Java 17 (Temurin), run on Google Colab
- `local-cluster[2,2,4096]` mode with explicit `spark.driver.memory` — used deliberately (not
  `local[*]`) so that `spark.task.maxFailures` is actually honored, which Part 3.3's failure
  injection depends on
- Dataset generated with a fixed random seed for full reproducibility

## Known Limitations (Disclosed Honestly)

- **Part 3.4's second truncation check** (iteration 30) showed no measurable lineage growth
  since the prior checkpoint, which doesn't match the expected pattern — flagged in the notebook
  as an open anomaly rather than hidden or explained away without evidence.
- **The recovery-time wall-clock comparison** in Part 3.4 came out too noisy at the working
  set's small scale (5,000 rows, 1 partition) to support a clean quantitative multiplier. The
  "checkpointing bounds recovery time" claim is instead supported by the measured lineage-depth
  bound (5 iterations of history vs. 200), which is real and sufficient on its own.

## Course Context

Assignment: *Architecting and Implementing a Resilient Global Telemetry Platform*, covering
Modules 1–8 (Big Data fundamentals through Spark resilience mechanics).

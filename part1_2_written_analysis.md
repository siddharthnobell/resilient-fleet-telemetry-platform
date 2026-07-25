# Parts 1 & 2 — Written Analysis

*System Architecture, Data Paradigms, Batch Processing & MapReduce*

---

## Part 1: System Architecture & Data Paradigms

### 1. Scaling Strategy

**The hardware wall on a single node**

A single machine hits three physical ceilings regardless of budget:

- **CPU wall** — since ~2005, clock speeds plateaued because denser transistors generate heat
  faster than it can be dissipated, and signal propagation is physically capped.
- **Memory wall** — RAM capacity can grow, but cost scales *exponentially*, and CPU–RAM bus
  bandwidth is fixed regardless of RAM size.
- **I/O wall** — disk/network transfer rate is typically the slowest link, with a hard ceiling
  on concurrent throughput.

A fleet of 500,000 vehicles streaming telemetry 24/7 hits all three simultaneously and
continuously — there's no off-peak window to catch up.

**Why horizontal (scale-out), not vertical (scale-up)**

1. **Cost curve** — vertical scaling is exponential (the "price wall"); horizontal scaling with
   commodity nodes is roughly linear.
2. **Single point of failure** — a 24/7 monitoring platform can't tolerate the "risk wall" of
   vertical scaling: one machine down means a fleet-wide blackout. Horizontal clusters lose one
   node out of hundreds without interruption.
3. **Hardware ceiling** — even the biggest single server eventually runs out of expansion slots.
   A fleet growing past 500,000 vehicles needs a strategy with no such ceiling.

**Tied to the Three Vs**

| V | This platform's pressure | Why it forces scale-out |
|---|---|---|
| **Volume** | 500,000 vehicles × continuous multi-signal telemetry exceeds any single machine's storage/RAM within hours | Requires distributed storage across many nodes |
| **Velocity** | Continuous 24/7 arrival; real-time monitoring needs near-real-time processing | Requires parallel processing — one CPU can't keep pace |
| **Variety** | Numeric time series (temp, speed), geospatial (GPS), potentially unstructured diagnostic logs | Favors flexible, distributed storage over one rigid schema |

**Conclusion**: this is a workload where a single node *structurally* cannot keep up with any
one V alone. Horizontal scaling is the only path that keeps the platform growable, available,
and affordable at once.

### 2. Consistency Models: ACID vs. BASE, and the CAP Theorem Choice

| | ACID | BASE |
|---|---|---|
| Stands for | Atomicity, Consistency, Isolation, Durability | Basically Available, Soft state, Eventual consistency |
| Guarantee | Strictly valid, immediately consistent state | Stays available/responsive; converges over time |
| Cost | Requires locking/coordination — can block under load or partition | Accepts temporary staleness for uptime/speed |
| Fit | Financial ledgers, inventory — a wrong answer is worse than a slow one | High-velocity streams where brief staleness is harmless |

**CAP applied here**: partition tolerance isn't optional for 500,000 geographically distributed
vehicles (constant signal drops, tower handoffs). That leaves the real choice as
**Consistency vs. Availability**.

For high-velocity coordinate ingestion specifically, the right choice is **AP / BASE**:

1. **Staleness is cheap, unavailability isn't** — a 1–2 second-old GPS ping is harmless; a newer
   one is already in flight. Blocking writes during a network blip (constant at this scale)
   actively drops the monitoring data the platform exists to provide.
2. **Overwrite-heavy, not transaction-heavy** — each coordinate supersedes the last; there's no
   equivalent of a balance that must never double-count.
3. **Strict consistency doesn't scale here** — ACID-style locking/consensus overhead grows with
   fleet size, undermining the horizontal-scaling argument from Part 1.1.

**Scoping note**: this AP/BASE choice is for the *real-time ingestion* layer specifically.
Predictive maintenance (historical analysis/training) is a separate downstream concern that can
use stronger consistency once data lands in durable storage — this is best treated as a
multi-layered architecture, not one consistency model applied everywhere.

---

## Part 2: Batch Processing & MapReduce

### 1. Logical Flow: Total Miles Driven Per Vehicle Model

**Assumption (stated explicitly)**: each telemetry ping reports *incremental* distance
(`miles_since_last_ping`), the common OBD-II/ELD transmission format — this keeps it a clean
single-pass job.

| Phase | What happens | This job's specifics |
|---|---|---|
| **Split** | Input broken into fixed-size blocks | 128MB chunks of raw logs, no grouping yet |
| **Map** | Each mapper processes its block in parallel | Emit **key = vehicle_model, value = miles_since_last_ping**; discard GPS/temp/battery fields |
| **Shuffle** | Routes every key instance to the same reducer | `hash(vehicle_model) mod R` — the network-intensive step |
| **Sort** | Groups/orders values per key before reduce runs | Reducer gets `(vehicle_model, [d1, d2, d3, …])` pre-grouped |
| **Reduce** | Aggregation logic runs once per key | `sum(list)` → `(vehicle_model, total_miles)`, written to DFS |

**Why the assumption matters**: if telemetry instead reports *cumulative* odometer readings,
naive summing wildly overcounts. That case needs a **two-stage chained job**: Job 1 groups by
`vehicle_id`, computes `max(odometer) − min(odometer)` per vehicle; Job 2 re-runs
Split→Map→Shuffle→Sort→Reduce on Job 1's output, grouped by `vehicle_model`, to sum across
vehicles. Worth naming explicitly since it's a classic real-world gotcha in fleet telemetry
pipelines.

### 2. Hadoop vs. Spark for Iterative ML

Predictive maintenance (e.g., k-means clustering, gradient-descent failure prediction) revisits
the same dataset dozens to hundreds of times as a model converges.

**Hadoop's failure mode**: every iteration is treated as a separate job, reading from HDFS and
writing intermediate/final results back to disk (replicated 3×) each time. For a 100-iteration
run, that's 100 full disk read/write cycles — in practice, total I/O time can exceed the actual
computation time.

**Spark's fix**: reads historical data from HDFS **once**, caches it in worker RAM; every
subsequent iteration operates on the cached copy — no repeated disk round-trips. Combined with
DAG-based lazy evaluation (pipelining operations within an iteration instead of materializing
every step to disk), this is what produces the commonly-cited 10–100× speedup — specifically
for iterative workloads, not simple one-pass batch jobs.

**Honest trade-off**: this speed requires the working dataset to fit in cluster RAM — a real
capacity-planning constraint at 500,000-vehicle scale (sampling, feature reduction, or
provisioning enough memory), not a reason to avoid Spark.

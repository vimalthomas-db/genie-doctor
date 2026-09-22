# Genie Doctor — Requirements (v2, 2026-09-02)

Fleet-health scorecard for **Genie spaces**, built in the shape of the Banner Health
Executive Scorecard. Ranks and diagnoses every monitored space across four pillars
**Freshness · Correctness · Feedback · Richness** plus benchmarks, with an overview
"first page" of portfolio KPIs + trends and per-space (per-agent) deep-dive subpages.

Focus of this pass (chosen 2026-09-02): **Freshness, Speed, Correctness, Feedback,
Richness.** Scope stays **Genie spaces only**.

---

## Scoring model — PERCENTAGES, no grade (decided 2026-09-02)
No A–F letter grade and no LLM verdict. Every pillar is an **objective percentage or
count**. Quality is expressed as **% of benchmark traces that pass**; feedback as
**% positive**. This is deterministic, defensible, and drops `ai_query` from the hot path.

## The four pillars + benchmarks

### 1. Freshness  *(new — problem #1)*
Is the data behind the score current, and is the space itself active?
- **Harvest freshness**: age of the latest harvest per space (data staleness).
- **Activity freshness**: days-since-last-message (`days_since_active`).
- **Config freshness**: space `last_updated_timestamp` (curation recency).
- Live reality today: harvest idle spans **9→316 days** — a monitor showing 316-day-old
  data is not trustworthy, so freshness is a first-class KPI and gates confidence.

### 2. Quality (Correctness)  *(the discriminating pillar)*
"Didn't crash" ≠ "answered right." Completion/error saturates (5 of 6 spaces at 0% err),
so benchmark trace quality is the real quality signal — shown as a **percentage**.
- **`benchmark_quality_pct` = num_correct ÷ num_done** — % of benchmark traces that PASS.
- **`coverage_pct` = num_done ÷ benchmark_questions_defined** — how much of the defined
  set was actually run (a high quality on low coverage is not yet proven).
- Three underlying numbers stay visible: **defined** (curated_questions `BENCHMARK`) →
  **run** (eval-runs num_done) → **correct** (num_correct).
- 10 of 11 owned spaces have **defined-but-never-run** benchmarks → show that state
  (not "NO BENCHMARK"). Benchmark triggering stays OFF for v3.

### 3. Feedback
- **`feedback_positive_pct` = thumbs_up ÷ (thumbs_up + thumbs_down)** — how good the
  feedback is, as a percentage.
- **`feedback_coverage_pct` = rated ÷ msgs** — shown honestly (a near-zero-coverage
  space's positivity % is not yet meaningful).
- Live reality: ~1 rated message fleet-wide → coverage is the caveat, not a fabricated
  default. Semantic sentiment is a stretch goal, not v2.

### 4. Richness  *(new — the curation-quality pillar)*
How well-curated the space is. **Proven retrievable** via the legacy data-rooms API:
- **# examples** (SQL example queries) = count of `curated_questions`.
- **ranked SQL examples** = ordered curated_questions (by position / created_timestamp),
  each with `answer_text` (SQL) + `eval_note`; `is_deprecated` excluded.
- **instructions** = `/instructions` where `instruction_type = TEXT_INSTRUCTION`.
- **expressions** = `/instructions` where `instruction_type = SQL_INSTRUCTION`
  (each with `usage_guidance`, `instruction_status` ACCEPTED/PROPOSED, `use_as_tool`).
- **filters / expressions** = folded into `SQL_INSTRUCTION` (VERIFIED 2026-09-02: fleet
  has ONLY `SQL_INSTRUCTION` + `TEXT_INSTRUCTION` — no separate FILTER/JOIN/VALUE_DICT
  type; Genie authors filters/joins/expressions as SQL instructions).
- **examples vs benchmarks** = curated_questions `question_type`: `SAMPLE_QUESTION` = the
  "# examples" richness signal; `BENCHMARK` = defined benchmark questions (feeds Correctness).
- **data breadth** = count of `table_identifiers` (NOTE: reads 0 for some well-built
  spaces e.g. Banner/Pulse Semantic — verify breadth source for those before trusting).
- A "richness score" rolls these up: a space with many accepted instructions, curated
  examples, and defined benchmarks is richer than a bare space.

---

## Data sources (ALL verified live on Pulse, 2026-09-02)

| Signal group | Source | Notes |
|---|---|---|
| Activity / errors / latency / feedback / users | `genie_message_details` (Delta) | harvested; stale — must re-harvest |
| Conversations, depth | `genie_conversations` (Delta) | |
| Space title / warehouse / owner | `genie_spaces` (Delta) + `GET /api/2.0/genie/spaces/{id}` | |
| Warehouse health | `warehouses.get` | dead warehouse = hard F |
| **Benchmark RUNS (executed)** | `GET /api/2.0/genie/spaces/{id}/eval-runs` | needs CAN_MANAGE |
| **Benchmark DEFINED + examples** | `GET /api/2.0/data-rooms/{id}/curated-questions` | `question_type=BENCHMARK` = defined benchmark; others = examples |
| **Instructions / expressions** | `GET /api/2.0/data-rooms/{id}/instructions` | `instruction_type` TEXT / SQL |
| **Data breadth + config recency** | `GET /api/2.0/data-rooms/{id}` | `table_identifiers`, `last_updated_timestamp`, `run_as_type` |
| Benchmark trace detail (optional) | `GET .../eval-runs/{id}/results` | per-trace GOOD/BAD if we want trace-level pass% |

> **data-rooms is the legacy internal Genie API** — undocumented, could change. Isolate
> it behind one client function so a future public endpoint is a one-line swap.

---

## Architecture — Banner 3-tier

**Tier 1 — Overview (the "first page").** Portfolio, all agents at once.
- Header KPI row (overall): **# agents monitored**, and one headline % per pillar —
  Freshness (% fresh / median data age), **avg benchmark quality %**, **avg feedback
  positive %** (with coverage caveat), Richness (median richness).
- **Time trends** (fleet, over time): quality %, feedback %, richness — line charts from
  the daily snapshot table.
- Benchmarks summary: defined vs run vs correct across the fleet.
- Ranked leaderboard (worst-first, by benchmark quality %) — one row per agent, its
  pillar percentages shown as bars/bullets (no grade chip).

**Tier 2 — Per-agent subpage (selectable).** Pick a key agent from a selector.
- The four pillars as percentages + counts: Freshness tiles, Quality
  (quality % + coverage %, defined/run/correct + eval-run table), Feedback (positive % +
  coverage % + samples), Richness (examples list w/ SQL, SQL/text instruction counts,
  table breadth).
- Trends for that space; top questions; recent errors.

**Design system:** navy `#00205B` ink, blue `#007EB4`, orange `#E9631A` accent; semantic
RAG green `#1E7F5C` / amber `#B26E00` / red `#C0362C` carried by **color + shape + text**
(never color alone). KPI tile + benchmark-bullet + top/bottom + ranked-drilldown kit,
mirroring `~/banner_exec_scorecard_design.html`.

---

## Holistic solution — deployable data product (one DAB)

Not a live-querying Streamlit app. A self-contained **Databricks Asset Bundle** so
`databricks bundle deploy -t <workspace>` stands up tables + pipeline + job + Lakebase +
app together in any workspace. Layers:

```
Genie spaces in the workspace
   │ REST harvest (Python)          │ system.access.audit (declarative source)
   ▼                                 ▼
BRONZE (raw, append): genie_spaces · conversations · messages · eval_runs ·
        instructions · curated_questions · audit_events
   │ SDP declarative pipeline (MVs / streaming tables + expectations)
   ▼
GOLD (results): space_scorecard (4 pillars+grade) · benchmark_facts (defined/run/correct)
        · richness_facts · fleet_rollup · daily snapshots
   │ Lakebase synced tables (managed)
   ▼
Lakebase (Postgres)  →  App reads PG only (ms latency, no live API/LLM per load)
```

**SDP boundary (honest):** declarative pipelines can't poll arbitrary REST APIs, so:
- **Ingestion = Python task** (reuse genie-monitoring harvester + new data-rooms calls) →
  lands bronze. **User filter lives here.**
- **Transform = SDP pipeline** bronze→gold (expectations, incremental, lineage; +
  `system.access.audit` as native source).

**DAB resources:** `pipelines:` (SDP transforms) · `jobs:` (task1 ingest → task2 pipeline,
`schedule`) · `database_instances:` (`genie-doctor-db`, exists) + synced tables · `apps:`
(Banner-scorecard UI) · catalog/schema + bronze tables via setup task.

### Confirmed operating decisions (2026-09-02)
- **Ingestion scope: OWNER only** — spaces where `parent_path` = the configured owner
  (e.g. `/Users/<owner>@databricks.com%`). Config-driven owner param.
- **Refresh: once a day** — job `schedule` = daily; each run writes a fresh snapshot →
  fleet trends accrue one point/day.
- **Benchmark triggering: OFF (read-only)** for v3 — read defined + run state only; do not
  POST eval-runs. (Flag exists to enable later.)

### Speed (non-negotiable)
Batch computes everything (slow OK); app reads Postgres only. No warehouse / eval API /
data-rooms API / ai_query on page load.

### Portability — deploy into ANY workspace (2026-09-02)
The bundle must stand up cleanly in a fresh workspace with no hand-editing of IDs:
- **No SQL warehouse dependency anywhere** — pipeline + job are serverless; the app
  reads Lakebase Postgres. (The old warehouse coupling is gone with the grade model.)
- Only workspace-specific input is **`owner_prefix`**; host comes from the deploy profile.
- The bundle **creates** the Lakebase instance + synced tables (for a fresh workspace);
  on Pulse the instance already exists, so it's deferred / bound rather than re-created.
- Targets: `pulse` (dev, pinned to existing data) and `prod` (template for a new
  workspace — set catalog/schema/owner_prefix).

### Self-service refresh (2026-09-02)
- On `bundle deploy` the **app auto-deploys and starts** (no manual start step).
- The app has a **"Run diagnostics" / Refresh button** that triggers the daily job on
  demand (full harvest → transform). Mechanism: bind the job to the app as a resource
  with `CAN_MANAGE_RUN`; the app calls `jobs.run_now(job_id)` (job id from the bound
  resource env var) and shows run status. Data still served from Postgres; the button
  just refreshes it.

---

## Non-goals / deferred
- Non-Genie agents (Model Serving / Agent Framework) — out of scope this pass.
- Business-importance weighting — dropped (too subjective).
- Semantic NLP sentiment — stretch goal, not v2.

## Open questions
- OK for the ingestion/app service principal to hold **CAN_MANAGE** on owner's spaces
  (required for eval-runs reads and include_all harvest)?
- Confirm the full set of `instruction_type` values across the fleet (filters? joins?
  value dictionaries?) to finalize the Richness sub-metrics.
- Resolved 2026-09-02: user filter = OWNER only · refresh = daily · benchmark triggering
  = read-only (off).

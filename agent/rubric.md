# Genie Doctor — Gold-Standard Rubric

The yardstick the diagnostic agent compares each Genie space against. It defines
**what an accurate, well-curated Genie space looks like** and the named failure modes.
Focus is **accuracy — how correct the agent's answers are — NOT popularity or usage.**

The agent uses this rubric as calibration. It may only recommend a step that is backed
by evidence in the space's harvested bundle, and every step must cite that evidence.

---

## What "good" looks like

### 1. Instructions
- **Text instructions present and specific** — describe the domain, the grain, and the
  definition of every key term (e.g. what counts as a "denial", "overturned", "active
  member", the time window).
- **SQL instructions are reusable and valid** — each encodes a filter / expression / join
  pattern, references real tables and columns, and **matches the definitions in the text
  instructions**.
- **No internal conflict** — a concept is defined exactly ONE way across all instructions.
  Two instructions must not disagree on a filter, a grain, or a term's meaning.

### 2. Benchmarks (ground truth) — the accuracy backbone
- **At least one benchmark question per key business question.**
- **Ground-truth SQL (`benchmark_answer`) is correct** — its filters, joins, and grain
  match the instruction definitions; it references only real columns; it computes the
  intended answer. A wrong ground truth silently caps measured accuracy and is worse
  than no benchmark.
- **Benchmarks are actually run** (eval runs exist) and **cover the full defined set**.
- **Accuracy is high** (GOOD ÷ assessed) and **BAD traces are investigated**, not ignored.

### 3. Feedback
- Users are satisfied (positive feedback); negatives are triaged into instruction or
  benchmark fixes.

### 4. Data breadth
- Curated tables cover the questions the space is asked.

---

## Named failure modes

| Category | Decided by | Signal / how to detect |
|---|---|---|
| `NO_BENCHMARK` | **deterministic** | `benchmark_questions_defined = 0` OR `num_runs = 0` |
| `EMPTY_INSTRUCTIONS` | **deterministic** | `text_instructions = 0` (and/or `sql_instructions = 0`) |
| `LOW_COVERAGE` | **deterministic** | `coverage_pct < 100` (ran a subset of the defined set) |
| `LOW_ACCURACY` | **deterministic** | `benchmark_quality_pct` low despite adequate coverage |
| `NEGATIVE_FEEDBACK` | **deterministic** | `feedback_positive_pct` low OR thumbs_down present |
| `CONFLICTING_INSTRUCTIONS` | **agent (reasoning)** | two instructions define the same concept/filter/grain differently |
| `BAD_GROUND_TRUTH` | **agent (reasoning + schema check)** | a `benchmark_answer` SQL references a missing column, or its filter/grain contradicts an instruction definition, or it does not answer the question |

**Deterministic** findings are computed in code and are NOT the agent's to invent or
override. The agent reasons ONLY on `CONFLICTING_INSTRUCTIONS` and `BAD_GROUND_TRUTH` —
the two things that genuinely require reading the content.

---

## Output contract (per space)
A ranked list of **improvement steps** — no verdict, no grade. Each step:
- `step` — a concrete action ("Add ground-truth SQL for KBQ 'most common denial reason'")
- `why` — the grounded reason, referencing the observed evidence
- `evidence` — the exact id or metric it is based on (`instruction_id`,
  `benchmark_question_id`, or a named metric + value)
- `category` — one of the failure modes above
- `impact` — high / medium / low (effect on **accuracy**)

**Grounding contract:** a step with no verifiable citation is dropped before it reaches
the user. If the agent cannot cite it, the agent may not say it.

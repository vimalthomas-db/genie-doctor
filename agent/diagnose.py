"""Genie Doctor — diagnostic agent.

Produces a ranked list of GROUNDED improvement steps for one Genie space.
No verdict, no grade.

Trust model (see rubric.md):
  * DETERMINISTIC findings (code, not the LLM): NO_BENCHMARK, EMPTY_INSTRUCTIONS,
    LOW_COVERAGE, LOW_ACCURACY, NEGATIVE_FEEDBACK.
  * LLM findings (reasoning required): CONFLICTING_INSTRUCTIONS, BAD_GROUND_TRUTH.
  * Every step must cite a real evidence id / metric; steps whose citation cannot be
    verified against the bundle are DROPPED before returning.
"""
import json
import os
import re

MODEL = os.environ.get("MODEL", "databricks-claude-sonnet-4-5")
LOW_ACCURACY_THRESHOLD = 80.0
LOW_FEEDBACK_THRESHOLD = 50.0

DET = "deterministic"
LLM = "agent"


def _num(v):
    return None if v is None else float(v)


# --------------------------------------------------------------------------
# Deterministic findings — computed facts, NOT the LLM's to invent.
# --------------------------------------------------------------------------
def deterministic_findings(m: dict) -> list:
    steps = []
    bdef = int(m.get("benchmark_questions_defined") or 0)
    runs = int(m.get("num_runs") or 0)
    sqli = int(m.get("sql_instructions") or 0)
    txti = int(m.get("text_instructions") or 0)
    cov = _num(m.get("coverage_pct"))
    acc = _num(m.get("benchmark_quality_pct"))
    fbpos = _num(m.get("feedback_positive_pct"))
    tdown = int(m.get("thumbs_down") or 0)

    if bdef == 0 or runs == 0:
        step = ("Define 3–5 benchmark questions from your top real user questions, attach a "
                "verified ground-truth SQL answer to each, then run an eval to measure accuracy.")
        if sqli > 0:
            step += f" You already have {sqli} curated SQL example(s) — promote the key ones into benchmark questions."
        steps.append({
            "category": "NO_BENCHMARK", "impact": "high", "source": DET, "step": step,
            "why": "Accuracy cannot be measured or trusted until a benchmark with ground truth is defined and run.",
            "evidence": f"benchmark_questions_defined={bdef}, num_runs={runs}, sql_instructions={sqli}"})
    if txti == 0 and sqli == 0:
        steps.append({
            "category": "EMPTY_INSTRUCTIONS", "impact": "high", "source": DET,
            "step": "This space has no curation at all — add text instructions (domain, grain, key-term definitions) AND curated SQL example queries.",
            "why": "With no instructions Genie has no guidance, so it guesses table/column meaning and answers inaccurately.",
            "evidence": f"text_instructions={txti}, sql_instructions={sqli}"})
    elif txti == 0:
        steps.append({
            "category": "EMPTY_INSTRUCTIONS", "impact": "medium", "source": DET,
            "step": "Add text instructions defining key terms, the grain (one row = ?), and scope.",
            "why": "SQL examples exist but no prose defines the domain terms, so ambiguous questions resolve wrongly.",
            "evidence": f"text_instructions={txti}"})
    elif sqli == 0:
        steps.append({
            "category": "NO_SQL_EXAMPLES", "impact": "medium", "source": DET,
            "step": "Add curated SQL example queries (question → ground-truth SQL) covering your common questions.",
            "why": "Text instructions exist but there are no worked SQL examples for Genie to pattern-match against.",
            "evidence": f"sql_instructions={sqli}"})
    if cov is not None and cov < 100:
        steps.append({
            "category": "LOW_COVERAGE", "impact": "medium", "source": DET,
            "step": "Run the full benchmark set, not a subset.",
            "why": "Only part of the defined benchmark was run, so accuracy is not fully proven.",
            "evidence": f"coverage_pct={cov}"})
    if acc is not None and cov is not None and cov >= 100 and acc < LOW_ACCURACY_THRESHOLD:
        steps.append({
            "category": "LOW_ACCURACY", "impact": "high", "source": DET,
            "step": "Investigate failing benchmark traces (see BAD assessments) and fix the underlying instructions or ground truth.",
            "why": f"Benchmark pass rate is {acc}% on full coverage.",
            "evidence": f"benchmark_quality_pct={acc}, coverage_pct={cov}"})
    if fbpos is not None and fbpos < LOW_FEEDBACK_THRESHOLD and tdown > 0:
        steps.append({
            "category": "NEGATIVE_FEEDBACK", "impact": "medium", "source": DET,
            "step": "Triage negative feedback into instruction or benchmark fixes.",
            "why": f"Only {fbpos}% of rated messages are positive ({tdown} thumbs-down).",
            "evidence": f"feedback_positive_pct={fbpos}, thumbs_down={tdown}"})
    return steps


# --------------------------------------------------------------------------
# LLM findings — the two things that genuinely require reasoning.
# --------------------------------------------------------------------------
LLM_SYSTEM = """You are the Genie Doctor's reasoning module. You are given ONE Genie space's
curation: its text/SQL instructions, its benchmark questions with ground-truth SQL, its
benchmark trace results (expected vs actual SQL), and the table schema.

Find ONLY these two problem types — nothing else:
  CONFLICTING_INSTRUCTIONS: two instructions define the same concept, filter, or grain in
    incompatible ways (e.g. a denial defined as status_label='Denied' in one and
    reason_code IN (...) in another).
  BAD_GROUND_TRUTH: a benchmark's expected SQL is itself wrong — it references a column NOT
    in the schema, or its filter/grain contradicts an instruction's definition, or it does
    not answer the question.

Rules:
- Recommend a step ONLY if you can cite the exact instruction_id(s) or benchmark_question_id
  it is based on. No generic advice. If you cannot cite it, do not say it.
- Do NOT report missing benchmarks, empty instructions, coverage, accuracy, or feedback —
  those are handled elsewhere.
Respond with STRICT JSON only: {"steps":[{"category","step","why","evidence","impact"}]}.
evidence MUST contain the real instruction_id / benchmark_question_id you used. impact is
high|medium|low. If nothing qualifies, return {"steps":[]}."""


def _bundle_for_llm(ev: dict) -> str:
    keep = {
        "schema": ev.get("schema", {}),
        "instructions": [{k: i.get(k) for k in ("instruction_id", "instruction_type", "title", "content")}
                         for i in ev.get("instructions", [])],
        "benchmark_questions": [{k: q.get(k) for k in ("benchmark_question_id", "question_text", "answer_text")}
                                for q in ev.get("benchmark_questions", [])],
        "benchmark_results": [{k: r.get(k) for k in ("benchmark_question_id", "question", "assessment", "expected_sql", "actual_sql")}
                              for r in ev.get("benchmark_results", [])],
    }
    return json.dumps(keep, indent=1)


def _valid_ids(ev: dict) -> set:
    ids = {i.get("instruction_id") for i in ev.get("instructions", [])}
    ids |= {q.get("benchmark_question_id") for q in ev.get("benchmark_questions", [])}
    ids |= {r.get("benchmark_question_id") for r in ev.get("benchmark_results", [])}
    return {x for x in ids if x}


def llm_findings(ev: dict, query_fn) -> list:
    """query_fn(system, user) -> raw text. Kept injectable so the eval harness and the
    pipeline can supply their own model client."""
    raw = query_fn(LLM_SYSTEM, _bundle_for_llm(ev))
    m = re.search(r"\{.*\}", raw or "", re.DOTALL)
    if not m:
        return []
    try:
        steps = json.loads(m.group(0)).get("steps", [])
    except Exception:
        return []
    # GROUNDING CONTRACT: keep only steps that cite an id present in the bundle.
    valid = _valid_ids(ev)
    kept = []
    for s in steps:
        if s.get("category") not in ("CONFLICTING_INSTRUCTIONS", "BAD_GROUND_TRUTH"):
            continue
        ev_str = str(s.get("evidence", ""))
        if any(vid in ev_str for vid in valid):
            s["source"] = LLM
            kept.append(s)
    return kept


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------
_RANK = {"high": 0, "medium": 1, "low": 2}


def diagnose(ev: dict, query_fn=None) -> list:
    steps = deterministic_findings(ev.get("metrics", {}))
    if query_fn is not None:
        steps += llm_findings(ev, query_fn)
    steps.sort(key=lambda s: _RANK.get(s.get("impact", "low"), 3))
    return steps

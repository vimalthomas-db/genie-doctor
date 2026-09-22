"""Genie Doctor — batch diagnosis task (self-contained; no sibling import).

For each space: assemble an evidence bundle from gold + bronze, run the grounded
diagnostic agent, and write ranked improvement steps to {catalog}.{schema}.space_improvements.
Diagnostic logic is INLINED (spark_python_task runs via exec with no __file__, so a
sibling `import diagnose` is not reliable). Keep in sync with agent/diagnose.py.

No verdict / no grade. Deterministic findings are computed in code; the LLM reasons ONLY
on CONFLICTING_INSTRUCTIONS + BAD_GROUND_TRUTH, and every step must cite real evidence.
"""
import argparse
import json
import re
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType)

MODEL = "databricks-claude-sonnet-4-5"
LOW_ACCURACY_THRESHOLD = 80.0
LOW_FEEDBACK_THRESHOLD = 50.0
DET, LLM = "auto", "agent"
_RANK = {"high": 0, "medium": 1, "low": 2}


def _n(v):
    return None if v is None else float(v)


def deterministic_findings(m):
    s = []
    bdef = int(m.get("benchmark_questions_defined") or 0)
    runs = int(m.get("num_runs") or 0)
    sqli = int(m.get("sql_instructions") or 0)
    txti = int(m.get("text_instructions") or 0)
    cov, acc = _n(m.get("coverage_pct")), _n(m.get("benchmark_quality_pct"))
    fbpos = _n(m.get("feedback_positive_pct"))
    tdown = int(m.get("thumbs_down") or 0)
    if bdef == 0 or runs == 0:
        step = ("Define 3–5 benchmark questions from your top real user questions, attach a "
                "verified ground-truth SQL answer to each, then run an eval to measure accuracy.")
        if sqli > 0:
            step += f" You already have {sqli} curated SQL example(s) — promote the key ones into benchmark questions."
        s.append(("NO_BENCHMARK", "high", DET, step,
                  "Accuracy cannot be measured or trusted until a benchmark with ground truth is defined and run.",
                  f"benchmark_questions_defined={bdef}, num_runs={runs}, sql_instructions={sqli}"))
    if txti == 0 and sqli == 0:
        s.append(("EMPTY_INSTRUCTIONS", "high", DET,
                  "This space has no curation at all — add text instructions (domain, grain, key-term definitions) AND curated SQL example queries.",
                  "With no instructions Genie has no guidance, so it guesses table/column meaning and answers inaccurately.",
                  f"text_instructions={txti}, sql_instructions={sqli}"))
    elif txti == 0:
        s.append(("EMPTY_INSTRUCTIONS", "medium", DET,
                  "Add text instructions defining key terms, the grain (one row = ?), and scope.",
                  "SQL examples exist but no prose defines the domain terms, so ambiguous questions resolve wrongly.",
                  f"text_instructions={txti}"))
    elif sqli == 0:
        s.append(("NO_SQL_EXAMPLES", "medium", DET,
                  "Add curated SQL example queries (question → ground-truth SQL) covering your common questions.",
                  "Text instructions exist but there are no worked SQL examples for Genie to pattern-match against.",
                  f"sql_instructions={sqli}"))
    if cov is not None and cov < 100:
        s.append(("LOW_COVERAGE", "medium", DET, "Run the full benchmark set, not a subset.",
                  "Only part of the defined benchmark was run, so accuracy is not fully proven.", f"coverage_pct={cov}"))
    if acc is not None and cov is not None and cov >= 100 and acc < LOW_ACCURACY_THRESHOLD:
        s.append(("LOW_ACCURACY", "high", DET, "Investigate failing benchmark traces and fix the underlying instructions or ground truth.",
                  f"Benchmark pass rate is {acc}% on full coverage.", f"benchmark_quality_pct={acc}, coverage_pct={cov}"))
    if fbpos is not None and fbpos < LOW_FEEDBACK_THRESHOLD and tdown > 0:
        s.append(("NEGATIVE_FEEDBACK", "medium", DET, "Triage negative feedback into instruction or benchmark fixes.",
                  f"Only {fbpos}% of rated messages are positive ({tdown} thumbs-down).", f"feedback_positive_pct={fbpos}, thumbs_down={tdown}"))
    return [{"category": c, "impact": i, "source": src, "step": st, "why": w, "evidence": e} for (c, i, src, st, w, e) in s]


LLM_SYSTEM = """You are the Genie Doctor's reasoning module. You are given ONE Genie space's
curation: text/SQL instructions, benchmark questions with ground-truth SQL, benchmark trace
results (expected vs actual SQL), and the table schema.
Find ONLY these two problem types:
  CONFLICTING_INSTRUCTIONS: two instructions define the same concept/filter/grain incompatibly.
  BAD_GROUND_TRUTH: a benchmark's expected SQL is itself wrong (references a column NOT in the
    schema, contradicts an instruction definition, or does not answer the question).
Rules: recommend a step ONLY if you cite the exact instruction_id or benchmark_question_id it
is based on. No generic advice. Do NOT report missing benchmarks / empty instructions /
coverage / accuracy / feedback (handled elsewhere).
Respond with STRICT JSON only: {"steps":[{"category","step","why","evidence","impact"}]}.
evidence MUST contain the real id used; impact is high|medium|low. Nothing qualifies -> {"steps":[]}."""


def _valid_ids(ev):
    ids = {i.get("instruction_id") for i in ev.get("instructions", [])}
    ids |= {q.get("benchmark_question_id") for q in ev.get("benchmark_questions", [])}
    ids |= {r.get("benchmark_question_id") for r in ev.get("benchmark_results", [])}
    return {x for x in ids if x}


def llm_findings(ev, qfn):
    bundle = json.dumps({
        "schema": ev.get("schema", {}),
        "instructions": ev.get("instructions", []),
        "benchmark_questions": ev.get("benchmark_questions", []),
        "benchmark_results": ev.get("benchmark_results", []),
    }, indent=1)
    try:
        raw = qfn(LLM_SYSTEM, bundle)
    except Exception as e:
        print(f"[llm] error: {str(e)[:100]}"); return []
    m = re.search(r"\{.*\}", raw or "", re.DOTALL)
    if not m:
        return []
    try:
        steps = json.loads(m.group(0)).get("steps", [])
    except Exception:
        return []
    valid, kept = _valid_ids(ev), []
    for s in steps:
        if s.get("category") not in ("CONFLICTING_INSTRUCTIONS", "BAD_GROUND_TRUTH"):
            continue
        if any(vid in str(s.get("evidence", "")) for vid in valid):
            s["source"] = LLM
            kept.append(s)
    return kept


def diagnose(ev, qfn):
    steps = deterministic_findings(ev.get("metrics", {})) + llm_findings(ev, qfn)
    steps.sort(key=lambda s: _RANK.get(s.get("impact", "low"), 3))
    return steps


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    return p.parse_args()


def main():
    a = parse_args()
    cat, sch = a.catalog, a.schema
    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()

    def qfn(system, user):
        r = w.serving_endpoints.query(
            name=MODEL, max_tokens=1500,
            messages=[ChatMessage(role=ChatMessageRole.SYSTEM, content=system),
                      ChatMessage(role=ChatMessageRole.USER, content=user)])
        return r.choices[0].message.content

    scorecard = spark.read.table(f"{cat}.{sch}.space_scorecard").collect()
    instr = spark.read.table(f"{cat}.{sch}.genie_instructions").collect()
    cq = spark.read.table(f"{cat}.{sch}.genie_curated_qs").collect()
    try:
        results = spark.read.table(f"{cat}.{sch}.genie_eval_results").collect()
    except Exception:
        results = []

    def rows_for(rs, sid):
        return [r.asDict() for r in rs if r["space_id"] == sid]

    out, gen_at = [], datetime.now(timezone.utc)
    for sc in scorecard:
        sid = sc["space_id"]
        scd = sc.asDict()
        ev = {
            "space_id": sid, "title": sc["title"],
            "metrics": {k: scd.get(k) for k in (
                "benchmark_questions_defined", "num_runs", "coverage_pct", "benchmark_quality_pct",
                "sql_instructions", "text_instructions", "feedback_positive_pct", "thumbs_down", "msgs")},
            "instructions": [{"instruction_id": r["instruction_id"], "instruction_type": r["instruction_type"],
                              "title": r["title"], "content": r["content"]} for r in rows_for(instr, sid)],
            "benchmark_questions": [{"benchmark_question_id": r["curated_question_id"],
                                     "question_text": r["question_text"], "answer_text": r["answer_text"]}
                                    for r in rows_for(cq, sid) if r.get("question_type") == "BENCHMARK"],
            "benchmark_results": [{"benchmark_question_id": r["benchmark_question_id"], "question": r["question"],
                                   "assessment": r["assessment"], "expected_sql": r["expected_response"],
                                   "actual_sql": r["actual_response"]} for r in rows_for(results, sid)],
        }
        try:
            steps = diagnose(ev, qfn)
        except Exception as e:
            print(f"[diagnose] {sid} error: {str(e)[:120]}"); steps = []
        for i, s in enumerate(steps, 1):
            out.append((sid, sc["title"], i, s.get("category"), s.get("impact"),
                        s.get("source"), s.get("step"), s.get("why"), str(s.get("evidence")), gen_at))
        print(f"[diagnose] {sc['title']}: {len(steps)} steps")

    schema = StructType([
        StructField("space_id", StringType()), StructField("title", StringType()),
        StructField("rank", IntegerType()), StructField("category", StringType()),
        StructField("impact", StringType()), StructField("source", StringType()),
        StructField("step", StringType()), StructField("why", StringType()),
        StructField("evidence", StringType()), StructField("generated_at", TimestampType())])
    (spark.createDataFrame(out, schema).write.mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(f"{cat}.{sch}.space_improvements"))
    print(f"[diagnose] wrote {len(out)} steps -> {cat}.{sch}.space_improvements")


if __name__ == "__main__":
    main()

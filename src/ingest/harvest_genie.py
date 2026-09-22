"""Genie Doctor — BRONZE ingestion for curation + benchmark eval-runs.

Runs as a job task (spark_python_task). Declarative pipelines can't poll arbitrary
REST, so this lands raw bronze; the SDP pipeline transforms bronze -> gold.

Owner-scoped: only spaces whose parent_path starts with --owner_prefix.

Writes to {catalog}.{schema} (overwrite each run — small, fully re-derivable):
  genie_eval_runs    space_id, eval_run_id, eval_run_status, num_questions, num_correct,
                     num_needs_review, num_done, created_timestamp, run_by_user
  genie_instructions space_id, instruction_id, instruction_type, title, content,
                     usage_guidance, instruction_status, use_as_tool
  genie_curated_qs   space_id, curated_question_id, question_type, question_text,
                     answer_text, eval_note, is_deprecated
  genie_space_meta   space_id, title, parent_path, warehouse_id, run_as_type,
                     last_updated_timestamp, table_identifiers (array<string>)

Sources (verified live 2026-09-02):
  GET /api/2.0/genie/spaces[/{id}]                  discovery + parent_path (owner)
  GET /api/2.0/genie/spaces/{id}/eval-runs          executed benchmark runs (CAN_MANAGE)
  GET /api/2.0/data-rooms/{id}                      table_identifiers, meta  (legacy)
  GET /api/2.0/data-rooms/{id}/instructions         SQL_/TEXT_ instructions  (legacy)
  GET /api/2.0/data-rooms/{id}/curated-questions    BENCHMARK + SAMPLE_QUESTION (legacy)
"""
import argparse
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, LongType,
                               IntegerType, BooleanType, ArrayType)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--owner_prefix", required=True)
    p.add_argument("--warehouse_id", required=False, default="")
    return p.parse_args()


def get(w, path):
    try:
        return w.api_client.do("GET", path) or {}
    except Exception as e:
        return {"__err__": str(e)[:120]}


def list_owned_spaces(w, owner_prefix):
    spaces, token = [], None
    while True:
        q = f"?page_token={token}" if token else ""
        resp = get(w, f"/api/2.0/genie/spaces{q}")
        spaces.extend(resp.get("spaces", []) or [])
        token = resp.get("next_page_token")
        if not token:
            break

    def owned(s):
        d = get(w, f"/api/2.0/genie/spaces/{s['space_id']}")
        pp = d.get("parent_path") or ""
        return {**s, **d} if pp.startswith(owner_prefix) else None

    with ThreadPoolExecutor(max_workers=8) as ex:
        return [s for s in ex.map(owned, spaces) if s]


def collect(w, sid):
    er = get(w, f"/api/2.0/genie/spaces/{sid}/eval-runs").get("eval_runs", []) or []
    ins = get(w, f"/api/2.0/data-rooms/{sid}/instructions").get("instructions", []) or []
    cq = get(w, f"/api/2.0/data-rooms/{sid}/curated-questions").get("curated_questions", []) or []
    meta = get(w, f"/api/2.0/data-rooms/{sid}")
    return sid, er, ins, cq, meta


def collect_eval_results(w, sid, runs):
    """Level-3: per-question GOOD/BAD + expected vs actual SQL for the LATEST run.
    (Latest run reflects current accuracy; bounded API calls.)"""
    if not runs:
        return []
    latest = max(runs, key=lambda r: r.get("created_timestamp", 0))
    rid = latest.get("eval_run_id")
    lst = get(w, f"/api/2.0/genie/spaces/{sid}/eval-runs/{rid}/results").get("eval_results", []) or []
    rows = []
    for item in lst:
        result_id = item.get("result_id")
        det = get(w, f"/api/2.0/genie/spaces/{sid}/eval-runs/{rid}/results/{result_id}")
        rows.append((sid, rid, item.get("benchmark_question_id"), item.get("question"),
                     det.get("assessment"), str(det.get("expected_response"))[:6000],
                     str(det.get("actual_response"))[:6000], result_id))
    return rows


def _i(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def main():
    args = parse_args()
    cat, sch = args.catalog, args.schema
    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()

    owned = list_owned_spaces(w, args.owner_prefix)
    print(f"[harvest] {len(owned)} owner spaces under {args.owner_prefix}")

    eval_rows, instr_rows, cq_rows, meta_rows, result_rows = [], [], [], [], []
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(lambda s: collect(w, s["space_id"]), owned))
    by_id = {s["space_id"]: s for s in owned}

    # Level-3 per-question results for spaces that have runs (concurrent).
    with ThreadPoolExecutor(max_workers=8) as ex:
        for rows in ex.map(lambda t: collect_eval_results(w, t[0], t[1]),
                           [(sid, er) for sid, er, *_ in results]):
            result_rows.extend(rows)

    for sid, er, ins, cq, meta in results:
        for r in er:
            eval_rows.append((sid, r.get("eval_run_id"), r.get("eval_run_status"),
                              _i(r.get("num_questions")), _i(r.get("num_correct")),
                              _i(r.get("num_needs_review")), _i(r.get("num_done")),
                              _i(r.get("created_timestamp")), r.get("run_by_user")))
        for r in ins:
            instr_rows.append((sid, r.get("instruction_id"), r.get("instruction_type"),
                               r.get("title"), r.get("content"), r.get("usage_guidance"),
                               r.get("instruction_status"), str(r.get("use_as_tool"))))
        for r in cq:
            cq_rows.append((sid, r.get("curated_question_id"), r.get("question_type"),
                            r.get("question_text"), r.get("answer_text"),
                            r.get("eval_note"), str(r.get("is_deprecated")).lower() == "true"))
        base = by_id.get(sid, {})
        meta_rows.append((sid, base.get("title"), base.get("parent_path"),
                          base.get("warehouse_id"), meta.get("run_as_type"),
                          _i(meta.get("last_updated_timestamp")),
                          [str(t) for t in (meta.get("table_identifiers") or [])]))

    eval_schema = StructType([
        StructField("space_id", StringType()), StructField("eval_run_id", StringType()),
        StructField("eval_run_status", StringType()), StructField("num_questions", IntegerType()),
        StructField("num_correct", IntegerType()), StructField("num_needs_review", IntegerType()),
        StructField("num_done", IntegerType()), StructField("created_timestamp", LongType()),
        StructField("run_by_user", StringType())])
    instr_schema = StructType([
        StructField("space_id", StringType()), StructField("instruction_id", StringType()),
        StructField("instruction_type", StringType()), StructField("title", StringType()),
        StructField("content", StringType()), StructField("usage_guidance", StringType()),
        StructField("instruction_status", StringType()), StructField("use_as_tool", StringType())])
    cq_schema = StructType([
        StructField("space_id", StringType()), StructField("curated_question_id", StringType()),
        StructField("question_type", StringType()), StructField("question_text", StringType()),
        StructField("answer_text", StringType()), StructField("eval_note", StringType()),
        StructField("is_deprecated", BooleanType())])
    meta_schema = StructType([
        StructField("space_id", StringType()), StructField("title", StringType()),
        StructField("parent_path", StringType()), StructField("warehouse_id", StringType()),
        StructField("run_as_type", StringType()), StructField("last_updated_timestamp", LongType()),
        StructField("table_identifiers", ArrayType(StringType()))])
    result_schema = StructType([
        StructField("space_id", StringType()), StructField("eval_run_id", StringType()),
        StructField("benchmark_question_id", StringType()), StructField("question", StringType()),
        StructField("assessment", StringType()), StructField("expected_response", StringType()),
        StructField("actual_response", StringType()), StructField("result_id", StringType())])

    def write(rows, schema, name):
        (spark.createDataFrame(rows, schema).write.mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(f"{cat}.{sch}.{name}"))
        print(f"[harvest] wrote {len(rows):>4} rows -> {cat}.{sch}.{name}")

    write(eval_rows, eval_schema, "genie_eval_runs")
    write(instr_rows, instr_schema, "genie_instructions")
    write(cq_rows, cq_schema, "genie_curated_qs")
    write(meta_rows, meta_schema, "genie_space_meta")
    write(result_rows, result_schema, "genie_eval_results")
    print("[harvest] done.")


if __name__ == "__main__":
    main()

# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Doctor — Batch Compute
# MAGIC Harvest-adjacent compute: factors + eval-run benchmarks + LLM verdicts + grade/score,
# MAGIC written to three Delta result tables that Lakebase mirrors for the app to serve fast.
# MAGIC
# MAGIC Writes to `vjoseph_pbi_demo.genie_monitoring`:
# MAGIC - `genie_doctor_scores`   (snapshot per run — one row per space per snapshot_date)
# MAGIC - `genie_doctor_benchmarks` (one row per eval-run)
# MAGIC - `genie_doctor_trends`   (daily time series per space)

# COMMAND ----------

import time
import datetime as dt
import pandas as pd
from databricks.sdk import WorkspaceClient

CATALOG = "vjoseph_pbi_demo"
SCHEMA = "genie_monitoring"
MODEL = "databricks-claude-sonnet-4-5"
WORKSPACE_LABEL = "adb-984752964297111 · Pulse"
FQ = f"{CATALOG}.{SCHEMA}"

SPACE_IDS = [
    "01f19b18a773106fb17036f173ff295f",  # ELV CDIP Member Abrasion (Pilot)
    "01f144b020641fc687ad499c406acdf4",  # Care Delivery Metrics
    "01f19da2f7991a57bb76e27a3a4892a1",  # Rush Road Home — Crown Foundation Metrics
    "01f1903b3fbb12dbad4a99689341c594",  # Healthcare Operations Analytics
    "01f159f1fdd21d338a6c31452502f0a3",  # New Space
    "01f15f51902e196d9e6a2853fb336611",  # Healthcare Appointment Analytics
    "01f168cfc5f51625817019e7d3fb59b8",  # Cost Trend Drivers — Moving Average Demo
    "01f19dad34451398a2715d3aaa4e88db",  # Banner Health — System Executive Scorecard
    "01f155de5bc415b7bfbe5d23bde9fa01",  # ACT_Renown_Demo
    "01f137acbaf91682accafca6be13458b",  # PBI Retail Demo - Metric Views Dashboard
    "01f16364bee41fd9949ba9dee9dc888b",  # Pulse Semantic Foundation
]
TITLE_FALLBACK = {
    "01f19b18a773106fb17036f173ff295f": "ELV CDIP Member Abrasion (Pilot)",
    "01f144b020641fc687ad499c406acdf4": "Care Delivery Metrics",
    "01f19da2f7991a57bb76e27a3a4892a1": "Rush Road Home — Crown Foundation Metrics",
    "01f1903b3fbb12dbad4a99689341c594": "Healthcare Operations Analytics",
    "01f159f1fdd21d338a6c31452502f0a3": "New Space",
    "01f15f51902e196d9e6a2853fb336611": "Healthcare Appointment Analytics",
    "01f168cfc5f51625817019e7d3fb59b8": "Cost Trend Drivers — Moving Average Demo",
    "01f19dad34451398a2715d3aaa4e88db": "Banner Health — System Executive Scorecard",
    "01f155de5bc415b7bfbe5d23bde9fa01": "ACT_Renown_Demo",
    "01f137acbaf91682accafca6be13458b": "PBI Retail Demo - Metric Views Dashboard",
    "01f16364bee41fd9949ba9dee9dc888b": "Pulse Semantic Foundation — Unified Healthcare Analytics",
}
GRADE_SCORE = {"A": 92, "B": 80, "C": 66, "D": 48, "F": 22}
IN = ",".join(f"'{s}'" for s in SPACE_IDS)
w = WorkspaceClient()

# COMMAND ----------
# MAGIC %md ## 1. Space universe (title + warehouse_id)

universe = {}
try:
    rows = spark.sql(f"""
        SELECT space_id, title, warehouse_id FROM {FQ}.genie_spaces
        WHERE space_id IN ({IN})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY space_id ORDER BY harvested_at DESC)=1
    """).collect()
    for r in rows:
        universe[r["space_id"]] = {"title": r["title"], "warehouse_id": r["warehouse_id"]}
except Exception as e:
    print("universe query failed:", e)

for sid in SPACE_IDS:
    if sid in universe and universe[sid].get("title"):
        continue
    fb = TITLE_FALLBACK.get(sid, f"Space {sid[:8]}")
    try:
        d = w.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}") or {}
        universe[sid] = {"title": d.get("title") or fb, "warehouse_id": d.get("warehouse_id")}
    except Exception:
        universe.setdefault(sid, {"title": fb, "warehouse_id": None})
print(universe)

# COMMAND ----------
# MAGIC %md ## 2. Deterministic factors

factors = {r["space_id"]: r.asDict() for r in spark.sql(f"""
    SELECT space_id,
      COUNT(*) AS msgs,
      COUNT(DISTINCT user_id) AS users,
      ROUND(100.0*SUM(CASE WHEN status<>'COMPLETED' OR error_type IS NOT NULL
            THEN 1 ELSE 0 END)/COUNT(*),1) AS error_pct,
      ROUND(approx_percentile(response_time_sec,0.9),1) AS p90_latency_s,
      SUM(CASE WHEN feedback_rating='POSITIVE' THEN 1 ELSE 0 END) AS thumbs_up,
      SUM(CASE WHEN feedback_rating='NEGATIVE' THEN 1 ELSE 0 END) AS thumbs_down,
      ROUND(100.0*SUM(CASE WHEN feedback_rating IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS fb_coverage_pct,
      date_diff(current_date(), CAST(MAX(created_ts) AS DATE)) AS days_since_active
    FROM {FQ}.genie_message_details WHERE space_id IN ({IN}) GROUP BY space_id
""").collect()}
print(f"{len(factors)} spaces have activity")

# COMMAND ----------
# MAGIC %md ## 3. Warehouse health + eval-run benchmarks

existing_wh = set()
try:
    existing_wh = {wh.id for wh in w.warehouses.list()}
except Exception as e:
    print("warehouse list failed:", e)

def warehouse_ok(wid):
    if not wid:
        return True  # unknown -> don't flag
    if wid in existing_wh:
        return True
    try:
        w.warehouses.get(wid)
        return True
    except Exception as e:
        m = str(e).upper()
        bad = any(k in m for k in ("DELETED", "DOES NOT EXIST", "NOT_FOUND", "404"))
        return not bad

def eval_runs(sid):
    try:
        resp = w.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}/eval-runs")
        return (resp or {}).get("eval_runs") or []
    except Exception:
        return []

def bench_metrics(runs):
    if not runs:
        return dict(benchmark_ran=False, num_runs=0, accuracy_pct=None, coverage_pct=None,
                    full_benchmark_size=0, num_questions=0, num_correct=0, num_needs_review=0,
                    num_failed=0, confidence=None)
    done_of = lambda r: r.get("num_done") if r.get("num_done") is not None else (r.get("num_questions") or 0)
    latest = max(runs, key=lambda r: r.get("created_timestamp", 0))
    full = max((r.get("num_questions") or 0) for r in runs) or 0
    nq = latest.get("num_questions") or 0
    nc = latest.get("num_correct") or 0
    nr = latest.get("num_needs_review") or 0
    nd = done_of(latest)
    failed = max(nd - nc - nr, 0)
    coverage = round(100.0 * nd / nq, 1) if nq else 0.0
    accuracy = round(100.0 * nc / full, 1) if full else None
    ts = latest.get("created_timestamp", 0)
    days = (time.time() * 1000 - ts) / 86_400_000 if ts else 999
    partial = nq < full
    confidence = "high" if (coverage >= 100 and not partial and days <= 30) else "low"
    return dict(benchmark_ran=True, num_runs=len(runs), accuracy_pct=accuracy,
                coverage_pct=coverage, full_benchmark_size=full, num_questions=nq,
                num_correct=nc, num_needs_review=nr, num_failed=failed, confidence=confidence)

# COMMAND ----------
# MAGIC %md ## 4. LLM verdict (ai_query) + assemble scores

VERDICT_PROMPT = (
    "You are Genie Doctor, an auditor of Databricks Genie spaces for a quality/admin team. "
    "Rank trustworthiness. CRITICAL RULE: benchmark accuracy (are answers actually correct?) "
    "outweighs usage, completion, or no-errors -- completion only means the query did not crash, "
    "NOT that the answer was right. No benchmark coverage on a high-traffic space is itself a "
    "serious risk. Accuracy is measured against the FULL defined benchmark set; LOW coverage or "
    "LOW confidence means the score is UNPROVEN even if it looks high, so treat a high accuracy "
    "with low confidence as a risk and tell them to run the full benchmark. Given ONE space, "
    "respond in EXACTLY this format on one line: "
    "'GRADE: <A-F> | WHY: <=20 words | FIX: <=15 words'. "
    "Space=\"{title}\"; questions_asked={msgs}; distinct_users={users}; error_rate_pct={err}; "
    "p90_latency_sec={p90}; thumbs_up={tu}; thumbs_down={td}; feedback_coverage_pct={fb}; "
    "days_since_last_use={days}; benchmark_accuracy={acc}; benchmark_runs={runs}; "
    "benchmark_coverage_pct={cov}; benchmark_confidence={conf}; full_benchmark_size={full}."
)

def ai_verdict(prompt):
    esc = prompt.replace("'", "''")
    r = spark.sql(f"SELECT ai_query('{MODEL}', '{esc}') AS v").collect()
    raw = (r[0]["v"] or "") if r else ""
    import re
    m = re.search(r"GRADE:\s*([A-F])\s*\|\s*WHY:\s*(.*?)\s*\|\s*FIX:\s*(.*)", raw, re.I | re.S)
    if m:
        return m.group(1).upper(), m.group(2).strip(), m.group(3).strip()
    return "?", raw.strip(), ""

score_rows, bench_rows = [], []
for sid in SPACE_IDS:
    info = universe.get(sid, {"title": TITLE_FALLBACK.get(sid, sid), "warehouse_id": None})
    wid = info.get("warehouse_id")
    f = factors.get(sid) or {}
    msgs = int(f.get("msgs") or 0)
    base = dict(msgs=msgs, users=int(f.get("users") or 0), error_pct=float(f.get("error_pct") or 0),
                p90_latency_s=float(f.get("p90_latency_s") or 0), thumbs_up=int(f.get("thumbs_up") or 0),
                thumbs_down=int(f.get("thumbs_down") or 0), fb_coverage_pct=float(f.get("fb_coverage_pct") or 0),
                days_since_active=int(f.get("days_since_active") or 0))
    runs = eval_runs(sid)
    b = bench_metrics(runs)
    wh_ok = warehouse_ok(wid)

    # collect per-run benchmark rows
    for r in runs:
        nq = r.get("num_questions") or 0
        nc = r.get("num_correct") or 0
        nr = r.get("num_needs_review") or 0
        nd = r.get("num_done") if r.get("num_done") is not None else nq
        bench_rows.append(dict(space_id=sid, title=info["title"], run_id=r.get("eval_run_id"),
            run_ts=(dt.datetime.fromtimestamp((r.get("created_timestamp") or 0)/1000) if r.get("created_timestamp") else None),
            num_questions=nq, num_correct=nc, num_failed=max(nd-nc-nr, 0), num_needs_review=nr,
            coverage=(round(100.0*nd/nq, 1) if nq else 0.0),
            accuracy=(round(100.0*nc/(b["full_benchmark_size"] or 1), 1)),
            run_status=r.get("eval_run_status")))

    # grade
    if not wh_ok:
        grade, why, fix = "F", "SQL warehouse deleted — space is non-functional", "Reassign an existing SQL warehouse in space Settings"
    elif msgs == 0:
        grade, why, fix = "—", "No activity harvested yet — nothing to judge.", "Drive usage or run an eval benchmark."
    else:
        prompt = VERDICT_PROMPT.format(title=info["title"], msgs=msgs, users=base["users"],
            err=base["error_pct"], p90=base["p90_latency_s"], tu=base["thumbs_up"], td=base["thumbs_down"],
            fb=base["fb_coverage_pct"], days=base["days_since_active"],
            acc=(f'{b["accuracy_pct"]}%' if b["accuracy_pct"] is not None else "NO BENCHMARK RUN"),
            runs=b["num_runs"], cov=b["coverage_pct"], conf=(b["confidence"] or "n/a"), full=b["full_benchmark_size"])
        grade, why, fix = ai_verdict(prompt)

    score_rows.append(dict(space_id=sid, title=info["title"], workspace=WORKSPACE_LABEL,
        warehouse_id=wid, warehouse_ok=wh_ok, grade=grade, score=GRADE_SCORE.get(grade),
        verdict_why=why, verdict_fix=fix, **base, **b))
    print(f"{grade:2} {info['title'][:42]:42} acc={b['accuracy_pct']} runs={b['num_runs']} wh_ok={wh_ok}")

# COMMAND ----------
# MAGIC %md ## 5. Write result Delta tables

today = dt.date.today()
scores_pd = pd.DataFrame(score_rows)
scores_pd.insert(0, "snapshot_date", today)
scores_pd["updated_at"] = dt.datetime.now()

sdf = (spark.createDataFrame(scores_pd)
       .withColumn("snapshot_date", __import__("pyspark").sql.functions.col("snapshot_date").cast("date")))
sdf.createOrReplaceTempView("scores_src")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.genie_doctor_scores (
  snapshot_date DATE, space_id STRING, title STRING, workspace STRING, warehouse_id STRING,
  warehouse_ok BOOLEAN, grade STRING, score DOUBLE, verdict_why STRING, verdict_fix STRING,
  msgs BIGINT, users BIGINT, error_pct DOUBLE, p90_latency_s DOUBLE, thumbs_up BIGINT,
  thumbs_down BIGINT, fb_coverage_pct DOUBLE, days_since_active BIGINT, benchmark_ran BOOLEAN,
  num_runs BIGINT, accuracy_pct DOUBLE, coverage_pct DOUBLE, full_benchmark_size BIGINT,
  num_questions BIGINT, num_correct BIGINT, num_needs_review BIGINT, num_failed BIGINT,
  confidence STRING, updated_at TIMESTAMP) USING DELTA""")
spark.sql(f"DELETE FROM {FQ}.genie_doctor_scores WHERE snapshot_date = '{today}'")
spark.sql(f"""INSERT INTO {FQ}.genie_doctor_scores
  (snapshot_date, space_id, title, workspace, warehouse_id, warehouse_ok, grade, score,
   verdict_why, verdict_fix, msgs, users, error_pct, p90_latency_s, thumbs_up, thumbs_down,
   fb_coverage_pct, days_since_active, benchmark_ran, num_runs, accuracy_pct, coverage_pct,
   full_benchmark_size, num_questions, num_correct, num_needs_review, num_failed, confidence, updated_at)
  SELECT snapshot_date, space_id, title, workspace, warehouse_id, warehouse_ok, grade, score,
   verdict_why, verdict_fix, msgs, users, error_pct, p90_latency_s, thumbs_up, thumbs_down,
   fb_coverage_pct, days_since_active, benchmark_ran, num_runs, accuracy_pct, coverage_pct,
   full_benchmark_size, num_questions, num_correct, num_needs_review, num_failed, confidence, updated_at
  FROM scores_src""")
print("scores written:", spark.table(f"{FQ}.genie_doctor_scores").count())

# COMMAND ----------
# benchmarks (full overwrite — eval-run facts)
if bench_rows:
    bdf = spark.createDataFrame(pd.DataFrame(bench_rows))
    bdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.genie_doctor_benchmarks")
else:
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.genie_doctor_benchmarks (
      space_id STRING, title STRING, run_id STRING, run_ts TIMESTAMP, num_questions BIGINT,
      num_correct BIGINT, num_failed BIGINT, num_needs_review BIGINT, coverage DOUBLE,
      accuracy DOUBLE, run_status STRING) USING DELTA""")
print("benchmarks written:", spark.table(f"{FQ}.genie_doctor_benchmarks").count())

# COMMAND ----------
# trends (daily time series per space — full overwrite)
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.genie_doctor_trends USING DELTA AS
WITH msg AS (
  SELECT space_id, CAST(created_ts AS DATE) AS date,
    COUNT(*) AS messages,
    COUNT(DISTINCT user_id) AS users,
    ROUND(100.0*SUM(CASE WHEN status<>'COMPLETED' OR error_type IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS error_rate,
    SUM(CASE WHEN feedback_rating='POSITIVE' THEN 1 ELSE 0 END) AS feedback_pos,
    SUM(CASE WHEN feedback_rating='NEGATIVE' THEN 1 ELSE 0 END) AS feedback_neg,
    ROUND(100.0*SUM(CASE WHEN feedback_rating IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS feedback_cov,
    ROUND(approx_percentile(response_time_sec,0.9),1) AS p90_latency
  FROM {FQ}.genie_message_details WHERE space_id IN ({IN}) AND created_ts IS NOT NULL GROUP BY 1,2),
conv AS (
  SELECT space_id, CAST(created_ts AS DATE) AS date, COUNT(*) AS conversations
  FROM {FQ}.genie_conversations WHERE space_id IN ({IN}) AND created_ts IS NOT NULL GROUP BY 1,2)
SELECT COALESCE(m.space_id,c.space_id) AS space_id, COALESCE(m.date,c.date) AS date,
  COALESCE(c.conversations,0) AS conversations, COALESCE(m.messages,0) AS messages,
  COALESCE(m.users,0) AS users, m.error_rate, m.feedback_pos, m.feedback_neg,
  m.feedback_cov, m.p90_latency
FROM msg m FULL OUTER JOIN conv c ON m.space_id=c.space_id AND m.date=c.date""")
print("trends written:", spark.table(f"{FQ}.genie_doctor_trends").count())
print("DONE")

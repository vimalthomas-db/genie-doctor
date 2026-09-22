"""Genie Doctor — data layer.

SQL over the warehouse (Statement Execution API), Genie eval-runs REST API,
the LLM verdict (ai_query), warehouse-health, portfolio/agent trends, and a
lightweight score-history snapshot table.
"""

import os
import re
import time

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CATALOG = os.environ.get("CATALOG", "vjoseph_pbi_demo")
SCHEMA = os.environ.get("SCHEMA", "genie_monitoring")
MODEL = os.environ.get("MODEL", "databricks-claude-sonnet-4-5")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID") or os.environ.get("DATABRICKS_WAREHOUSE_ID")
WORKSPACE_LABEL = os.environ.get("WORKSPACE_LABEL", "adb-984752964297111 · Pulse")
SCORES_TABLE = f"{CATALOG}.{SCHEMA}.genie_doctor_scores"

DEFAULT_SPACE_IDS = [
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
SCOPE_SPACE_IDS = [s.strip() for s in os.environ.get("SCOPE_SPACE_IDS", "").split(",")
                   if s.strip()] or DEFAULT_SPACE_IDS

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


@st.cache_resource
def get_client() -> WorkspaceClient:
    return WorkspaceClient()


def _in_list(ids) -> str:
    return ",".join("'" + i.replace("'", "") + "'" for i in ids)


def run_sql(statement: str, parameters=None):
    w = get_client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, catalog=CATALOG,
        schema=SCHEMA, parameters=parameters, wait_timeout="50s")
    while resp.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1.5)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state.value != "SUCCEEDED":
        err = resp.status.error.message if resp.status.error else "unknown error"
        raise RuntimeError(f"SQL failed ({resp.status.state.value}): {err}")
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return [dict(zip(cols, r)) for r in rows]


# ---------------------------------------------------------------------------
# Space universe
# ---------------------------------------------------------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def get_space_universe() -> dict:
    uni = {}
    try:
        rows = run_sql(
            f"SELECT space_id, title, warehouse_id FROM {CATALOG}.{SCHEMA}.genie_spaces "
            f"WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)}) "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY space_id "
            f"ORDER BY harvested_at DESC) = 1")
        for r in rows:
            uni[r["space_id"]] = {"title": r["title"], "warehouse_id": r["warehouse_id"]}
    except Exception:
        pass
    w = get_client()
    for sid in SCOPE_SPACE_IDS:
        if sid in uni and uni[sid].get("title"):
            continue
        fb = TITLE_FALLBACK.get(sid, f"Space {sid[:8]}…")
        try:
            d = w.api_client.do("GET", f"/api/2.0/genie/spaces/{sid}") or {}
            uni[sid] = {"title": d.get("title") or fb, "warehouse_id": d.get("warehouse_id")}
        except Exception:
            uni.setdefault(sid, {"title": fb, "warehouse_id": None})
    return uni


# ---------------------------------------------------------------------------
# Deterministic factors
# ---------------------------------------------------------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def get_factors_map() -> dict:
    sql = f"""
    SELECT space_id,
      COUNT(*) AS msgs,
      COUNT(DISTINCT user_id) AS users,
      ROUND(100.0*SUM(CASE WHEN status<>'COMPLETED' OR error_type IS NOT NULL
            THEN 1 ELSE 0 END)/COUNT(*),1) AS error_pct,
      ROUND(approx_percentile(response_time_sec,0.9),1) AS p90_latency_s,
      SUM(CASE WHEN feedback_rating='POSITIVE' THEN 1 ELSE 0 END) AS thumbs_up,
      SUM(CASE WHEN feedback_rating='NEGATIVE' THEN 1 ELSE 0 END) AS thumbs_down,
      ROUND(100.0*SUM(CASE WHEN feedback_rating IS NOT NULL THEN 1 ELSE 0 END)
            /COUNT(*),1) AS fb_coverage_pct,
      date_diff(current_date(), CAST(MAX(created_ts) AS DATE)) AS days_since_active
    FROM {CATALOG}.{SCHEMA}.genie_message_details
    WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)})
    GROUP BY space_id
    """
    out = {}
    for r in run_sql(sql):
        out[r["space_id"]] = {
            "msgs": int(r["msgs"] or 0), "users": int(r["users"] or 0),
            "error_pct": float(r["error_pct"] or 0),
            "p90_latency_s": float(r["p90_latency_s"] or 0),
            "thumbs_up": int(r["thumbs_up"] or 0), "thumbs_down": int(r["thumbs_down"] or 0),
            "fb_coverage_pct": float(r["fb_coverage_pct"] or 0),
            "days_since_active": int(r["days_since_active"] or 0)}
    return out


# ---------------------------------------------------------------------------
# Warehouse health
# ---------------------------------------------------------------------------
@st.cache_data(ttl=600, show_spinner=False)
def get_existing_warehouse_ids() -> set:
    try:
        return {wh.id for wh in get_client().warehouses.list()}
    except Exception:
        return set()


def warehouse_missing(wid, existing) -> bool:
    if not wid or wid in existing:
        return False
    try:
        get_client().warehouses.get(wid)
        return False
    except Exception as e:
        m = str(e).upper()
        return any(k in m for k in ("DELETED", "DOES NOT EXIST",
                                    "RESOURCE_DOES_NOT_EXIST", "NOT_FOUND", "404"))


# ---------------------------------------------------------------------------
# Benchmark (expanded — all eval runs)
# ---------------------------------------------------------------------------
def _empty_benchmark(note="no benchmark run") -> dict:
    return {"benchmark_ran": False, "num_runs": 0, "accuracy_pct": None,
            "coverage_pct": None, "full_benchmark_size": 0, "num_questions": 0,
            "num_correct": 0, "num_needs_review": 0, "num_done": 0, "failed": 0,
            "confidence": None, "days_since_run": None, "note": note, "runs": []}


@st.cache_data(ttl=1800, show_spinner=False)
def get_eval_runs(space_id: str):
    try:
        resp = get_client().api_client.do(
            "GET", f"/api/2.0/genie/spaces/{space_id}/eval-runs")
    except Exception as e:
        m = str(e).upper()
        return None if ("403" in m or "PERMISSION" in m) else []
    return (resp or {}).get("eval_runs") or []


@st.cache_data(ttl=1800, show_spinner=False)
def get_benchmark(space_id: str) -> dict:
    """Expanded benchmark. ACCURACY uses the STRICT denominator = full defined
    set (MAX num_questions across runs), so partial runs are penalized."""
    runs = get_eval_runs(space_id)
    if runs is None:
        return _empty_benchmark("no access")
    if not runs:
        return _empty_benchmark()

    def done_of(r):
        nd = r.get("num_done")
        return nd if nd is not None else (r.get("num_questions") or 0)

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
    days = int((time.time() * 1000 - ts) / 86_400_000) if ts else None
    stale = days is not None and days > 30
    partial = nq < full
    confidence = "high" if (coverage >= 100 and not partial and not stale) else "low"
    return {"benchmark_ran": True, "num_runs": len(runs), "accuracy_pct": accuracy,
            "coverage_pct": coverage, "full_benchmark_size": full, "num_questions": nq,
            "num_correct": nc, "num_needs_review": nr, "num_done": nd, "failed": failed,
            "confidence": confidence, "days_since_run": days,
            "note": (f"{nc}/{full} on full set" if full else "ran"), "runs": runs}


# ---------------------------------------------------------------------------
# LLM verdict
# ---------------------------------------------------------------------------
VERDICT_PROMPT = (
    "You are Genie Doctor, an auditor of Databricks Genie spaces for a quality/admin "
    "team. Rank trustworthiness. CRITICAL RULE: benchmark accuracy (are answers "
    "actually correct?) outweighs usage, completion, or no-errors -- completion only "
    "means the query did not crash, NOT that the answer was right. No benchmark "
    "coverage on a high-traffic space is itself a serious risk. Accuracy is measured "
    "against the FULL defined benchmark set; LOW coverage or LOW confidence means the "
    "score is UNPROVEN even if it looks high, so treat a high accuracy with low "
    "confidence as a risk and tell them to run the full benchmark. Given ONE space, "
    "respond in EXACTLY this format on one line: "
    "'GRADE: <A-F> | WHY: <=20 words | FIX: <=15 words'. "
    "Space=\"{title}\"; questions_asked={msgs}; distinct_users={users}; "
    "error_rate_pct={error_pct}; p90_latency_sec={p90}; thumbs_up={tu}; "
    "thumbs_down={td}; feedback_coverage_pct={fb}; days_since_last_use={days}; "
    "benchmark_accuracy={acc}; benchmark_runs={runs}; benchmark_coverage_pct={cov}; "
    "benchmark_confidence={conf}; full_benchmark_size={full}."
)


def build_prompt(row: dict) -> str:
    acc = row.get("accuracy_pct")
    return VERDICT_PROMPT.format(
        title=row["title"], msgs=row["msgs"], users=row["users"],
        error_pct=row["error_pct"], p90=row["p90_latency_s"], tu=row["thumbs_up"],
        td=row["thumbs_down"], fb=row["fb_coverage_pct"], days=row["days_since_active"],
        acc=(f"{acc}%" if acc is not None else "NO BENCHMARK RUN"),
        runs=row.get("num_runs", 0), cov=row.get("coverage_pct"),
        conf=(row.get("confidence") or "n/a"), full=row.get("full_benchmark_size", 0))


@st.cache_data(ttl=1800, show_spinner=False)
def get_verdict(prompt: str):
    rows = run_sql("SELECT ai_query(:model, :prompt) AS verdict",
                   parameters=[StatementParameterListItem(name="model", value=MODEL),
                               StatementParameterListItem(name="prompt", value=prompt)])
    raw = (rows[0].get("verdict") or "") if rows else ""
    grade, why, fix = "?", raw.strip(), ""
    m = re.search(r"GRADE:\s*([A-F])\s*\|\s*WHY:\s*(.*?)\s*\|\s*FIX:\s*(.*)",
                  raw, re.IGNORECASE | re.DOTALL)
    if m:
        grade, why, fix = m.group(1).upper(), m.group(2).strip(), m.group(3).strip()
    return grade, why, fix


# ---------------------------------------------------------------------------
# Cockpit
# ---------------------------------------------------------------------------
@st.cache_data(ttl=1800, show_spinner=True)
def build_cockpit() -> pd.DataFrame:
    universe = get_space_universe()
    factors = get_factors_map()
    existing_wh = get_existing_warehouse_ids()
    records = []
    for sid in SCOPE_SPACE_IDS:
        fb_title = TITLE_FALLBACK.get(sid, f"Space {sid[:8]}…")
        info = universe.get(sid, {"title": fb_title, "warehouse_id": None})
        wid = info.get("warehouse_id")
        f = factors.get(sid)
        has_activity = f is not None and f.get("msgs", 0) > 0
        row = {"space_id": sid, "title": info.get("title") or fb_title,
               "warehouse_id": wid, "msgs": 0, "users": 0, "error_pct": 0.0,
               "p90_latency_s": 0.0, "thumbs_up": 0, "thumbs_down": 0,
               "fb_coverage_pct": 0.0, "days_since_active": 0}
        if f:
            row.update(f)
        wh_missing = warehouse_missing(wid, existing_wh)
        row["wh_missing"] = wh_missing
        row.update(get_benchmark(sid))
        if wh_missing:
            row["grade"] = "F"
            row["why"] = "SQL warehouse deleted — space is non-functional"
            row["fix"] = "Reassign an existing SQL warehouse in space Settings"
        elif not has_activity:
            row["grade"] = "—"
            row["why"] = "No activity harvested yet — nothing to judge."
            row["fix"] = "Drive usage or run an eval benchmark."
        else:
            g, why, fix = get_verdict(build_prompt(row))
            row["grade"], row["why"], row["fix"] = g, why, fix
        row["score"] = GRADE_SCORE.get(row["grade"])
        records.append(row)
    df = pd.DataFrame(records)

    def tier(r):
        if r["wh_missing"]:
            return 0
        if r["grade"] == "—":
            return 2
        return 1
    df["_tier"] = df.apply(tier, axis=1)
    df["_bench"] = df["accuracy_pct"].fillna(-1)
    df = df.sort_values(["_tier", "_bench"]).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    write_snapshot(df)  # best-effort score history
    return df


# ---------------------------------------------------------------------------
# Score-history snapshot
# ---------------------------------------------------------------------------
def write_snapshot(df: pd.DataFrame):
    try:
        for _, r in df.iterrows():
            params = [
                StatementParameterListItem(name="sid", value=r["space_id"]),
                StatementParameterListItem(name="title", value=str(r["title"])),
                StatementParameterListItem(name="score",
                    value=(None if pd.isna(r.get("score")) else str(float(r["score"]))), type="DOUBLE"),
                StatementParameterListItem(name="grade", value=str(r["grade"])),
                StatementParameterListItem(name="acc",
                    value=(None if r.get("accuracy_pct") is None else str(float(r["accuracy_pct"]))), type="DOUBLE"),
                StatementParameterListItem(name="err", value=str(float(r["error_pct"])), type="DOUBLE"),
                StatementParameterListItem(name="msgs", value=str(int(r["msgs"])), type="INT"),
                StatementParameterListItem(name="users", value=str(int(r["users"])), type="INT"),
                StatementParameterListItem(name="days", value=str(int(r["days_since_active"])), type="INT"),
                StatementParameterListItem(name="bran", value=str(bool(r["benchmark_ran"])).lower(), type="BOOLEAN"),
            ]
            run_sql(f"""
            MERGE INTO {SCORES_TABLE} t
            USING (SELECT current_date() AS d, :sid AS space_id) s
            ON t.snapshot_date = s.d AND t.space_id = s.space_id
            WHEN MATCHED THEN UPDATE SET title=:title, score=:score, grade=:grade,
                 accuracy_pct=:acc, error_pct=:err, msgs=:msgs, users=:users,
                 days_since_active=:days, benchmark_ran=:bran, updated_at=current_timestamp()
            WHEN NOT MATCHED THEN INSERT (snapshot_date, space_id, title, score, grade,
                 accuracy_pct, error_pct, msgs, users, days_since_active, benchmark_ran, updated_at)
            VALUES (s.d, :sid, :title, :score, :grade, :acc, :err, :msgs, :users, :days, :bran, current_timestamp())
            """, parameters=params)
    except Exception:
        pass  # best-effort; charts fall back to "trend builds as snapshots accrue"


@st.cache_data(ttl=600, show_spinner=False)
def get_score_history() -> pd.DataFrame:
    try:
        rows = run_sql(
            f"SELECT snapshot_date, space_id, title, score, grade, accuracy_pct "
            f"FROM {SCORES_TABLE} WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)}) "
            f"ORDER BY snapshot_date")
        df = pd.DataFrame(rows)
        if not df.empty:
            df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
            df["score"] = pd.to_numeric(df["score"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Trends (from existing timestamps)
# ---------------------------------------------------------------------------
def _trend(sql: str) -> pd.DataFrame:
    try:
        df = pd.DataFrame(run_sql(sql))
        if not df.empty and "wk" in df.columns:
            df["wk"] = pd.to_datetime(df["wk"])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=1800, show_spinner=False)
def trend_conversations() -> pd.DataFrame:
    return _trend(f"""
      SELECT date_trunc('WEEK', created_ts) AS wk, space_id, COUNT(*) AS n
      FROM {CATALOG}.{SCHEMA}.genie_conversations
      WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)}) AND created_ts IS NOT NULL
      GROUP BY 1,2 ORDER BY 1""")


@st.cache_data(ttl=1800, show_spinner=False)
def trend_users() -> pd.DataFrame:
    return _trend(f"""
      SELECT date_trunc('WEEK', created_ts) AS wk, space_id,
             COUNT(DISTINCT user_id) AS n
      FROM {CATALOG}.{SCHEMA}.genie_message_details
      WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)}) AND created_ts IS NOT NULL
      GROUP BY 1,2 ORDER BY 1""")


@st.cache_data(ttl=1800, show_spinner=False)
def trend_feedback() -> pd.DataFrame:
    return _trend(f"""
      SELECT date_trunc('WEEK', created_ts) AS wk, space_id,
             SUM(CASE WHEN feedback_rating='POSITIVE' THEN 1 ELSE 0 END) AS pos,
             SUM(CASE WHEN feedback_rating='NEGATIVE' THEN 1 ELSE 0 END) AS neg,
             SUM(CASE WHEN feedback_rating IS NOT NULL THEN 1 ELSE 0 END) AS rated
      FROM {CATALOG}.{SCHEMA}.genie_message_details
      WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)}) AND created_ts IS NOT NULL
      GROUP BY 1,2 ORDER BY 1""")


@st.cache_data(ttl=1800, show_spinner=False)
def trend_quality() -> pd.DataFrame:
    """Weekly error-rate and p90 latency per space."""
    return _trend(f"""
      SELECT date_trunc('WEEK', created_ts) AS wk, space_id,
             ROUND(100.0*SUM(CASE WHEN status<>'COMPLETED' OR error_type IS NOT NULL
                   THEN 1 ELSE 0 END)/COUNT(*),1) AS error_pct,
             ROUND(approx_percentile(response_time_sec,0.9),1) AS p90_s,
             COUNT(*) AS msgs
      FROM {CATALOG}.{SCHEMA}.genie_message_details
      WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)}) AND created_ts IS NOT NULL
      GROUP BY 1,2 ORDER BY 1""")


@st.cache_data(ttl=1800, show_spinner=False)
def trend_depth() -> pd.DataFrame:
    """Avg messages per conversation per space (engagement depth)."""
    return _trend(f"""
      SELECT space_id, ROUND(AVG(message_count),2) AS avg_msgs_per_convo,
             COUNT(*) AS convos
      FROM {CATALOG}.{SCHEMA}.genie_conversations
      WHERE space_id IN ({_in_list(SCOPE_SPACE_IDS)})
      GROUP BY 1""")


# ---------------------------------------------------------------------------
# Agent supporting detail
# ---------------------------------------------------------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def agent_top_questions(space_id: str, limit=8) -> pd.DataFrame:
    return _trend(f"""
      SELECT user_question AS question, COUNT(*) AS asked,
             MAX(created_ts) AS last_asked
      FROM {CATALOG}.{SCHEMA}.genie_message_details
      WHERE space_id = '{space_id.replace("'", "")}' AND user_question IS NOT NULL
      GROUP BY 1 ORDER BY asked DESC, last_asked DESC LIMIT {int(limit)}""")


@st.cache_data(ttl=1800, show_spinner=False)
def agent_recent_errors(space_id: str, limit=6) -> pd.DataFrame:
    return _trend(f"""
      SELECT created_ts, user_question, error_type, error_message
      FROM {CATALOG}.{SCHEMA}.genie_message_details
      WHERE space_id = '{space_id.replace("'", "")}'
        AND (status<>'COMPLETED' OR error_type IS NOT NULL)
      ORDER BY created_ts DESC LIMIT {int(limit)}""")


@st.cache_data(ttl=1800, show_spinner=False)
def agent_feedback_samples(space_id: str, limit=6) -> pd.DataFrame:
    return _trend(f"""
      SELECT created_ts, user_question, feedback_rating
      FROM {CATALOG}.{SCHEMA}.genie_message_details
      WHERE space_id = '{space_id.replace("'", "")}' AND feedback_rating IS NOT NULL
      ORDER BY created_ts DESC LIMIT {int(limit)}""")

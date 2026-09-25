"""Genie Doctor — fleet-health scorecard (percentage model, no grade).

Microsite-style UI: cohesive HTML (DM Sans / lava / oat / navy), stat tiles, elegant
clickable cards, query-param navigation. Reads the gold tables (Postgres via Lakebase,
warehouse fallback): space_scorecard, fleet_rollup, space_improvements.
"""
import os
from urllib.parse import quote
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Genie Doctor", page_icon="🩺", layout="wide")

CATALOG = os.environ.get("CATALOG", "vjoseph_pbi_demo")
SCHEMA = os.environ.get("SCHEMA", "genie_monitoring")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID") or os.environ.get("DATABRICKS_WAREHOUSE_ID")
HOST = (os.environ.get("DATABRICKS_HOST")
        or "https://adb-984752964297111.11.azure.databricks.net").rstrip("/")
WORKSPACE = (os.environ.get("WORKSPACE_NAME")
             or HOST.replace("https://", "").replace("http://", "").split(".")[0])

# Databricks microsite palette.
INK = "#1B3139"
LAVA = "#FF3621"     # identity only (brand marks) — never for status
ACCENT = "#2272B4"   # cool blue: interactive accents (active filter, links) — distinct from red=attention
OAT = "#F6F2EC"
GREEN, AMBER, RED, MUTE, FAINT = "#1E7F5C", "#B26E00", "#C0362C", "#5A6B72", "#AEB8BC"
HAIR = "#E7E2D8"


def genie_url(space_id):
    return f"{HOST}/genie/rooms/{space_id}"


def current_ws():
    """Workspace name: user-typed (query param) → env → derived from host."""
    return (st.query_params.get("ws") or os.environ.get("WORKSPACE_NAME") or WORKSPACE)


def page_top(ws):
    """Common title bar shared across the summary and detail pages."""
    return (f"<div class='gd-topbar'>"
            f"<span class='t'>Genie Doctor</span>"
            f"<span class='gd-ws' title='workspace'>🗂 {ws}</span></div>")


st.markdown(f"""
<style>
 @import url('https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&display=swap');
 html,body,[class*="css"],.stApp {{ font-family:'DM Sans',-apple-system,system-ui,sans-serif; }}
 .stApp {{ background:{OAT}; }}
 header[data-testid="stHeader"] {{ background:transparent; }}
 .block-container {{ max-width:1360px; padding-top:1.6rem; padding-bottom:3rem; }}
 #MainMenu, footer {{ visibility:hidden; }}
 a {{ text-decoration:none; }}

 /* Streamlit control widgets (Owner / Workspace) — match the app font + label style. */
 [data-testid="stWidgetLabel"] p {{ font-family:'DM Sans',-apple-system,system-ui,sans-serif;
     font-size:0.66rem; font-weight:700; text-transform:uppercase; letter-spacing:0.07em; color:{MUTE}; }}
 .stTextInput input, [data-baseweb="select"] > div {{
     font-family:'DM Sans',-apple-system,system-ui,sans-serif; font-size:0.9rem; }}

 /* Common top title bar (shared across pages). */
 .gd-topbar {{ display:flex; align-items:center; gap:16px; padding:2px 0 18px; margin-bottom:22px;
               border-bottom:2px solid {INK}; flex-wrap:wrap; }}
 .gd-topbar .t {{ font-size:2.2rem; font-weight:700; letter-spacing:-0.035em; color:{INK}; line-height:1; }}
 .gd-topbar .s {{ color:{MUTE}; font-size:0.88rem; flex:1 1 260px; min-width:200px; line-height:1.45; }}
 .gd-topbar .gd-ws {{ font-size:0.8rem; font-weight:600; color:{INK}; background:#fff; border:1px solid {HAIR};
                      border-radius:999px; padding:6px 14px; white-space:nowrap; margin-left:auto; }}
 .gd-orow {{ display:flex; justify-content:space-between; align-items:center; padding:6px 11px; border-radius:7px;
             color:{MUTE}; font-size:0.8rem; font-weight:600; }}
 .gd-orow:hover {{ background:#fff; color:{INK}; }}
 .gd-orow.on {{ background:{INK}; color:#fff; }}
 .gd-orow .c {{ font-size:0.72rem; opacity:.8; }}

 /* Two-pane app shell: left rail fills the viewport height; KPIs stretch to fill it. */
 .gd-shell {{ display:flex; gap:24px; align-items:flex-start; }}
 .gd-rail {{ width:300px; flex:0 0 300px; background:#EFEAE0; border:1px solid {HAIR};
             border-radius:16px; padding:16px 16px 18px; display:flex; flex-direction:column;
             min-height:calc(100vh - 210px); }}
 .gd-kwrap {{ flex:1 1 auto; display:flex; flex-direction:column; gap:12px; }}
 .gd-kwrap .gd-krow {{ flex:1 1 0; margin-bottom:0; }}
 .gd-railfoot {{ color:{FAINT}; font-size:0.68rem; line-height:1.5; margin-top:auto; padding-top:16px; }}
 .gd-rail-sum {{ margin-top:2.6rem; }}   /* align KPI boxes with the agent cards (which sit below a band title) */
 .gd-main {{ flex:1 1 auto; min-width:0; }}
 .gd-brand-h {{ font-size:1.3rem; font-weight:700; letter-spacing:-0.02em; color:{INK}; margin:2px 0 8px; line-height:1.15; }}
 .gd-krow {{ display:flex; justify-content:space-between; align-items:center; background:#fff; border:1px solid {HAIR};
             border-left:4px solid transparent; border-radius:12px; padding:15px 17px; margin-bottom:10px; color:inherit;
             transition:box-shadow .15s, transform .12s; }}
 .gd-krow:hover {{ box-shadow:0 5px 16px rgba(27,49,57,.10); transform:translateY(-1px); }}
 .gd-krow.on {{ border-left-color:{ACCENT}; box-shadow:0 3px 12px rgba(27,49,57,.10); }}
 .gd-krow .l {{ font-size:0.9rem; font-weight:600; color:{INK}; line-height:1.25; }}
 .gd-krow .l small {{ display:block; color:{MUTE}; font-size:0.72rem; font-weight:500; margin-top:1px; }}
 .gd-krow .v {{ font-size:2rem; font-weight:700; letter-spacing:-0.02em; }}
 .gd-secnav {{ margin-top:16px; border-top:1px solid {HAIR}; padding-top:12px; }}
 .gd-secnav-h {{ font-size:0.64rem; font-weight:700; letter-spacing:0.1em; text-transform:uppercase; color:{FAINT}; padding:0 11px 4px; }}
 .gd-secnav a {{ display:block; padding:6px 11px; border-radius:7px; color:{MUTE}; font-size:0.8rem; font-weight:600; }}
 .gd-secnav a:hover {{ background:#fff; color:{INK}; }}
 .gd-main-h {{ font-size:1.15rem; font-weight:700; color:{INK}; letter-spacing:-0.01em; margin:2px 0 4px; }}
 @media(max-width:900px){{ .gd-shell{{flex-direction:column;}} .gd-rail{{position:static; width:auto; flex:none;}} }}

 .gd-band {{ display:flex; align-items:center; gap:9px; margin:28px 0 10px; flex-wrap:wrap; }}
 .gd-band .dot {{ width:10px; height:10px; border-radius:50%; }}
 .gd-band .bl {{ font-size:0.75rem; font-weight:700; letter-spacing:0.08em; text-transform:uppercase; color:{INK}; }}
 .gd-band .bc {{ font-size:0.72rem; font-weight:700; color:#fff; background:{INK}; border-radius:999px; padding:1px 8px; }}
 .gd-band .bd {{ font-size:0.8rem; color:{MUTE}; }}
 .gd-clear {{ display:inline-block; font-size:0.8rem; color:{MUTE}; font-weight:600; margin:2px 0 6px; }}

 .gd-sec {{ display:flex; align-items:center; gap:10px; margin:30px 0 12px; }}
 .gd-sec::before {{ content:''; width:22px; height:3px; background:{LAVA}; border-radius:2px; }}
 .gd-sec span {{ font-size:0.75rem; font-weight:700; letter-spacing:0.1em; text-transform:uppercase; color:{INK}; }}

 .gd-cards {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(196px,1fr)); gap:10px; margin-top:6px; }}
 .gd-card {{ display:flex; flex-direction:column; background:#fff; border:1px solid {HAIR}; border-left:4px solid {FAINT};
             border-radius:11px; padding:11px 13px; color:inherit;
             box-shadow:0 1px 2px rgba(27,49,57,.04); transition:box-shadow .15s, transform .15s; }}
 .gd-card:hover {{ box-shadow:0 6px 16px rgba(27,49,57,.10); transform:translateY(-2px); }}
 .gd-cardlink {{ display:flex; flex-direction:column; color:inherit; }}
 .gd-recs {{ display:block; margin-top:8px; padding-top:8px; border-top:1px solid {HAIR};
             font-size:0.66rem; font-weight:700; letter-spacing:0.02em; }}
 .gd-recs:hover {{ text-decoration:underline; }}
 .gd-name {{ font-size:0.9rem; font-weight:600; color:{INK}; letter-spacing:-0.01em; line-height:1.25; min-height:2.3em; }}
 .gd-link {{ font-size:0.75rem; color:{ACCENT}; font-weight:600; margin-left:8px; }}
 .gd-meta {{ color:{MUTE}; font-size:0.72rem; margin-top:8px; line-height:1.5; }}
 .gd-chip {{ display:inline-block; align-self:flex-start; font-size:0.64rem; font-weight:700; letter-spacing:0.04em;
             padding:2px 8px; border-radius:999px; margin-top:6px; }}
 .gd-qlbl {{ color:{MUTE}; font-size:0.64rem; text-transform:uppercase; letter-spacing:0.05em; font-weight:700; margin-top:10px; }}
 .gd-qnum {{ font-size:1.45rem; font-weight:700; line-height:1; margin:3px 0 5px; }}
 .gd-bar {{ height:6px; border-radius:6px; background:#ECE7DC; overflow:hidden; }}
 .gd-bar > i {{ display:block; height:6px; border-radius:6px; }}
 .gd-qsub {{ color:{MUTE}; font-size:0.72rem; margin-top:6px; }}

 .gd-back {{ font-size:0.85rem; color:{MUTE}; font-weight:600; }}
 .gd-ptiles {{ display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin:6px 0 4px; }}
 .gd-ptile {{ background:#fff; border:1px solid {HAIR}; border-radius:12px; padding:14px 16px; }}
 .gd-ptile .l {{ color:{MUTE}; font-size:0.64rem; text-transform:uppercase; letter-spacing:0.05em; font-weight:700; }}
 .gd-ptile .v {{ font-size:1.5rem; font-weight:700; color:{INK}; margin-top:5px; }}

 .gd-step {{ background:#fff; border:1px solid {HAIR}; border-left:4px solid {ACCENT};
             border-radius:12px; padding:14px 16px; margin-bottom:10px; }}
 .gd-step .h {{ font-weight:600; color:{INK}; }}
 .gd-step .w {{ color:{MUTE}; font-size:0.9rem; margin-top:4px; line-height:1.5; }}
 .gd-tag {{ display:inline-block; font-size:0.64rem; font-weight:700; letter-spacing:0.03em;
            padding:2px 9px; border-radius:999px; margin-right:6px; }}
 .gd-ev {{ font-family:ui-monospace,Menlo,monospace; font-size:0.74rem; color:{MUTE};
           background:{OAT}; border:1px solid {HAIR}; border-radius:6px; padding:4px 9px;
           display:inline-block; margin-top:8px; overflow-wrap:anywhere; }}
 .gd-tabs {{ display:flex; gap:6px; margin:30px 0 12px; align-items:center; flex-wrap:wrap; }}
 .gd-tabs::before {{ content:''; width:22px; height:3px; background:{LAVA}; border-radius:2px; margin-right:4px; }}
 .gd-tabs .tl {{ font-size:0.74rem; font-weight:700; letter-spacing:0.1em; text-transform:uppercase; color:{INK}; margin-right:8px; }}
 .gd-tabs a {{ font-size:0.76rem; color:{MUTE}; border:1px solid {HAIR}; background:#fff; border-radius:999px; padding:3px 12px; }}
 .gd-tabs a.on {{ color:#fff; background:{INK}; border-color:{INK}; }}
 .gd-matrix {{ border-collapse:separate; border-spacing:4px; font-size:0.78rem; }}
 .gd-matrix th {{ font-weight:600; color:{MUTE}; font-size:0.7rem; padding:2px 6px; text-align:center; }}
 .gd-matrix th.ql {{ text-align:left; color:{INK}; max-width:340px; font-weight:500; }}
 .gd-matrix td {{ width:30px; height:26px; text-align:center; font-weight:700; border-radius:5px; }}
 @media(max-width:820px){{ .gd-grid,.gd-ptiles{{grid-template-columns:repeat(2,1fr);}} }}
</style>""", unsafe_allow_html=True)

# --------------------------------------------------------------------------- data
# The serving backend is an explicit CHOICE, set by the SERVING_BACKEND env var:
#   lakehouse  -> read the gold Delta tables through the bound SQL warehouse
#                 (Statement Execution API). No Lakebase dependency — always available.
#   lakebase   -> read the Postgres mirror of those gold tables (ms latency). Needs a
#                 provisioned Lakebase instance + PG host/user; degrades to lakehouse if
#                 it is not configured or not reachable.
#   auto       -> use lakebase when it is configured AND reachable, else lakehouse.
# Both backends run the SAME unqualified SQL (`SELECT ... FROM space_scorecard`): the
# warehouse call qualifies it with CATALOG/SCHEMA, Postgres resolves it on search_path.
SERVING_BACKEND = os.environ.get("SERVING_BACKEND", "auto").strip().lower()
LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "genie-doctor-db")
PGHOST = os.environ.get("PGHOST") or os.environ.get("DATABRICKS_DATABASE_HOST")
PGDATABASE = os.environ.get("PGDATABASE") or os.environ.get("DATABRICKS_DATABASE_NAME") or "genie_doctor"
PGUSER = os.environ.get("PGUSER") or os.environ.get("DATABRICKS_CLIENT_ID")

SOURCE = {"used": "?", "backend": None, "note": ""}


@st.cache_resource
def client():
    return WorkspaceClient()


@st.cache_resource
def pg_conn():
    import uuid
    import psycopg2
    cred = client().database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_INSTANCE])
    conn = psycopg2.connect(host=PGHOST, port=5432, dbname=PGDATABASE, user=PGUSER,
                            password=cred.token, sslmode="require")
    conn.autocommit = True
    return conn


def _lakebase_configured() -> bool:
    return bool(PGHOST and PGUSER)


@st.cache_resource(show_spinner=False)
def active_backend() -> str:
    """Resolve the serving backend ONCE, honoring SERVING_BACKEND and degrading safely
    to the Lakehouse warehouse when Lakebase is absent or unreachable."""
    choice = SERVING_BACKEND if SERVING_BACKEND in ("lakehouse", "lakebase", "auto") else "auto"
    if choice == "lakehouse":
        return "lakehouse"
    if not _lakebase_configured():
        if choice == "lakebase":
            SOURCE["note"] = ("SERVING_BACKEND=lakebase but no Postgres host/user is "
                              "configured — using the Lakehouse warehouse.")
        return "lakehouse"
    try:                       # lakebase or auto, and PG is configured — probe it once.
        pg_conn()
        return "lakebase"
    except Exception as e:
        SOURCE["note"] = f"Lakebase unreachable ({str(e)[:70]}) — using the Lakehouse warehouse."
        return "lakehouse"


def _q_pg(sql):
    with pg_conn().cursor() as cur:
        cur.execute(sql)
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _q_warehouse(sql):
    import time
    if not WAREHOUSE_ID:
        raise RuntimeError("Lakehouse backend needs a SQL warehouse, but WAREHOUSE_ID is "
                           "unset — bind one to the app in resources/app.yml.")
    r = client().statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, catalog=CATALOG, schema=SCHEMA, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1); r = client().statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(r.status.error.message if r.status.error else "SQL failed")
    cols = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame([dict(zip(cols, row)) for row in (r.result.data_array or [])])


@st.cache_data(ttl=300, show_spinner=False)
def q(sql):
    if active_backend() == "lakebase":
        try:
            df = _q_pg(sql)
            SOURCE.update(used="Lakebase (Postgres)", backend="lakebase")
            return df
        except Exception as e:      # per-query safety net: degrade to the warehouse.
            SOURCE["note"] = f"Lakebase query failed ({str(e)[:60]}) — fell back to Lakehouse."
    df = _q_warehouse(sql)
    SOURCE.update(used="Lakehouse (warehouse · Delta)", backend="lakehouse")
    return df


def _num(v):
    return 0.0 if v is None or (isinstance(v, float) and pd.isna(v)) else float(v)


def pct(v):
    return f"{_num(v):.0f}%"


def color_for(v):
    v = _num(v)
    return GREEN if v >= 80 else (AMBER if v >= 50 else RED)


_BOOL = ["benchmark_ran"]
_INT = ["msgs", "users", "days_since_active", "sql_instructions", "text_instructions",
        "sample_questions", "benchmark_questions_defined", "n_tables", "num_runs",
        "num_questions", "num_correct", "num_needs_review", "num_done", "thumbs_up", "thumbs_down", "rank"]
_FLT = ["benchmark_quality_pct", "coverage_pct", "feedback_positive_pct", "feedback_coverage_pct", "score"]


def _coerce(df):
    for c in _BOOL:
        if c in df.columns:
            df[c] = df[c].map(lambda v: str(v).strip().lower() in ("true", "t", "1", "yes"))
    for c in _INT:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in _FLT:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def scorecard():
    return _coerce(q("SELECT * FROM space_scorecard"))


@st.cache_data(ttl=300, show_spinner=False)
def improvements():
    try:
        return q("SELECT space_id, rank, category, impact, source, step, why, evidence FROM space_improvements")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def flagged(space_id):
    try:
        return q(f"SELECT question, sql_generated, error_type, created_ts FROM feedback_flags "
                 f"WHERE space_id = '{space_id}' ORDER BY created_ts DESC")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def run_trend(space_id):
    try:
        return _coerce(q(f"SELECT run_index, quality_pct, coverage_pct, num_correct, num_questions, "
                         f"num_done FROM benchmark_run_trend WHERE space_id = '{space_id}' ORDER BY run_index"))
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def question_matrix(space_id):
    try:
        return q(f"SELECT run_index, question, assessment FROM benchmark_question_matrix "
                 f"WHERE space_id = '{space_id}' ORDER BY run_index")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def all_trends():
    """All agents' run trends in one query, for the fleet-card sparkline previews."""
    try:
        df = q("SELECT space_id, run_index, quality_pct FROM benchmark_run_trend ORDER BY space_id, run_index")
        df["run_index"] = pd.to_numeric(df["run_index"], errors="coerce")
        df["quality_pct"] = pd.to_numeric(df["quality_pct"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def barhtml(v, color):
    return f"<div class='gd-bar'><i style='width:{max(2, min(100, _num(v)))}%;background:{color}'></i></div>"


def trend_svg(df):
    """Accuracy-over-runs sparkline: quality_pct per eval run, in execution order."""
    if df.empty:
        return "<div class='gd-qsub'>No eval runs yet — define benchmarks and run an evaluation.</div>"
    pts = []
    for _, r in df.iterrows():
        try:
            v = float(r["quality_pct"])
        except (TypeError, ValueError):
            v = None
        if v is not None and not pd.isna(v):
            pts.append((int(r["run_index"]), v))
    if not pts:
        return "<div class='gd-qsub'>Benchmarks defined, but no completed run has a score yet.</div>"
    W, H, PL, PR, PT, PB = 540, 150, 34, 14, 18, 26
    n = len(pts)
    def X(k):
        return PL + (W - PL - PR) * (k / (n - 1)) if n > 1 else PL + (W - PL - PR) / 2
    def Y(v):
        return PT + (H - PT - PB) * (1 - v / 100.0)
    grid = ""
    for gv in (0, 50, 100):
        gy = Y(gv)
        grid += (f"<line x1='{PL}' y1='{gy:.0f}' x2='{W-PR}' y2='{gy:.0f}' stroke='{HAIR}' stroke-width='1'/>"
                 f"<text x='{PL-6}' y='{gy+3:.0f}' font-size='9' text-anchor='end' fill='{MUTE}'>{gv}</text>")
    line = ""
    if n > 1:
        poly = " ".join(f"{X(k):.0f},{Y(v):.0f}" for k, (ri, v) in enumerate(pts))
        line = f"<polyline points='{poly}' fill='none' stroke='{INK}' stroke-width='2'/>"
    dots = ""
    for k, (ri, v) in enumerate(pts):
        c = color_for(v)
        dots += (f"<circle cx='{X(k):.0f}' cy='{Y(v):.0f}' r='4' fill='{c}'/>"
                 f"<text x='{X(k):.0f}' y='{Y(v)-9:.0f}' font-size='10' font-weight='700' text-anchor='middle' fill='{c}'>{v:.0f}%</text>"
                 f"<text x='{X(k):.0f}' y='{H-8:.0f}' font-size='9' text-anchor='middle' fill='{MUTE}'>run {ri}</text>")
    note = ""
    if n >= 2:
        d = pts[-1][1] - pts[0][1]
        arrow = "▲" if d > 0 else ("▼" if d < 0 else "▬")
        tc = GREEN if d > 0 else (RED if d < 0 else MUTE)
        note = (f"<div class='gd-qsub'>Trend across {n} runs: <b style='color:{tc}'>{arrow} {d:+.0f} pp</b> "
                f"(first {pts[0][1]:.0f}% → latest {pts[-1][1]:.0f}%). A trend only means something when "
                f"curation changed between runs.</div>")
    else:
        note = "<div class='gd-qsub'>Only one run so far — no trend yet.</div>"
    return f"<svg viewBox='0 0 {W} {H}' style='width:100%;max-width:{W}px;height:auto'>{grid}{line}{dots}</svg>{note}"


def mini_spark(pts, color):
    """Compact ECG/heartbeat-style sparkline of quality across runs for a fleet card."""
    vals = [(i, v) for i, v in pts if v is not None]
    if len(vals) < 2:
        return ""
    W, H, P = 200, 40, 8
    n = len(vals)
    def X(k):
        return P + (W - 2 * P) * (k / (n - 1))
    def Y(v):
        return P + (H - 2 * P) * (1 - v / 100.0)
    y50 = Y(50)
    first_y, last_y = Y(vals[0][1]), Y(vals[-1][1])
    # flat lead-in/out so it reads like a monitor trace
    seg = (f"0,{first_y:.0f} " + " ".join(f"{X(k):.0f},{Y(v):.0f}" for k, (i, v) in enumerate(vals))
           + f" {W},{last_y:.0f}")
    lx, ly = X(n - 1), last_y
    return (f"<svg viewBox='0 0 {W} {H}' style='width:100%;height:auto;margin-top:7px;display:block'>"
            f"<line x1='0' y1='{y50:.0f}' x2='{W}' y2='{y50:.0f}' stroke='{HAIR}' stroke-width='1' stroke-dasharray='2 4'/>"
            f"<polyline points='{seg}' fill='none' stroke='{color}' stroke-width='1.8' "
            f"stroke-linejoin='round' stroke-linecap='round'/>"
            f"<circle cx='{lx:.0f}' cy='{ly:.0f}' r='6' fill='{color}' opacity='0.16'/>"
            f"<circle cx='{lx:.0f}' cy='{ly:.0f}' r='2.7' fill='{color}'/></svg>")


def matrix_html(df):
    """Per-question pass/fail matrix across runs (rows=questions, cols=runs)."""
    if df.empty:
        return "<div class='gd-qsub'>No per-question results yet.</div>"
    qorder, runs, cell = [], [], {}
    for _, r in df.iterrows():
        ql = str(r["question"]); ri = int(r["run_index"]); a = str(r["assessment"] or "")
        if ql not in qorder:
            qorder.append(ql)
        if ri not in runs:
            runs.append(ri)
        cell[(ql, ri)] = a
    runs = sorted(runs)
    amap = {"GOOD": GREEN, "BAD": RED, "NEEDS_REVIEW": AMBER}
    aglyph = {"GOOD": "✓", "BAD": "✕", "NEEDS_REVIEW": "~"}
    head = "".join(f"<th>r{ri}</th>" for ri in runs)
    body = ""
    for ql in qorder:
        cells = ""
        for ri in runs:
            a = cell.get((ql, ri))
            col = amap.get(a, "#ECE7DC"); g = aglyph.get(a, "·")
            cells += f"<td style='background:{col};color:#fff'>{g}</td>"
        label = (ql[:60] + "…") if len(ql) > 60 else ql
        body += f"<tr><th class='ql'>{label}</th>{cells}</tr>"
    legend = (f"<div class='gd-qsub'><span style='color:{GREEN}'>✓ pass</span> &nbsp; "
              f"<span style='color:{RED}'>✕ fail</span> &nbsp; <span style='color:{AMBER}'>~ needs review</span></div>")
    return (f"<div style='overflow-x:auto'><table class='gd-matrix'>"
            f"<thead><tr><th class='ql'>Benchmark question</th>{head}</tr></thead>"
            f"<tbody>{body}</tbody></table></div>{legend}")


# --------------------------------------------------------------------------- summary
def render_summary():
    sc = scorecard()
    imp = improvements()
    filt = st.query_params.get("filter", "all")
    has_owner = "owner" in sc.columns
    owners_all = sorted([o for o in sc["owner"].dropna().unique() if o]) if has_owner else []

    # 1) Title bar anchors the top of the page.
    st.markdown(page_top(current_ws()), unsafe_allow_html=True)

    # 2) Controls row under the title: Owner filter + Workspace name.
    tc1, tc2 = st.columns([1, 1])
    with tc1:
        if owners_all:
            labels = ["All owners"] + owners_all
            cur = st.query_params.get("owner", "all")
            cur_label = cur if cur in owners_all else "All owners"
            pick = st.selectbox("Owner", labels, index=labels.index(cur_label))
            owner_sel = "all" if pick == "All owners" else pick
        else:
            owner_sel = "all"
    with tc2:
        ws_val = st.text_input("Workspace", value=current_ws(), placeholder="Type your workspace name")
    if ws_val and ws_val != current_ws():
        st.query_params["ws"] = ws_val
    if owner_sel != st.query_params.get("owner", "all"):
        st.query_params["owner"] = owner_sel

    if owner_sel != "all" and has_owner:
        sc = sc[sc["owner"] == owner_sel].copy()

    def href(filter=None, owner=None):
        f = filter if filter is not None else filt
        o = owner if owner is not None else owner_sel
        return f"?filter={f}&owner={o}&ws={quote(ws_val)}"

    n = len(sc)
    nb = int(sc["benchmark_ran"].sum()) if "benchmark_ran" in sc else 0
    aq = sc.loc[sc["benchmark_ran"] == True, "benchmark_quality_pct"].dropna() if "benchmark_ran" in sc else pd.Series(dtype=float)
    avgq = aq.mean() if not aq.empty else None
    att_ids = set(imp[imp["impact"] == "high"]["space_id"]) if (not imp.empty and "impact" in imp) else set()
    rec_counts = imp["space_id"].value_counts().to_dict() if not imp.empty else {}
    tr = all_trends()
    trend_map = {}
    if not tr.empty:
        for sid_, g in tr.groupby("space_id"):
            trend_map[sid_] = [(int(ri), float(qp)) for ri, qp in zip(g["run_index"], g["quality_pct"]) if pd.notna(qp)]

    def band_of(r):
        ran = bool(r["benchmark_ran"]); qv = _num(r["benchmark_quality_pct"])
        if (r["space_id"] in att_ids) or (ran and qv < 50):
            return "attention"
        if ran and qv >= 80:
            return "proven"
        return "monitor"
    sc = sc.assign(_band=sc.apply(band_of, axis=1))
    n_att = int((sc["_band"] == "attention").sum())
    n_prov = int((sc["_band"] == "proven").sum())

    def chip(r):
        ran = bool(r["benchmark_ran"]); qv = _num(r["benchmark_quality_pct"]); bdef = int(r["benchmark_questions_defined"])
        if ran and qv < 50:
            return ("LOW ACCURACY", RED)
        if r["space_id"] in att_ids:
            return ("NEEDS FIX", RED)
        if ran and qv >= 80:
            return ("PROVEN", GREEN)
        if ran:
            return ("MODERATE", AMBER)
        if bdef > 0:
            return ("DEFINED · NOT RUN", AMBER)
        return ("NO BENCHMARK", MUTE)

    n_mon = n - n_att - n_prov

    # KPI rows double as the left-rail navigation (click to filter; owner is preserved).
    def krow(f, label, sub, num, color):
        on = "on" if filt == f else ""
        return (f"<a class='gd-krow {on}' href='{href(filter=f)}' target='_top'>"
                f"<div class='l'>{label}<small>{sub}</small></div>"
                f"<div class='v' style='color:{color}'>{num}</div></a>")
    rail_kpis = "".join([
        krow("all", "All agents", "every space", n, INK),
        krow("proven", "Proven", "benchmarked ≥80%", n_prov, GREEN if n_prov else MUTE),
        krow("benchmarked", "Benchmarked", f"avg {pct(avgq)} pass", f"{nb}/{n}",
             color_for(avgq) if not aq.empty else MUTE),
        krow("monitor", "Monitor", "unproven / moderate", n_mon, AMBER if n_mon else MUTE),
        krow("attention", "Needs attention", "low / flagged", n_att, RED if n_att else GREEN),
    ])


    def card(r):
        ran = bool(r["benchmark_ran"]); qv = r["benchmark_quality_pct"]
        qc = color_for(qv) if ran else FAINT
        bdef = int(r["benchmark_questions_defined"])
        ctext, ccol = chip(r)
        instr = int(r["sql_instructions"]) + int(r["text_instructions"])
        meta = (f"{instr} instr · {int(r['sample_questions'])} ex · {bdef} bmk<br>"
                f"{int(r['msgs'])} qs · {int(r['days_since_active'])}d idle")
        sid = r["space_id"]
        nrec = int(rec_counts.get(sid, 0))
        if nrec:
            rec = (f"<a class='gd-recs' href='?space={sid}&ws={quote(ws_val)}#s-improve' target='_top' style='color:{ACCENT}'>"
                   f"💡 {nrec} recommendation{'s' if nrec != 1 else ''} →</a>")
        else:
            rec = (f"<a class='gd-recs' href='?space={sid}&ws={quote(ws_val)}#s-improve' target='_top' style='color:{GREEN}'>"
                   f"✓ no open fixes</a>")
        spark = mini_spark(trend_map.get(sid, []), qc)
        pulse = (f"<div class='gd-qlbl'>Accuracy pulse · {len(trend_map[sid])} runs</div>{spark}"
                 if spark else "")
        return (
            f"<div class='gd-card' style='border-left-color:{ccol}'>"
            f"<a class='gd-cardlink' href='?space={sid}&ws={quote(ws_val)}' target='_top'>"
            f"<div class='gd-name'>{r['title']}</div>"
            f"<span class='gd-chip' style='background:{ccol}1A;color:{ccol}'>{ctext}</span>"
            f"<div class='gd-qlbl'>Benchmark quality</div>"
            f"<div class='gd-qnum' style='color:{qc}'>{pct(qv)}</div>{barhtml(qv, qc)}"
            f"<div class='gd-qsub'>coverage {pct(r['coverage_pct'])} · feedback {pct(r['feedback_positive_pct'])}</div>"
            f"{pulse}"
            f"<div class='gd-meta'>{meta}</div></a>"
            f"{rec}</div>")

    def grid(df):
        return f"<div class='gd-cards'>{''.join(card(r) for _, r in df.iterrows())}</div>"

    def worst_first(df):
        return df.sort_values("benchmark_quality_pct", na_position="first")

    def best_first(df):
        return df.sort_values("benchmark_quality_pct", ascending=False, na_position="last")

    if filt == "proven":
        d = best_first(sc[sc["_band"] == "proven"])
        body = grid(d) if len(d) else "<div class='gd-qsub'>No agents are proven (benchmarked ≥80%) yet.</div>"
    elif filt == "attention":
        d = worst_first(sc[sc["_band"] == "attention"])
        body = grid(d) if len(d) else "<div class='gd-qsub'>Nothing flagged — no low-accuracy or high-impact issues.</div>"
    elif filt == "monitor":
        d = worst_first(sc[sc["_band"] == "monitor"])
        body = grid(d) if len(d) else "<div class='gd-qsub'>Nothing here.</div>"
    elif filt == "benchmarked":
        d = best_first(sc[sc["benchmark_ran"] == True])
        body = grid(d) if len(d) else "<div class='gd-qsub'>No agents have a completed eval run yet.</div>"
    else:  # all -> grouped health bands
        bands = [
            ("proven", "Proven", "benchmarked at ≥80% — trustworthy", GREEN),
            ("monitor", "Monitor", "unproven or moderate — benchmark them to promote", AMBER),
            ("attention", "Needs attention", "low accuracy, flagged, or a high-impact fix", RED),
        ]
        body = ""
        for key, lbl, desc, col in bands:
            d = sc[sc["_band"] == key]
            d = best_first(d) if key == "proven" else worst_first(d)
            if len(d) == 0:
                continue
            body += (f"<div class='gd-band'><span class='dot' style='background:{col}'></span>"
                     f"<span class='bl'>{lbl}</span><span class='bc'>{len(d)}</span>"
                     f"<span class='bd'>{desc}</span></div>{grid(d)}")

    titles = {"proven": "Proven agents", "monitor": "Monitor",
              "attention": "Needs attention", "benchmarked": "Benchmarked agents"}
    base = "" if filt == "all" else titles.get(filt, "")
    who = "" if owner_sel == "all" else owner_sel.split("@")[0]
    _parts = [p for p in (base, who) if p]
    main_h = f"<div class='gd-main-h'>{' · '.join(_parts)}</div>" if _parts else ""
    # 3) Two-pane shell: KPIs on the left, agent list on the right.
    st.markdown(
        f"<div class='gd-shell'>"
        f"<aside class='gd-rail gd-rail-sum'><div class='gd-kwrap'>{rail_kpis}</div>"
        f"<div class='gd-railfoot'>Source: {SOURCE['used']}<br>{CATALOG}.{SCHEMA}"
        f"{('<br>⚠ ' + SOURCE['note']) if SOURCE['note'] else ''}</div>"
        f"</aside>"
        f"<main class='gd-main'>{main_h}{body}</main>"
        f"</div>",
        unsafe_allow_html=True)


# --------------------------------------------------------------------------- detail
def render_detail(space_id):
    sc = scorecard()
    m = sc[sc["space_id"] == space_id]
    if m.empty:
        st.markdown("<a class='gd-clear' href='?' target='_top'>← Back to fleet</a>"
                    "<div class='gd-main-h'>Space not in scope.</div>", unsafe_allow_html=True)
        return
    r = m.iloc[0].to_dict()
    ran = bool(r["benchmark_ran"]); qv = r["benchmark_quality_pct"]; qc = color_for(qv) if ran else FAINT
    bdef = int(r["benchmark_questions_defined"])
    imp = improvements()
    mine = imp[imp["space_id"] == space_id] if not imp.empty else pd.DataFrame()
    has_high = (not mine.empty) and "impact" in mine and (mine["impact"] == "high").any()

    # status chip (mirrors the fleet card)
    if ran and _num(qv) < 50:
        ctext, ccol = ("LOW ACCURACY", RED)
    elif has_high:
        ctext, ccol = ("NEEDS FIX", RED)
    elif ran and _num(qv) >= 80:
        ctext, ccol = ("PROVEN", GREEN)
    elif ran:
        ctext, ccol = ("MODERATE", AMBER)
    elif bdef > 0:
        ctext, ccol = ("DEFINED · NOT RUN", AMBER)
    else:
        ctext, ccol = ("NO BENCHMARK", MUTE)

    instr = int(r["sql_instructions"]) + int(r["text_instructions"])

    # KPI rows double as navigation (click to jump to the section) — no separate duplicate menu.
    def srow(l, v, anchor, c=INK):
        return (f"<a class='gd-krow' href='#{anchor}'><div class='l'>{l}</div>"
                f"<div class='v' style='color:{c};font-size:1.15rem'>{v}</div></a>")
    rail_stats = "".join([
        srow("Quality", pct(qv), "s-quality", qc),
        srow("Coverage", pct(r["coverage_pct"]), "s-quality"),
        srow("Feedback", pct(r["feedback_positive_pct"]), "s-feedback"),
        srow("Richness", instr + int(r["sample_questions"]), "s-rich"),
        srow("Activity", f"{int(r['msgs'])} qs", "s-fresh"),
    ])
    # Only the sections that aren't already a KPI row above.
    _secs = [("s-trend", "Accuracy trend"), ("s-matrix", "Per-question results"),
             ("s-flag", "Flagged questions"), ("s-improve", "Recommendations")]
    secnav = ("<div class='gd-secnav'><div class='gd-secnav-h'>More</div>"
              + "".join(f"<a href='#{sid}'>{lbl}</a>" for sid, lbl in _secs) + "</div>")
    rail = (f"<a class='gd-clear' href='?ws={quote(current_ws())}' target='_top'>← Back to fleet</a>"
            f"<div class='gd-brand-h' style='font-size:1.1rem;margin:8px 0 6px'>{r['title']}</div>"
            f"<span class='gd-chip' style='background:{ccol}1A;color:{ccol}'>{ctext}</span>"
            f"<div style='margin:10px 0 12px'><a class='gd-link' href='{genie_url(space_id)}' target='_blank'>↗ open in Genie</a></div>"
            f"{rail_stats}{secnav}")

    ptile = lambda l, v: f"<div class='gd-ptile'><div class='l'>{l}</div><div class='v'>{v}</div></div>"
    def sec(sid, label):
        return f"<div class='gd-sec' id='{sid}'><span>{label}</span></div>"
    quality = ("<div class='gd-ptiles'>" + ptile("Pass rate", f"<span style='color:{qc}'>{pct(qv)}</span>")
               + ptile("Coverage", pct(r["coverage_pct"])) + ptile("Defined", bdef)
               + ptile("Eval runs", int(r["num_runs"])) + "</div>")
    feedback = ("<div class='gd-ptiles'>" + ptile("Positive", pct(r["feedback_positive_pct"]))
                + ptile("Coverage", pct(r["feedback_coverage_pct"])) + ptile("👍", int(r["thumbs_up"]))
                + ptile("👎", int(r["thumbs_down"])) + "</div>")
    rich = ("<div class='gd-ptiles'>" + ptile("SQL instr.", int(r["sql_instructions"]))
            + ptile("Text instr.", int(r["text_instructions"])) + ptile("Examples", int(r["sample_questions"]))
            + ptile("Tables", int(r["n_tables"])) + "</div>")
    fresh = ("<div class='gd-ptiles'>" + ptile("Questions", int(r["msgs"])) + ptile("Users", int(r["users"]))
             + ptile("Days idle", int(r["days_since_active"])) + ptile("Benchmark Qs", bdef) + "</div>")

    fl = flagged(space_id)
    if fl.empty:
        flag_html = (f"<div class='gd-step' style='border-left-color:{GREEN}'><div class='h'>No thumbs-down</div>"
                     f"<div class='w'>No questions were flagged by users.</div></div>")
    else:
        flag_html = ""
        for _, s in fl.iterrows():
            err = (f"<span class='gd-tag' style='background:{RED}1A;color:{RED}'>{s['error_type']}</span>"
                   if s.get("error_type") else "")
            sql = (f"<div class='gd-ev'>{str(s['sql_generated'])[:400]}</div>" if s.get("sql_generated") else "")
            ts = str(s["created_ts"])[:10]
            flag_html += (f"<div class='gd-step' style='border-left-color:{RED}'>"
                          f"<span class='gd-tag' style='background:{RED}1A;color:{RED}'>👎 thumbs-down</span>{err}"
                          f"<span style='color:{FAINT};font-size:0.74rem'>{ts}</span>"
                          f"<div class='h' style='margin-top:8px'>{s['question']}</div>{sql}</div>")

    if mine.empty:
        imp_html = (f"<div class='gd-step' style='border-left-color:{GREEN}'><div class='h'>No open fixes</div>"
                    f"<div class='w'>Meets the accuracy rubric on the signals we have.</div></div>")
    else:
        order = {"high": 0, "medium": 1, "low": 2}
        mm = mine.assign(_o=mine["impact"].map(order).fillna(3)).sort_values("_o")
        imp_html = ""
        for _, s in mm.iterrows():
            imp_v = str(s["impact"] or "").lower()
            ic = RED if imp_v == "high" else (AMBER if imp_v == "medium" else MUTE)
            src = "agent" if s["source"] == "agent" else "auto"
            imp_html += (f"<div class='gd-step' style='border-left-color:{ic}'>"
                         f"<span class='gd-tag' style='background:{ic}1A;color:{ic}'>{imp_v.upper() or 'STEP'}</span>"
                         f"<span class='gd-tag' style='background:#F0ECE3;color:{MUTE}'>{s['category']}</span>"
                         f"<span class='gd-tag' style='background:#F0ECE3;color:{MUTE}'>{src}</span>"
                         f"<div class='h' style='margin-top:8px'>{s['step']}</div>"
                         f"<div class='w'>{s['why']}</div><div class='gd-ev'>{s['evidence']}</div></div>")

    flag_label = f"Flagged questions · 👎 {len(fl)}"
    main = (f"<div class='gd-main-h'>{r['title']}</div>"
            f"{sec('s-quality', 'Quality · benchmark traces')}{quality}"
            f"{sec('s-feedback', 'Feedback')}{feedback}"
            f"{sec('s-rich', 'Richness · curation')}{rich}"
            f"{sec('s-fresh', 'Freshness · activity')}{fresh}"
            f"{sec('s-trend', 'Benchmark accuracy · over eval runs')}{trend_svg(run_trend(space_id))}"
            f"{sec('s-matrix', 'Per-question results · by run')}{matrix_html(question_matrix(space_id))}"
            f"{sec('s-flag', flag_label)}{flag_html}"
            f"{sec('s-improve', 'Recommendations · how to improve this agent')}{imp_html}")

    st.markdown(f"{page_top(current_ws())}<div class='gd-shell'><aside class='gd-rail'>{rail}</aside>"
                f"<main class='gd-main'>{main}</main></div>", unsafe_allow_html=True)


# --------------------------------------------------------------------------- router
_sel = st.query_params.get("space")
if _sel:
    render_detail(_sel)
else:
    render_summary()

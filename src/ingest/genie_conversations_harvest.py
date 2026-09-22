# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Genie Space Monitoring & Analytics — Complete Notebook
# MAGIC
# MAGIC This notebook provides **end-to-end observability** for Databricks AI/BI Genie Spaces.
# MAGIC It combines:
# MAGIC
# MAGIC 1. **Genie Conversation History Extraction** — Harvests raw prompts, messages, and resolves user IDs to emails via SCIM API
# MAGIC 2. **Full Monitoring Pipeline** — Creates Delta tables (`genie_spaces`, `genie_conversations`, `genie_message_details`) via Genie REST API
# MAGIC 3. **Analytics** — See `genie_monitoring_analytics.py` for 20 ready-to-use SQL queries covering usage, response quality, feedback, errors, trends, and a composite Genie Score
# MAGIC 4. **System Table Integration** — Joins with `system.access.audit` and `system.query.history` for enriched analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **What you get:**
# MAGIC - `genie_spaces` — Space-level metadata (name, description, warehouse, owner)
# MAGIC - `genie_conversations` — Conversation summaries with message counts and duration
# MAGIC - `genie_message_details` — Full message details including SQL generated, response time, feedback, and errors
# MAGIC - `genie_{space_id}_prompts` — Per-space raw prompt table with resolved usernames

# COMMAND ----------

# DBTITLE 1,Configuration Section
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install requests
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set configuration parameters
dbutils.widgets.text("CATALOG", "main", "Catalog")
dbutils.widgets.text("SCHEMA", "genie_monitoring", "Schema")
dbutils.widgets.text("SPACE_IDS_FILTER", "", "Space IDs Filter (comma-separated)")
dbutils.widgets.text("OWNER_PREFIX", "", "Owner parent_path prefix (owner-scoped harvest)")

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")

# Optional: restrict to specific space IDs. If empty, scans ALL spaces.
_raw_filter = dbutils.widgets.get("SPACE_IDS_FILTER") or ""
SPACE_IDS_FILTER = [s.strip() for s in _raw_filter.split(",") if s.strip()]

# Genie Doctor enhancement: owner-scoped harvest. When set, keep only spaces whose
# parent_path starts with OWNER_PREFIX (resolved via concurrent detail calls below).
OWNER_PREFIX = (dbutils.widgets.get("OWNER_PREFIX") or "").strip()

# COMMAND ----------

# DBTITLE 1,API Setup Section
# MAGIC %md
# MAGIC ## API Setup
# MAGIC
# MAGIC Sets up authentication and helper functions for the Genie REST API.

# COMMAND ----------

# DBTITLE 1,API helpers and authentication
import requests
import json
import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    BooleanType, TimestampType, DoubleType, ArrayType
)

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}
BASE = f"{host}/api/2.0/genie"


def api_get(path, max_retries=5):
    """GET helper with exponential backoff on 429/5xx/transport errors and explicit error logging."""
    backoff = 1.0
    url = f"{BASE}{path}"
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                print(f"  [api_get] transport error after {max_retries} retries: {e} url={path}")
                return None
            time.sleep(backoff)
            backoff *= 2
            continue

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            if attempt == max_retries - 1:
                print(f"  [api_get] {resp.status_code} after {max_retries} retries: {resp.text[:200]} url={path}")
                return None
            time.sleep(backoff)
            backoff *= 2
            continue
        # Non-retryable (403, 404, etc.)
        print(f"  [api_get] {resp.status_code}: {resp.text[:200]} url={path}")
        return None


def ts_to_dt(epoch_ms):
    """Convert epoch milliseconds to datetime (tz-aware UTC), or None."""
    if epoch_ms:
        return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return None

print(f"Workspace: {host}")
print("API setup complete.")

# COMMAND ----------

# DBTITLE 1,Schema Creation Section
# MAGIC %md
# MAGIC ## Create Target Schema & Tables

# COMMAND ----------

# DBTITLE 1,Create catalog, schema and Delta tables
# Self-contained bootstrap: create the gold catalog + schema if they don't exist yet.
# CREATE CATALOG needs the run identity to hold CREATE CATALOG on the metastore; on a
# workspace where the catalog already exists (or the identity lacks that privilege but
# the catalog is present) this is a harmless no-op.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
except Exception as _e:
    print(f"[bootstrap] CREATE CATALOG {CATALOG} skipped: {str(_e)[:160]}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# -- Table 1: Genie Spaces --
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.genie_spaces (
    space_id            STRING      COMMENT 'Genie Space ID',
    title               STRING      COMMENT 'Space display name',
    description         STRING      COMMENT 'Space description',
    warehouse_id        STRING      COMMENT 'SQL warehouse attached to this space',
    parent_path         STRING      COMMENT 'Workspace folder path where the space lives',
    harvested_at        TIMESTAMP   COMMENT 'When this row was harvested'
)
USING DELTA
COMMENT 'Genie Space metadata harvested from GET /api/2.0/genie/spaces.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
"""
)

# -- Table 2: Conversations --
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.genie_conversations (
    space_id                STRING      COMMENT 'Parent Genie Space ID',
    space_title             STRING      COMMENT 'Parent Genie Space name',
    conversation_id         STRING      COMMENT 'Conversation ID',
    conversation_title      STRING      COMMENT 'Conversation title (auto-generated from first message)',
    user_id                 LONG        COMMENT 'Numeric user ID who started the conversation',
    created_ts              TIMESTAMP   COMMENT 'Conversation created timestamp',
    last_updated_ts         TIMESTAMP   COMMENT 'Conversation last updated timestamp',
    message_count           INT         COMMENT 'Total messages in this conversation',
    completed_count         INT         COMMENT 'Messages with COMPLETED status',
    failed_count            INT         COMMENT 'Messages with FAILED status',
    first_message_ts        TIMESTAMP   COMMENT 'Timestamp of the first message',
    last_message_ts         TIMESTAMP   COMMENT 'Timestamp of the last message',
    conversation_duration_sec DOUBLE    COMMENT 'Seconds between first and last message',
    has_sql_output          BOOLEAN     COMMENT 'Whether any message produced SQL',
    has_errors              BOOLEAN     COMMENT 'Whether any message had errors',
    has_feedback            BOOLEAN     COMMENT 'Whether any message received feedback',
    harvested_at            TIMESTAMP   COMMENT 'When this row was harvested'
)
USING DELTA
COMMENT 'Genie conversation-level summary harvested from the API.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
"""
)

# -- Table 3: Message Details (comprehensive) --
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.genie_message_details (
    -- Identifiers
    space_id                STRING      COMMENT 'Genie Space ID',
    space_title             STRING      COMMENT 'Genie Space name',
    conversation_id         STRING      COMMENT 'Conversation ID',
    conversation_title      STRING      COMMENT 'Conversation title',
    message_id              STRING      COMMENT 'Message ID',
    user_id                 LONG        COMMENT 'Numeric user ID from Genie API',

    -- Content
    user_question           STRING      COMMENT 'Original user question / message content',
    status                  STRING      COMMENT 'Message status: COMPLETED, FAILED, EXECUTING_QUERY, etc.',

    -- Timing
    created_ts              TIMESTAMP   COMMENT 'Message created timestamp',
    completed_ts            TIMESTAMP   COMMENT 'Message last updated timestamp',
    response_time_sec       DOUBLE      COMMENT 'Time to respond in seconds (completed - created)',

    -- Query attachment (primary SQL output)
    sql_generated           STRING      COMMENT 'SQL query Genie generated',
    sql_title               STRING      COMMENT 'Title of the SQL query attachment',
    sql_description         STRING      COMMENT 'Natural language description of the SQL',
    sql_attachment_id       STRING      COMMENT 'Attachment ID for the query',
    sql_statement_id        STRING      COMMENT 'Statement ID for the first executed SQL attachment',
    sql_statement_ids       STRING      COMMENT 'JSON array of all statement IDs across all query attachments',
    sql_parameters          STRING      COMMENT 'JSON array of query parameters (keyword, sql_type, value)',
    sql_last_updated_ts     TIMESTAMP   COMMENT 'When the query attachment was last updated',

    -- Query result metadata
    row_count               LONG        COMMENT 'Number of rows in query result',
    is_truncated            BOOLEAN     COMMENT 'Whether the result was truncated',

    -- Legacy query result (top-level, deprecated)
    legacy_statement_id     STRING      COMMENT 'Statement ID from legacy query_result field',
    legacy_row_count        LONG        COMMENT 'Row count from legacy query_result field',
    legacy_is_truncated     BOOLEAN     COMMENT 'Truncation flag from legacy query_result',

    -- Text attachment (Genie explanation / clarification)
    text_response           STRING      COMMENT 'Genie text explanation or clarification',
    text_purpose            STRING      COMMENT 'Purpose of the text attachment (e.g., CLARIFICATION)',
    text_attachment_id      STRING      COMMENT 'Attachment ID for the text response',

    -- Suggested follow-up questions
    suggested_questions     STRING      COMMENT 'JSON array of suggested follow-up questions',

    -- Error details
    error_type              STRING      COMMENT 'Error type if message failed (60+ possible types)',
    error_message           STRING      COMMENT 'Error detail string if message failed',

    -- Feedback
    feedback_rating         STRING      COMMENT 'User feedback: THUMBS_UP, THUMBS_DOWN, or null',

    -- Attachment summary
    attachment_count        INT         COMMENT 'Total number of attachments on the message',
    has_query_attachment    BOOLEAN     COMMENT 'Whether message has a SQL query attachment',
    has_text_attachment     BOOLEAN     COMMENT 'Whether message has a text response attachment',
    has_suggested_questions BOOLEAN     COMMENT 'Whether message has suggested follow-up questions',

    -- Conversation position
    message_position        INT         COMMENT 'Position of this message in the conversation (1-based)',
    is_first_message        BOOLEAN     COMMENT 'Whether this is the first message in the conversation',

    -- Raw data
    raw_attachments_json    STRING      COMMENT 'Full JSON of all attachments for advanced analysis',

    -- Metadata
    harvested_at            TIMESTAMP   COMMENT 'When this row was harvested from the API'
)
USING DELTA
COMMENT 'Comprehensive Genie message details harvested from the REST API: response_time, SQL generated, error statements, text explanations, suggested questions, and feedback.'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
"""
)

print(f"Tables ready in {CATALOG}.{SCHEMA}")

# Set default catalog/schema so analytics %sql cells below resolve bare table names
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Data Harvesting Section
# MAGIC %md
# MAGIC ## Harvest All Data from Genie Spaces
# MAGIC
# MAGIC Iterates through all Genie Spaces (or filtered subset), extracting conversations and messages via the REST API.

# COMMAND ----------

# DBTITLE 1,Define harvest_space function
def harvest_space(space_info, cutoff_dt=None):
    """Harvest conversations and messages from a single Genie Space.

    cutoff_dt: timezone-aware UTC datetime. When set, only conversations with
    last_updated_timestamp >= cutoff_dt are fetched (incremental mode).
    None means fetch everything (full harvest / first run).

    Returns (space_row, conversation_rows, message_rows).
    """
    now = datetime.now(timezone.utc)
    sid = space_info.get("space_id")
    stitle = space_info.get("title", "")

    # -- Space row --
    space_row = {
        "space_id": sid,
        "title": stitle,
        "description": space_info.get("description"),
        "warehouse_id": space_info.get("warehouse_id"),
        "parent_path": space_info.get("parent_path"),
        "harvested_at": now,
    }

    convo_rows = []
    msg_rows = []

    # Get full space details (has more fields than list response)
    space_detail = api_get(f"/spaces/{sid}")
    if space_detail:
        space_row["description"] = (
            space_detail.get("description") or space_row["description"]
        )
        space_row["warehouse_id"] = (
            space_detail.get("warehouse_id") or space_row["warehouse_id"]
        )
        space_row["parent_path"] = (
            space_detail.get("parent_path") or space_row["parent_path"]
        )

    # -- List conversations with pagination (try include_all first, fallback without) --
    def _paginated_conversations(include_all: bool):
        out = []
        page_token = None
        base_url = f"{BASE}/spaces/{sid}/conversations"
        if include_all:
            base_url += "?include_all=true"
        while True:
            sep = "&" if "?" in base_url else "?"
            url = base_url + (f"{sep}page_token={page_token}" if page_token else "")
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            body = r.json() or {}
            page = body.get("conversations", []) or []

            if cutoff_dt is None:
                # Full harvest — collect everything.
                out.extend(page)
            else:
                # Incremental: API returns conversations newest-updated-first.
                # Collect those updated at or after the cutoff; stop as soon as
                # we see one that is older (all remaining pages will be older too).
                stop = False
                for convo in page:
                    updated_ms = convo.get("last_updated_timestamp")
                    if updated_ms and ts_to_dt(updated_ms) < cutoff_dt:
                        stop = True
                        break
                    out.append(convo)
                if stop:
                    break

            page_token = body.get("next_page_token")
            if not page_token:
                break
        return out

    try:
        conversations = _paginated_conversations(include_all=True)
        print(f"     [include_all=true] OK for space {sid} ({len(conversations)} convos)")
    except Exception:
        try:
            conversations = _paginated_conversations(include_all=False)
            print(f"     [include_all=false] Fallback for space {sid} ({len(conversations)} convos, no MANAGE access)")
        except Exception as e:
            print(f"     [ERROR] Could not list conversations for space {sid}: {e}")
            return space_row, convo_rows, msg_rows

    if not conversations:
        return space_row, convo_rows, msg_rows

    for convo in conversations:
        cid = convo.get("conversation_id") or convo.get("id")
        if not cid:
            continue

        convo_title = convo.get("title")
        convo_user_id = convo.get("user_id")
        convo_created = ts_to_dt(convo.get("created_timestamp"))
        convo_updated = ts_to_dt(convo.get("last_updated_timestamp"))

        # -- List messages in this conversation (paginated) --
        messages_acc = []
        _pt = None
        _msgs_url = f"{BASE}/spaces/{sid}/conversations/{cid}/messages"
        try:
            while True:
                _r = requests.get(_msgs_url, headers=headers, params={"page_token": _pt} if _pt else None)
                _r.raise_for_status()
                _body = _r.json() or {}
                messages_acc.extend(_body.get("messages", []) or [])
                _pt = _body.get("next_page_token")
                if not _pt:
                    break
        except Exception as _e:
            print(f"     [WARN] message fetch failed for convo {cid}: {_e}")
        msgs_resp = {"messages": messages_acc} if messages_acc else None
        if not msgs_resp or not msgs_resp.get("messages"):
            convo_rows.append(
                {
                    "space_id": sid,
                    "space_title": stitle,
                    "conversation_id": cid,
                    "conversation_title": convo_title,
                    "user_id": convo_user_id,
                    "created_ts": convo_created,
                    "last_updated_ts": convo_updated,
                    "message_count": 0,
                    "completed_count": 0,
                    "failed_count": 0,
                    "first_message_ts": None,
                    "last_message_ts": None,
                    "conversation_duration_sec": None,
                    "has_sql_output": False,
                    "has_errors": False,
                    "has_feedback": False,
                    "harvested_at": now,
                }
            )
            continue

        messages = msgs_resp["messages"]
        convo_has_sql = False
        convo_has_errors = False
        convo_has_feedback = False
        completed_count = 0
        failed_count = 0
        first_msg_ts = None
        last_msg_ts = None

        for idx, msg in enumerate(messages):
            mid = msg.get("message_id") or msg.get("id")
            created = msg.get("created_timestamp")
            updated = msg.get("last_updated_timestamp")
            created_dt = ts_to_dt(created)
            completed_dt = ts_to_dt(updated)

            # Track conversation-level timestamps
            if created_dt:
                if first_msg_ts is None or created_dt < first_msg_ts:
                    first_msg_ts = created_dt
                if last_msg_ts is None or created_dt > last_msg_ts:
                    last_msg_ts = created_dt

            # Response time
            response_time = None
            if created and updated:
                response_time = (updated - created) / 1000.0

            status = msg.get("status")
            if status == "COMPLETED":
                completed_count += 1
            elif status == "FAILED":
                failed_count += 1

            # -- Parse ALL attachments --
            attachments = msg.get("attachments", [])
            attachment_count = len(attachments)

            # Query attachment fields
            sql_generated = None
            sql_title = None
            sql_description = None
            sql_attachment_id = None
            sql_statement_id = None
            sql_statement_ids = []
            sql_parameters = None
            sql_last_updated = None
            row_count = None
            is_truncated = None
            has_query = False

            # Text attachment fields
            text_response = None
            text_purpose = None
            text_attachment_id = None
            has_text = False

            # Suggested questions
            suggested_questions = None
            has_suggestions = False

            for att in attachments:
                aid = att.get("attachment_id")

                # Query attachment
                query_att = att.get("query")
                if query_att:
                    sid_val = query_att.get("statement_id")
                    if sid_val:
                        sql_statement_ids.append(sid_val)
                if query_att and not has_query:
                    has_query = True
                    convo_has_sql = True
                    sql_generated = query_att.get("query")
                    sql_title = query_att.get("title")
                    sql_description = query_att.get("description")
                    sql_attachment_id = aid or query_att.get("id")
                    sql_statement_id = query_att.get("statement_id")
                    sql_last_updated = ts_to_dt(query_att.get("last_updated_timestamp"))

                    # Parameters
                    params = query_att.get("parameters")
                    if params:
                        sql_parameters = json.dumps(params)

                    # Result metadata
                    meta = query_att.get("query_result_metadata") or {}
                    row_count = meta.get("row_count")
                    is_truncated = meta.get("is_truncated")

                # Text attachment
                text_att = att.get("text")
                if text_att and not has_text:
                    has_text = True
                    text_response = text_att.get("content")
                    text_purpose = text_att.get("purpose")
                    text_attachment_id = aid or text_att.get("id")

                # Suggested questions attachment
                sq_att = att.get("suggested_questions")
                if sq_att and not has_suggestions:
                    has_suggestions = True
                    questions = sq_att.get("questions", [])
                    if questions:
                        suggested_questions = json.dumps(questions)

            # Legacy top-level query_result
            legacy_qr = msg.get("query_result")
            legacy_statement_id = legacy_qr.get("statement_id") if legacy_qr else None
            legacy_row_count = legacy_qr.get("row_count") if legacy_qr else None
            legacy_is_truncated = legacy_qr.get("is_truncated") if legacy_qr else None

            # Error info
            error = msg.get("error")
            error_type = error.get("type") if error else None
            error_message = error.get("error") if error else None
            if error:
                convo_has_errors = True

            # Feedback
            feedback = msg.get("feedback")
            feedback_rating = feedback.get("rating") if feedback else None
            if feedback:
                convo_has_feedback = True

            # Raw attachments JSON for anything we might have missed
            raw_attachments = json.dumps(attachments) if attachments else None

            msg_rows.append(
                {
                    "space_id": sid,
                    "space_title": stitle,
                    "conversation_id": cid,
                    "conversation_title": convo_title,
                    "message_id": mid,
                    "user_id": msg.get("user_id"),
                    "user_question": msg.get("content"),
                    "status": status,
                    "created_ts": created_dt,
                    "completed_ts": completed_dt,
                    "response_time_sec": response_time,
                    "sql_generated": sql_generated,
                    "sql_title": sql_title,
                    "sql_description": sql_description,
                    "sql_attachment_id": sql_attachment_id,
                    "sql_statement_id": sql_statement_id,
                    "sql_statement_ids": json.dumps(sql_statement_ids) if sql_statement_ids else None,
                    "sql_parameters": sql_parameters,
                    "sql_last_updated_ts": sql_last_updated,
                    "row_count": row_count,
                    "is_truncated": is_truncated,
                    "legacy_statement_id": legacy_statement_id,
                    "legacy_row_count": legacy_row_count,
                    "legacy_is_truncated": legacy_is_truncated,
                    "text_response": text_response,
                    "text_purpose": text_purpose,
                    "text_attachment_id": text_attachment_id,
                    "suggested_questions": suggested_questions,
                    "error_type": error_type,
                    "error_message": error_message,
                    "feedback_rating": feedback_rating,
                    "attachment_count": attachment_count,
                    "has_query_attachment": has_query,
                    "has_text_attachment": has_text,
                    "has_suggested_questions": has_suggestions,
                    "message_position": idx + 1,
                    "is_first_message": idx == 0,
                    "raw_attachments_json": raw_attachments,
                    "harvested_at": now,
                }
            )

        # Build conversation summary row
        convo_duration = None
        if first_msg_ts and last_msg_ts and first_msg_ts != last_msg_ts:
            convo_duration = (last_msg_ts - first_msg_ts).total_seconds()

        convo_rows.append(
            {
                "space_id": sid,
                "space_title": stitle,
                "conversation_id": cid,
                "conversation_title": convo_title,
                "user_id": convo_user_id,
                "created_ts": convo_created,
                "last_updated_ts": convo_updated,
                "message_count": len(messages),
                "completed_count": completed_count,
                "failed_count": failed_count,
                "first_message_ts": first_msg_ts,
                "last_message_ts": last_msg_ts,
                "conversation_duration_sec": convo_duration,
                "has_sql_output": convo_has_sql,
                "has_errors": convo_has_errors,
                "has_feedback": convo_has_feedback,
                "harvested_at": now,
            }
        )

    return space_row, convo_rows, msg_rows

# COMMAND ----------

# DBTITLE 1,Compute incremental harvest cutoff
# Default cutoff: now - 2 hours. If there is a prior harvest, use MAX(harvested_at) - 2 hours instead.
_default_cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
try:
    row = spark.sql(
        f"SELECT MAX(harvested_at) AS max_ts FROM {CATALOG}.{SCHEMA}.genie_conversations"
    ).collect()[0]
    max_ts = row["max_ts"]
    if max_ts:
        harvest_cutoff_dt = max_ts.astimezone(timezone.utc) - timedelta(hours=2)
    else:
        harvest_cutoff_dt = None  # empty table → full harvest
except Exception:
    harvest_cutoff_dt = _default_cutoff  # unreadable table → safe default
print(f"Incremental mode: fetching conversations updated >= {harvest_cutoff_dt.isoformat() if harvest_cutoff_dt else 'ALL (full harvest — empty table)'} UTC")

# COMMAND ----------

# DBTITLE 1,Run harvesting across all spaces
from concurrent.futures import ThreadPoolExecutor

all_spaces = []
if SPACE_IDS_FILTER:
    # FAST PATH: fetch each requested space DIRECTLY by id. Avoids enumerating the whole
    # workspace (5k+ spaces) and — critically — returns spaces the /spaces list omits, so
    # the requested set is harvested completely.
    def _byid(sid):
        d = api_get(f"/spaces/{sid}")
        if d and d.get("space_id"):
            return d
        print(f"  [byid] could not fetch space {sid}")
        return None
    with ThreadPoolExecutor(max_workers=16) as _ex:
        all_spaces = [s for s in _ex.map(_byid, SPACE_IDS_FILTER) if s]
    print(f"[byid] fetched {len(all_spaces)}/{len(SPACE_IDS_FILTER)} requested spaces directly")
else:
    # Full enumeration (paginated) — used only when no explicit space list is given.
    _pt = None
    while True:
        _url = f"{BASE}/spaces" + (f"?page_token={_pt}" if _pt else "")
        _r = requests.get(_url, headers=headers)
        if _r.status_code != 200:
            print(f"[ERROR] /spaces returned {_r.status_code}: {_r.text[:200]}")
            break
        _body = _r.json() or {}
        all_spaces.extend(_body.get("spaces", []) or [])
        _pt = _body.get("next_page_token")
        if not _pt:
            break
    # Owner-scope filter (parent_path comes only from the detail call).
    if OWNER_PREFIX:
        def _owned(s):
            d = api_get(f"/spaces/{s.get('space_id')}") or {}
            return s if (d.get("parent_path") or "").startswith(OWNER_PREFIX) else None
        with ThreadPoolExecutor(max_workers=16) as _ex:
            all_spaces = [s for s in _ex.map(_owned, all_spaces) if s]
        print(f"[owner-scope] {len(all_spaces)} spaces under {OWNER_PREFIX}")

print(f"Harvesting from {len(all_spaces)} Genie Space(s)...\n")

all_space_rows = []
all_convo_rows = []
all_msg_rows = []

for space in all_spaces:
    sid = space.get("space_id")
    title = space.get("title", "")
    print(f"  -> Space: {title} ({sid})")
    space_row, convo_rows, msg_rows = harvest_space(space, cutoff_dt=harvest_cutoff_dt)
    all_space_rows.append(space_row)
    all_convo_rows.extend(convo_rows)
    all_msg_rows.extend(msg_rows)
    print(f"     {len(convo_rows)} conversations, {len(msg_rows)} messages")

print(f"\nTotals: {len(all_space_rows)} spaces, {len(all_convo_rows)} conversations, {len(all_msg_rows)} messages")

# COMMAND ----------

# DBTITLE 1,Write to Delta Section
# MAGIC %md
# MAGIC ## Write to Delta (Merge/Upsert)

# COMMAND ----------

# DBTITLE 1,Upsert spaces to Delta
# -- Upsert Spaces --
SPACE_SCHEMA = StructType(
    [
        StructField("space_id", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("warehouse_id", StringType()),
        StructField("parent_path", StringType()),
        StructField("harvested_at", TimestampType()),
    ]
)

if all_space_rows:
    df_spaces = spark.createDataFrame(all_space_rows, schema=SPACE_SCHEMA)
    df_spaces.createOrReplaceTempView("genie_new_spaces")
    spark.sql(
        f"""
        MERGE INTO {CATALOG}.{SCHEMA}.genie_spaces AS t
        USING genie_new_spaces AS s
        ON t.space_id = s.space_id
        WHEN MATCHED THEN UPDATE SET
            t.title = s.title,
            t.description = s.description,
            t.warehouse_id = s.warehouse_id,
            t.parent_path = s.parent_path,
            t.harvested_at = s.harvested_at
        WHEN NOT MATCHED THEN INSERT (space_id, title, description, warehouse_id, parent_path, harvested_at)
            VALUES (s.space_id, s.title, s.description, s.warehouse_id, s.parent_path, s.harvested_at)
    """
    )
    print(f"Upserted {len(all_space_rows)} space rows")

# COMMAND ----------

# DBTITLE 1,Upsert conversations to Delta
# -- Upsert Conversations --
CONVO_SCHEMA = StructType(
    [
        StructField("space_id", StringType()),
        StructField("space_title", StringType()),
        StructField("conversation_id", StringType()),
        StructField("conversation_title", StringType()),
        StructField("user_id", LongType()),
        StructField("created_ts", TimestampType()),
        StructField("last_updated_ts", TimestampType()),
        StructField("message_count", IntegerType()),
        StructField("completed_count", IntegerType()),
        StructField("failed_count", IntegerType()),
        StructField("first_message_ts", TimestampType()),
        StructField("last_message_ts", TimestampType()),
        StructField("conversation_duration_sec", DoubleType()),
        StructField("has_sql_output", BooleanType()),
        StructField("has_errors", BooleanType()),
        StructField("has_feedback", BooleanType()),
        StructField("harvested_at", TimestampType()),
    ]
)

if all_convo_rows:
    df_convos = spark.createDataFrame(all_convo_rows, schema=CONVO_SCHEMA)
    df_convos.createOrReplaceTempView("genie_new_conversations")
    spark.sql(
        f"""
        MERGE INTO {CATALOG}.{SCHEMA}.genie_conversations AS t
        USING genie_new_conversations AS s
        ON t.space_id = s.space_id AND t.conversation_id = s.conversation_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    print(f"Upserted {len(all_convo_rows)} conversation rows")

# COMMAND ----------

# DBTITLE 1,Upsert messages to Delta
# -- Upsert Messages --
MSG_SCHEMA = StructType(
    [
        StructField("space_id", StringType()),
        StructField("space_title", StringType()),
        StructField("conversation_id", StringType()),
        StructField("conversation_title", StringType()),
        StructField("message_id", StringType()),
        StructField("user_id", LongType()),
        StructField("user_question", StringType()),
        StructField("status", StringType()),
        StructField("created_ts", TimestampType()),
        StructField("completed_ts", TimestampType()),
        StructField("response_time_sec", DoubleType()),
        StructField("sql_generated", StringType()),
        StructField("sql_title", StringType()),
        StructField("sql_description", StringType()),
        StructField("sql_attachment_id", StringType()),
        StructField("sql_statement_id", StringType()),
        StructField("sql_statement_ids", StringType()),
        StructField("sql_parameters", StringType()),
        StructField("sql_last_updated_ts", TimestampType()),
        StructField("row_count", LongType()),
        StructField("is_truncated", BooleanType()),
        StructField("legacy_statement_id", StringType()),
        StructField("legacy_row_count", LongType()),
        StructField("legacy_is_truncated", BooleanType()),
        StructField("text_response", StringType()),
        StructField("text_purpose", StringType()),
        StructField("text_attachment_id", StringType()),
        StructField("suggested_questions", StringType()),
        StructField("error_type", StringType()),
        StructField("error_message", StringType()),
        StructField("feedback_rating", StringType()),
        StructField("attachment_count", IntegerType()),
        StructField("has_query_attachment", BooleanType()),
        StructField("has_text_attachment", BooleanType()),
        StructField("has_suggested_questions", BooleanType()),
        StructField("message_position", IntegerType()),
        StructField("is_first_message", BooleanType()),
        StructField("raw_attachments_json", StringType()),
        StructField("harvested_at", TimestampType()),
    ]
)

if all_msg_rows:
    df_msgs = spark.createDataFrame(all_msg_rows, schema=MSG_SCHEMA)
    df_msgs.createOrReplaceTempView("genie_new_messages")
    spark.sql(
        f"""
        MERGE INTO {CATALOG}.{SCHEMA}.genie_message_details AS t
        USING genie_new_messages AS s
        ON t.space_id = s.space_id
           AND t.conversation_id = s.conversation_id
           AND t.message_id = s.message_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    print(f"Upserted {len(all_msg_rows)} message rows")


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Curation + benchmark eval harvest (single-process, same scoped spaces)
# Harvest instructions, curated questions, data-room meta, eval runs, and Level-3
# eval results for the SAME already-scoped spaces (all_spaces). Full-snapshot overwrite.
from concurrent.futures import ThreadPoolExecutor
_DR = f"{host}/api/2.0/data-rooms"
_GS = f"{host}/api/2.0/genie/spaces"


def _cget(url):
    try:
        r = requests.get(url, headers=headers, timeout=30)
        return r.json() if r.status_code == 200 else {}
    except Exception:
        return {}


def _ci(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _harvest_curation(space):
    sid = space.get("space_id")
    er = _cget(f"{_GS}/{sid}/eval-runs").get("eval_runs", []) or []
    ins = _cget(f"{_DR}/{sid}/instructions").get("instructions", []) or []
    cq = _cget(f"{_DR}/{sid}/curated-questions").get("curated_questions", []) or []
    meta = _cget(f"{_DR}/{sid}")
    res = []
    if er:
        latest = max(er, key=lambda r: r.get("created_timestamp", 0))
        rid = latest.get("eval_run_id")
        for item in _cget(f"{_GS}/{sid}/eval-runs/{rid}/results").get("eval_results", []) or []:
            det = _cget(f"{_GS}/{sid}/eval-runs/{rid}/results/{item.get('result_id')}")
            res.append((sid, rid, item.get("benchmark_question_id"), item.get("question"),
                        det.get("assessment"), str(det.get("expected_response"))[:6000],
                        str(det.get("actual_response"))[:6000], item.get("result_id")))
    pp = space.get("parent_path") or ""
    meta["_owner"] = pp.split("/Users/")[-1].split("/")[0] if "/Users/" in pp else (pp or "unknown")
    return sid, space.get("title"), er, ins, cq, meta, res


_eval_rows, _instr_rows, _cq_rows, _meta_rows, _result_rows = [], [], [], [], []
with ThreadPoolExecutor(max_workers=16) as _ex:
    for sid, title, er, ins, cq, meta, res in _ex.map(_harvest_curation, all_spaces):
        for r in er:
            _eval_rows.append((sid, r.get("eval_run_id"), r.get("eval_run_status"),
                               _ci(r.get("num_questions")), _ci(r.get("num_correct")),
                               _ci(r.get("num_needs_review")), _ci(r.get("num_done")),
                               _ci(r.get("created_timestamp")), str(r.get("run_by_user"))))
        for r in ins:
            _instr_rows.append((sid, r.get("instruction_id"), r.get("instruction_type"),
                                r.get("title"), r.get("content"), r.get("usage_guidance"),
                                r.get("instruction_status"), str(r.get("use_as_tool"))))
        for r in cq:
            _cq_rows.append((sid, r.get("curated_question_id"), r.get("question_type"),
                             r.get("question_text"), r.get("answer_text"), r.get("eval_note"),
                             str(r.get("is_deprecated")).lower() == "true"))
        _meta_rows.append((sid, title, meta.get("run_as_type"), _ci(meta.get("last_updated_timestamp")),
                           [str(t) for t in (meta.get("table_identifiers") or [])], meta.get("_owner")))
        _result_rows.extend(res)

_ES = StructType([StructField("space_id", StringType()), StructField("eval_run_id", StringType()),
    StructField("eval_run_status", StringType()), StructField("num_questions", IntegerType()),
    StructField("num_correct", IntegerType()), StructField("num_needs_review", IntegerType()),
    StructField("num_done", IntegerType()), StructField("created_timestamp", LongType()),
    StructField("run_by_user", StringType())])
_IS = StructType([StructField("space_id", StringType()), StructField("instruction_id", StringType()),
    StructField("instruction_type", StringType()), StructField("title", StringType()),
    StructField("content", StringType()), StructField("usage_guidance", StringType()),
    StructField("instruction_status", StringType()), StructField("use_as_tool", StringType())])
_CS = StructType([StructField("space_id", StringType()), StructField("curated_question_id", StringType()),
    StructField("question_type", StringType()), StructField("question_text", StringType()),
    StructField("answer_text", StringType()), StructField("eval_note", StringType()),
    StructField("is_deprecated", BooleanType())])
_MS = StructType([StructField("space_id", StringType()), StructField("title", StringType()),
    StructField("run_as_type", StringType()), StructField("last_updated_timestamp", LongType()),
    StructField("table_identifiers", ArrayType(StringType())), StructField("owner", StringType())])
_RS = StructType([StructField("space_id", StringType()), StructField("eval_run_id", StringType()),
    StructField("benchmark_question_id", StringType()), StructField("question", StringType()),
    StructField("assessment", StringType()), StructField("expected_response", StringType()),
    StructField("actual_response", StringType()), StructField("result_id", StringType())])


def _cwrite(rows, schema, name):
    (spark.createDataFrame(rows, schema).write.mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"[curation] {len(rows):>4} -> {name}")


_cwrite(_eval_rows, _ES, "genie_eval_runs")
_cwrite(_instr_rows, _IS, "genie_instructions")
_cwrite(_cq_rows, _CS, "genie_curated_qs")
_cwrite(_meta_rows, _MS, "genie_space_meta")
_cwrite(_result_rows, _RS, "genie_eval_results")
print("[curation] done")

#!/usr/bin/env bash
# Genie Doctor — one-command deployment for any workspace.
#
#   ./deploy.sh <profile> <target>
#     e.g.  ./deploy.sh e2-demo-fe e2
#
# Two serving "versions", chosen by the `serving_backend` variable on the target:
#   lakehouse (default) -> app reads gold Delta via the SQL warehouse. Simple one-pass
#                          deploy: deploy -> run job (build gold) -> start app -> grants.
#   lakebase            -> app reads a Postgres mirror. Needs the async sequence below
#                          (Lakebase provisions async; synced tables need the gold tables
#                          to exist first): deploy -> wait -> run job -> deploy -> app -> grants.
# Idempotent: re-run any time.
set -uo pipefail
P="${1:?usage: ./deploy.sh <profile> <target>}"
T="${2:?usage: ./deploy.sh <profile> <target>}"
cd "$(dirname "$0")"

var() { python3 -c "import sys,json;print((json.load(sys.stdin).get('variables',{}).get('$1') or {}).get('value') or '')"; }

echo "==> Reading resolved config for target '$T'..."
CFG="$(databricks bundle validate -t "$T" -p "$P" -o json 2>/dev/null)"
SB="$(printf '%s' "$CFG" | var serving_backend)"; SB="${SB:-lakehouse}"
INST="$(printf '%s' "$CFG" | var lakebase_instance)"
CAT="$(printf '%s' "$CFG" | var catalog)"
SCH="$(printf '%s' "$CFG" | var schema)"
WH="$(printf '%s' "$CFG" | var warehouse_id)"
LDB="$(printf '%s' "$CFG" | var lakebase_db)"
echo "    serving_backend=$SB  gold=$CAT.$SCH  warehouse=$WH"
[ -z "$CAT" ] && { echo "!! could not read config — check profile/target"; exit 1; }

# ---------------------------------------------------------------------------
# LAKEHOUSE (default) — no Lakebase; the app reads gold Delta via the warehouse.
# ---------------------------------------------------------------------------
if [ "$SB" != "lakebase" ]; then
  echo "==> [1/4] Deploy (pipeline + job + app; no Lakebase)"
  databricks bundle deploy -t "$T" -p "$P" || { echo "!! deploy failed"; exit 1; }

  echo "==> [2/4] Run the job once (builds the gold tables; also the daily trigger's first fire)"
  databricks bundle run genie_doctor_refresh -t "$T" -p "$P"

  echo "==> [3/4] Start the app"
  databricks bundle run genie-doctor -t "$T" -p "$P" || true

  echo "==> [4/4] Grant the app service principal read access (UC — powers the warehouse path)"
  python3 scripts/postdeploy_grants.py --profile "$P" --warehouse "$WH" \
    --catalog "$CAT" --schema "$SCH" --app genie-doctor || true

  URL="$(databricks apps get genie-doctor -p "$P" -o json 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('url',''))" 2>/dev/null)"
  echo ""
  echo "==> DONE — Genie Doctor (Lakehouse) deployed to '$T'."
  echo "    App: ${URL:-<check: databricks apps get genie-doctor -p $P>}"
  echo "    Daily refresh job is scheduled (06:00 America/New_York) and has run once."
  exit 0
fi

# ---------------------------------------------------------------------------
# LAKEBASE — Postgres serving (async instance + synced tables depend on gold).
# Requires resources/database.yml to be uncommented in databricks.yml `include:`.
# ---------------------------------------------------------------------------
[ -z "$INST" ] && { echo "!! serving_backend=lakebase but no lakebase_instance in config"; exit 1; }
echo "    instance=$INST"

echo "==> [1/6] Deploy (pipeline + job + Lakebase instance; instance provisions async)"
databricks bundle deploy -t "$T" -p "$P" \
  || echo "    (first-pass errors on instance-dependent resources are expected)"

echo "==> [2/6] Wait for Lakebase instance '$INST' to be AVAILABLE"
HOST=""
for i in $(seq 1 40); do
  J="$(databricks database get-database-instance "$INST" -p "$P" -o json 2>/dev/null)"
  ST="$(printf '%s' "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('state',''))" 2>/dev/null)"
  if [ "$ST" = "AVAILABLE" ]; then
    HOST="$(printf '%s' "$J" | python3 -c "import sys,json;print(json.load(sys.stdin).get('read_write_dns',''))")"
    break
  fi
  echo "    state=$ST ... ($i/40)"; sleep 15
done
[ -z "$HOST" ] && { echo "!! instance did not reach AVAILABLE"; exit 1; }
echo "    host=$HOST"

echo "==> [3/6] Run the job once (builds gold tables; also the daily trigger's first fire)"
databricks bundle run genie_doctor_refresh -t "$T" -p "$P"

echo "==> [4/6] Deploy again (instance ready + gold exists -> catalog + synced tables + app)"
for attempt in $(seq 1 6); do
  if databricks bundle deploy -t "$T" -p "$P" --var="pg_host=$HOST"; then
    echo "    deploy pass $attempt: clean"; break
  fi
  echo "    deploy pass $attempt hit the async catalog/instance race — retrying in 20s..."
  sleep 20
done

echo "==> [5/6] Start the app"
databricks bundle run genie-doctor -t "$T" -p "$P" --var="pg_host=$HOST" || true

echo "==> [6/6] Grant the app service principal read access"
python3 scripts/postdeploy_grants.py --profile "$P" --warehouse "$WH" \
  --catalog "$CAT" --schema "$SCH" --instance "$INST" --lakebase_db "$LDB" --app genie-doctor || true

URL="$(databricks apps get genie-doctor -p "$P" -o json 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('url',''))" 2>/dev/null)"
echo ""
echo "==> DONE — Genie Doctor (Lakebase) deployed to '$T'."
echo "    App: ${URL:-<check: databricks apps get genie-doctor -p $P>}"
echo "    Daily refresh job is scheduled (06:00 America/New_York) and has run once."

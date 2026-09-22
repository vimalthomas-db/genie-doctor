#!/usr/bin/env bash
# Genie Doctor — one-command deployment for any workspace.
#
#   ./deploy.sh <profile> <target>
#     e.g.  ./deploy.sh e2-demo-fe e2
#
# Runs the full sequence the platform requires (Lakebase provisioning is async and the
# synced tables depend on the job's gold tables): deploy -> wait for instance -> run job
# -> deploy -> start app -> grant the app SP read access. Idempotent: re-run any time.
set -uo pipefail
P="${1:?usage: ./deploy.sh <profile> <target>}"
T="${2:?usage: ./deploy.sh <profile> <target>}"
cd "$(dirname "$0")"

var() { python3 -c "import sys,json;print((json.load(sys.stdin).get('variables',{}).get('$1') or {}).get('value') or '')"; }

echo "==> Reading resolved config for target '$T'..."
CFG="$(databricks bundle validate -t "$T" -p "$P" -o json 2>/dev/null)"
INST="$(printf '%s' "$CFG" | var lakebase_instance)"
CAT="$(printf '%s' "$CFG" | var catalog)"
SCH="$(printf '%s' "$CFG" | var schema)"
WH="$(printf '%s' "$CFG" | var warehouse_id)"
LDB="$(printf '%s' "$CFG" | var lakebase_db)"
echo "    instance=$INST  gold=$CAT.$SCH  warehouse=$WH"
[ -z "$INST" ] && { echo "!! could not read config — check profile/target"; exit 1; }

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
# The Lakebase UC catalog is created ASYNC in this pass; the synced tables that target it
# can lose the race on the first try ("Catalog ... does not exist"). Retry the deploy until
# it goes fully green (bounded) — this is what makes the multi-pass genuinely hands-off.
for attempt in $(seq 1 6); do
  if databricks bundle deploy -t "$T" -p "$P" --var="pg_host=$HOST"; then
    echo "    deploy pass $attempt: clean"; break
  fi
  echo "    deploy pass $attempt hit the async catalog/instance race — retrying in 20s..."
  sleep 20
done

echo "==> [5/6] Start the app"
# Pass pg_host here too: `bundle run` re-resolves config, and an empty PGHOST env value is
# rejected by the Apps API. (Targets that hardcode pg_host in databricks.yml don't need this,
# but passing it makes the script correct for every target.)
databricks bundle run genie-doctor -t "$T" -p "$P" --var="pg_host=$HOST" || true

echo "==> [6/6] Grant the app service principal read access"
python3 scripts/postdeploy_grants.py --profile "$P" --warehouse "$WH" \
  --catalog "$CAT" --schema "$SCH" --instance "$INST" --lakebase_db "$LDB" --app genie-doctor || true

URL="$(databricks apps get genie-doctor -p "$P" -o json 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('url',''))" 2>/dev/null)"
echo ""
echo "==> DONE — Genie Doctor deployed to '$T'."
echo "    App: ${URL:-<check: databricks apps get genie-doctor -p $P>}"
echo "    Daily refresh job is scheduled (06:00 America/New_York) and has run once."

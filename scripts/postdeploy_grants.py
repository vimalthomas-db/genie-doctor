#!/usr/bin/env python3
"""Grant the Genie Doctor app's service principal the read access it needs.

A) Unity Catalog grants (REQUIRED) — powers the app's warehouse-fallback path.
B) Postgres role + SELECT (BEST-EFFORT) — enables the fast Lakebase path; skipped
   cleanly if the SP has no Postgres role yet (the app still works via the warehouse).

Called by deploy.sh; safe to run standalone too.
"""
import argparse
import time
import uuid
from databricks.sdk import WorkspaceClient


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="")
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--schema", required=True)
    ap.add_argument("--instance", required=True)
    ap.add_argument("--lakebase_db", default="genie_doctor")
    ap.add_argument("--app", default="genie-doctor")
    a = ap.parse_args()
    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()

    app = w.apps.get(a.app)
    sp = (getattr(app, "service_principal_client_id", None)
          or getattr(app, "service_principal_id", None))
    if not sp:
        print("[grants] could not resolve the app service principal — aborting."); return
    print(f"[grants] app service principal = {sp}")

    # --- A) Unity Catalog grants (required for the warehouse fallback) ---
    def sql(stmt):
        r = w.statement_execution.execute_statement(warehouse_id=a.warehouse, statement=stmt, wait_timeout="50s")
        while r.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(1); r = w.statement_execution.get_statement(r.statement_id)
        return r.status.state.value, (r.status.error.message if r.status.error else "")
    for stmt in (f"GRANT USE CATALOG ON CATALOG {a.catalog} TO `{sp}`",
                 f"GRANT USE SCHEMA ON SCHEMA {a.catalog}.{a.schema} TO `{sp}`",
                 f"GRANT SELECT ON SCHEMA {a.catalog}.{a.schema} TO `{sp}`"):
        st, err = sql(stmt)
        print(f"[grants] UC {st}: {stmt.split(' TO ')[0]}" + (f"  ({err[:90]})" if err else ""))

    # --- B) Postgres fast-path grants (best-effort) ---
    try:
        import psycopg2
        inst = w.database.get_database_instance(name=a.instance)
        cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[a.instance])
        conn = psycopg2.connect(host=inst.read_write_dns, port=5432, dbname=a.lakebase_db,
                                user=w.current_user.me().user_name, password=cred.token, sslmode="require")
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'GRANT USAGE ON SCHEMA public TO "{sp}"')
        cur.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{sp}"')
        cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "{sp}"')
        print("[grants] PG: granted SELECT to app SP — fast Lakebase path enabled.")
    except Exception as e:
        print(f"[grants] PG: fast-path skipped (app uses warehouse fallback). Reason: {str(e)[:120]}")


if __name__ == "__main__":
    main()

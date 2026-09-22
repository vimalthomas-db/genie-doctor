"""Genie Doctor — refresh Lakebase synced tables after the batch run.

SNAPSHOT synced tables do not auto-refresh on source change, so the daily job triggers
their sync pipelines here to push the latest gold into Postgres (what the app reads).
Pipeline ids are passed in (they are created with the synced tables; see resources/
database.yml). Non-fatal: a bad id is logged and skipped.
"""
import argparse
from databricks.sdk import WorkspaceClient


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline_ids", default="")
    a = p.parse_args()
    w = WorkspaceClient()
    ids = [x.strip() for x in a.pipeline_ids.split(",") if x.strip()]
    if not ids:
        print("[refresh_sync] no sync pipeline ids provided — skipping"); return
    for pid in ids:
        try:
            u = w.pipelines.start_update(pipeline_id=pid)
            print(f"[refresh_sync] refreshed {pid} -> {getattr(u, 'update_id', '')}")
        except Exception as e:
            print(f"[refresh_sync] skip {pid}: {str(e)[:120]}")


if __name__ == "__main__":
    main()

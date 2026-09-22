# Genie Doctor — Deployment Runbook (any workspace)

## TL;DR — one command
```bash
./deploy.sh <profile> <target>        # e.g. ./deploy.sh e2-demo-fe e2
```
`deploy.sh` runs the whole sequence and the grants for you (idempotent — re-run any time).
Before first use, add a target block in `databricks.yml` (see "One-time setup" below).
The rest of this doc explains what the script does and why.

---

The bundle is self-contained: `databricks.yml` + `resources/*.yml` define the pipeline,
job, Lakebase instance, UC catalog, synced tables, and the app. But **two Databricks
platform realities** mean deployment is a short **deploy → run → deploy** sequence, not a
single bundle command (which is why `deploy.sh` wraps it):

1. **Lakebase provisioning is async** — a new instance takes a few minutes to become
   AVAILABLE. Resources that attach to it (catalog, synced tables) can't be created in the
   same pass that creates the instance.
2. **Synced tables need their source Delta tables to exist** — those are produced by the
   *job run*. So you deploy, run the job once to build the gold tables, then deploy again to
   create the synced tables on top.

## One-time setup per target (in `databricks.yml`)
Add a target block with: `host`, `catalog`, `schema`, `space_ids` (or `owner_prefix`),
`warehouse_id` (an existing serverless warehouse), `lakebase_instance`, `lakebase_catalog`
(unique name), `lakebase_db`, and — after step 3 below — `pg_host`.

## Sequence

```bash
P=<profile>; T=<target>          # e.g. P=e2-demo-fe  T=e2

# 1. Deploy — creates pipeline + job + Lakebase instance (instance provisions async).
databricks bundle deploy -t $T -p $P
#    Expect catalog/synced/app to error on this first pass ("instance not found") — normal.

# 2. Wait for the instance to be AVAILABLE.
databricks database get-database-instance <lakebase_instance> -p $P -o json | jq .state

# 3. Capture the instance host and put it in the target's `pg_host` var.
databricks database get-database-instance <lakebase_instance> -p $P -o json | jq -r .read_write_dns

# 4. Run the job once — builds the gold tables (this is also the "daily trigger, fired now").
databricks bundle run genie_doctor_refresh -t $T -p $P

# 5. Deploy again — instance is ready + gold tables exist, so the UC catalog + synced
#    tables + app all create now.
databricks bundle deploy -t $T -p $P

# 6. Start the app.
databricks bundle run genie-doctor -t $T -p $P
```

The daily schedule (`resources/job.yml`, 06:00 America/New_York, UNPAUSED) is active after
step 1 — production-mode targets keep it unpaused (dev mode pauses it).

## Last mile — grant the app's service principal read access
Get the app SP client id: `databricks apps get genie-doctor -p $P -o json | jq -r .service_principal_client_id`

### A. Unity Catalog grants (REQUIRED — powers the warehouse fallback path)
Without these the app errors with `User does not have USE CATALOG`. Run on the warehouse:
```sql
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-client-id>`;
GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<app-sp-client-id>`;
GRANT SELECT      ON SCHEMA  <catalog>.<schema> TO `<app-sp-client-id>`;
```

### B. Postgres role + grants (OPTIONAL — enables the fast Lakebase path)
The app reads Postgres directly for speed. The app SP needs a Postgres role with SELECT on
the synced tables. Two options:

- **Automatic (preferred):** re-enable the `database` resource binding on the app in
  `resources/app.yml` (commented out today). It auto-provisions the app SP's Postgres role.
  Requires the deploying user to hold **MANAGE** on the Lakebase instance — if the deploy
  fails with `needs MANAGE permission on the resource`, grant that first, then redeploy.
- **Manual fallback:** grant the app SP (client id from `databricks apps get <app>`) SELECT:
  ```sql
  GRANT USAGE ON SCHEMA public TO "<app-sp-client-id>";
  GRANT SELECT ON ALL TABLES IN SCHEMA public TO "<app-sp-client-id>";
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "<app-sp-client-id>";
  ```
  (Connect to the instance as the owner with an OAuth token; the SP role must exist first —
  it is created by the binding above or via the Lakebase console.)

**Until the grant is in place the app still works** — `app.py` falls back to the bound SQL
warehouse automatically (slower, but functional).

## Verify
```bash
databricks apps get genie-doctor -p $P -o json | jq '{compute:.compute_status.state, url:.url}'
databricks jobs get <job-id> -p $P -o json | jq '.settings.schedule'   # UNPAUSED daily
```

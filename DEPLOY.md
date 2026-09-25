# Genie Doctor — Deployment Runbook (any workspace)

## Two serving versions — pick one with `serving_backend`
The app serves the same gold tables from one of two backends; choose per target with the
`serving_backend` variable in `databricks.yml`:

| Backend | What the app reads | When to use | Deploy |
|---|---|---|---|
| **`lakehouse`** (default) | gold **Delta** via the bound SQL warehouse | anywhere — **no Lakebase needed**; the safe default | one-pass |
| **`lakebase`** | a **Postgres** mirror of the gold tables (ms latency) | when Lakebase is available and you want the fast path | async multi-pass |

**Lakehouse is the default and requires no Lakebase entitlement** — if Lakebase isn't
available in your workspace, do nothing special; it just works. To switch to Lakebase:
1. Uncomment `resources/database.yml` in the `include:` list in `databricks.yml`.
2. Set `serving_backend: lakebase` on the target.
3. Uncomment the `LAKEBASE_INSTANCE` / `PGDATABASE` / `PGHOST` env block in `resources/app.yml`.
4. `deploy.sh` then runs the async Lakebase sequence automatically (captures `pg_host` for you).

The app also degrades safely at runtime: `serving_backend=lakebase` (or `auto`) falls back
to the Lakehouse warehouse if Postgres is unconfigured or unreachable, and shows the active
source in the left-rail footer.

## TL;DR — one command
```bash
./deploy.sh <profile> <target>        # e.g. ./deploy.sh e2-demo-fe e2
```
`deploy.sh` reads `serving_backend` and runs the right sequence + grants for you (idempotent
— re-run any time). **Lakehouse** = deploy → run job → start app → grants. **Lakebase** =
the async sequence below. Before first use, add a target block in `databricks.yml` (see
"One-time setup").

---

The Lakebase path is a short **deploy → run → deploy** sequence (not a single bundle
command, which is why `deploy.sh` wraps it) because of **two Databricks platform realities**:

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

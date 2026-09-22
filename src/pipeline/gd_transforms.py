"""Genie Doctor — SDP declarative transforms: BRONZE -> GOLD.

Reads bronze (landed by the harvesters) + system.access.audit, materializes the gold
scorecard tables the app serves. Data-quality expectations gate the outputs.

NO letter grade, NO LLM verdict. Everything is an objective PERCENTAGE or COUNT:
  Freshness    harvest age (days), days_since_active, config recency (days)
  Quality      benchmark_quality_pct = % of benchmark traces that PASS (num_correct
               / num_done); plus coverage_pct = num_done / defined
  Feedback     feedback_positive_pct = thumbs_up / (thumbs_up+thumbs_down);
               feedback_coverage_pct = rated / msgs
  Richness     SQL_INSTRUCTION + TEXT_INSTRUCTION counts, SAMPLE_QUESTION examples,
               BENCHMARK questions defined, table breadth
"""
import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")   # noqa: F821  (spark is provided in pipelines)
SCHEMA = spark.conf.get("schema")      # noqa: F821
B = f"{CATALOG}.{SCHEMA}"              # bronze namespace


# --- SILVER --------------------------------------------------------------------
@dlt.table(comment="Per-space activity/quality/feedback factors from message grain.")
@dlt.expect("has_space_id", "space_id IS NOT NULL")
def factors():
    df = spark.read.table(f"{B}.genie_message_details")  # noqa: F821
    return (df.groupBy("space_id").agg(
        F.count("*").alias("msgs"),
        F.countDistinct("user_id").alias("users"),
        F.round(100.0 * F.sum(F.expr(
            "CASE WHEN status<>'COMPLETED' OR error_type IS NOT NULL THEN 1 ELSE 0 END"
        )) / F.count("*"), 1).alias("error_pct"),
        F.round(F.expr("approx_percentile(response_time_sec, 0.9)"), 1).alias("p90_latency_s"),
        F.sum(F.expr("CASE WHEN feedback_rating='POSITIVE' THEN 1 ELSE 0 END")).alias("thumbs_up"),
        F.sum(F.expr("CASE WHEN feedback_rating='NEGATIVE' THEN 1 ELSE 0 END")).alias("thumbs_down"),
        F.round(100.0 * F.sum(F.expr(
            "CASE WHEN feedback_rating IS NOT NULL THEN 1 ELSE 0 END")) / F.count("*"), 1).alias("feedback_coverage_pct"),
        F.datediff(F.current_date(), F.max(F.col("created_ts").cast("date"))).alias("days_since_active"),
    ))


@dlt.table(comment="RICHNESS: curation counts per space (data-rooms instructions + curated qs).")
def richness_facts():
    ins = spark.read.table(f"{B}.genie_instructions")   # noqa: F821
    cq = spark.read.table(f"{B}.genie_curated_qs")       # noqa: F821
    meta = spark.read.table(f"{B}.genie_space_meta")     # noqa: F821
    ins_agg = ins.groupBy("space_id").agg(
        F.sum(F.expr("CASE WHEN instruction_type='SQL_INSTRUCTION' THEN 1 ELSE 0 END")).alias("sql_instructions"),
        F.sum(F.expr("CASE WHEN instruction_type='TEXT_INSTRUCTION' THEN 1 ELSE 0 END")).alias("text_instructions"),
        F.sum(F.expr("CASE WHEN instruction_status='ACCEPTED' THEN 1 ELSE 0 END")).alias("accepted_instructions"))
    cq_agg = cq.filter("is_deprecated = false OR is_deprecated IS NULL").groupBy("space_id").agg(
        F.sum(F.expr("CASE WHEN question_type='SAMPLE_QUESTION' THEN 1 ELSE 0 END")).alias("sample_questions"),
        F.sum(F.expr("CASE WHEN question_type='BENCHMARK' THEN 1 ELSE 0 END")).alias("benchmark_questions_defined"))
    return (ins_agg.join(cq_agg, "space_id", "full_outer")
            .join(meta.select("space_id", F.size("table_identifiers").alias("n_tables")), "space_id", "left")
            .na.fill(0))


@dlt.table(comment="QUALITY: benchmark trace pass-rate — % of benchmark traces that pass.")
def benchmark_facts():
    runs = spark.read.table(f"{B}.genie_eval_runs")           # noqa: F821
    rich = dlt.read("richness_facts").select("space_id", "benchmark_questions_defined")
    # Latest DONE run per space, then pass-rate over its traces.
    w = "ROW_NUMBER() OVER (PARTITION BY space_id ORDER BY created_timestamp DESC)"
    latest = (runs.withColumn("_rn", F.expr(w)).filter("_rn = 1")
              .select("space_id",
                      F.col("num_questions"), F.col("num_correct"),
                      F.col("num_needs_review"), F.col("num_done"),
                      F.col("created_timestamp").alias("latest_run_ts")))
    counts = runs.groupBy("space_id").agg(F.count("*").alias("num_runs"))
    df = (rich.join(latest, "space_id", "left").join(counts, "space_id", "left").na.fill(0))
    # The eval-runs API often leaves num_done null even for DONE runs, so treat
    # "traces that ran" as num_done when present else num_questions.
    df = df.withColumn("done_eff",
                       F.coalesce(F.when(F.col("num_done") > 0, F.col("num_done")),
                                  F.col("num_questions")))
    return df.withColumn(
        "benchmark_ran", F.col("num_runs") > 0
    ).withColumn(
        # QUALITY % = passing benchmark traces / traces that ran.
        "benchmark_quality_pct",
        F.when(F.col("done_eff") > 0,
               F.round(100.0 * F.col("num_correct") / F.col("done_eff"), 1))
    ).withColumn(
        # COVERAGE % = traces run / benchmark questions defined.
        "coverage_pct",
        F.when(F.col("benchmark_questions_defined") > 0,
               F.round(100.0 * F.col("done_eff") / F.col("benchmark_questions_defined"), 1))
    )


# --- GOLD ----------------------------------------------------------------------
@dlt.table(comment="GOLD: per-space scorecard — four pillars as PERCENTAGES + counts. No grade. "
                   "Spined on genie_space_meta so it is OWNER-SCOPED and carries the title.")
def space_scorecard():
    meta = (spark.read.table(f"{B}.genie_space_meta")   # noqa: F821  (owner-scoped spine)
            .select("space_id", "title", "owner",
                    F.size("table_identifiers").alias("n_tables")))
    f = dlt.read("factors")
    r = dlt.read("richness_facts").drop("n_tables")     # meta supplies n_tables
    b = dlt.read("benchmark_facts").drop("benchmark_questions_defined")
    df = (meta.join(f, "space_id", "left")
          .join(r, "space_id", "left")
          .join(b, "space_id", "left"))
    # Fill COUNTS with 0; leave PERCENTAGES null (null = "not measured", not "0%").
    count_cols = ["msgs", "users", "thumbs_up", "thumbs_down", "days_since_active",
                  "sql_instructions", "text_instructions", "accepted_instructions",
                  "sample_questions", "benchmark_questions_defined", "n_tables",
                  "num_runs", "num_questions", "num_correct", "num_needs_review", "num_done"]
    df = df.fillna(0, subset=[c for c in count_cols if c in df.columns])
    return (df.withColumn("benchmark_ran", F.coalesce(F.col("benchmark_ran"), F.lit(False)))
            .withColumn(  # FEEDBACK % = positive of the rated messages.
                "feedback_positive_pct",
                F.when((F.col("thumbs_up") + F.col("thumbs_down")) > 0,
                       F.round(100.0 * F.col("thumbs_up")
                               / (F.col("thumbs_up") + F.col("thumbs_down")), 1)))
            .withColumn("snapshot_date", F.current_date()))


@dlt.table(comment="GOLD: thumbs-down questions per space — the actual flagged questions "
                   "(with the SQL Genie generated) for the detail page.")
def feedback_flags():
    md = spark.read.table(f"{B}.genie_message_details")   # noqa: F821
    return (md.filter("feedback_rating = 'NEGATIVE'")
            .select("space_id",
                    F.col("message_id"),
                    F.col("user_question").alias("question"),
                    F.substring(F.col("sql_generated"), 1, 2000).alias("sql_generated"),
                    F.col("error_type"),
                    F.col("created_ts")))


@dlt.table(comment="GOLD: benchmark eval runs by month — fleet timeline of when benchmarks ran.")
def benchmark_runs_monthly():
    runs = spark.read.table(f"{B}.genie_eval_runs")   # noqa: F821
    return (runs.withColumn(
                "run_month",
                F.date_format(F.to_date(F.from_unixtime(F.col("created_timestamp") / 1000)), "yyyy-MM"))
            .filter(F.col("run_month").isNotNull())
            .groupBy("run_month").agg(
                F.count("*").alias("runs"),
                F.countDistinct("space_id").alias("spaces"),
                F.sum("num_correct").alias("correct"))
            .orderBy("run_month"))


@dlt.table(comment="GOLD: per-space benchmark eval-run TREND — one row per run, ordered by "
                   "execution, with quality% and coverage% (for the accuracy-over-runs chart).")
def benchmark_run_trend():
    runs = spark.read.table(f"{B}.genie_eval_runs")   # noqa: F821
    rich = dlt.read("richness_facts").select("space_id", "benchmark_questions_defined")
    df = runs.join(rich, "space_id", "left")
    df = df.withColumn("done_eff",
                       F.coalesce(F.when(F.col("num_done") > 0, F.col("num_done")),
                                  F.col("num_questions")))
    df = df.withColumn("run_index",
                       F.expr("ROW_NUMBER() OVER (PARTITION BY space_id ORDER BY created_timestamp ASC)"))
    return (df.withColumn("quality_pct",
                          F.when(F.col("done_eff") > 0,
                                 F.round(100.0 * F.col("num_correct") / F.col("done_eff"), 1)))
            .withColumn("coverage_pct",
                        F.when(F.col("benchmark_questions_defined") > 0,
                               F.round(100.0 * F.col("done_eff") / F.col("benchmark_questions_defined"), 1)))
            .withColumn("run_ts", (F.col("created_timestamp") / 1000).cast("timestamp"))
            .select("space_id", "eval_run_id", "run_index", "run_ts",
                    "num_correct", "num_questions", "num_done", "quality_pct", "coverage_pct"))


@dlt.table(comment="GOLD: per-space per-run per-question benchmark ASSESSMENT "
                   "(GOOD/BAD/NEEDS_REVIEW) for the pass/fail matrix.")
def benchmark_question_matrix():
    res = spark.read.table(f"{B}.genie_eval_results")   # noqa: F821
    runs = (spark.read.table(f"{B}.genie_eval_runs")     # noqa: F821
            .select("space_id", "eval_run_id", "created_timestamp"))
    df = res.join(runs, ["space_id", "eval_run_id"], "left")
    df = (df.withColumn("run_index",
                        F.expr("DENSE_RANK() OVER (PARTITION BY space_id ORDER BY created_timestamp ASC)"))
          .withColumn("run_ts", (F.col("created_timestamp") / 1000).cast("timestamp")))
    return df.select("space_id", "eval_run_id", "run_index", "run_ts", "result_id",
                     "benchmark_question_id",
                     F.substring(F.col("question"), 1, 300).alias("question"), "assessment")


@dlt.table(comment="GOLD: fleet rollup — one row per snapshot_date for the overview KPIs/trends.")
def fleet_rollup():
    s = dlt.read("space_scorecard")
    return s.agg(
        F.current_date().alias("snapshot_date"),
        F.count("*").alias("n_agents"),
        F.sum(F.col("benchmark_ran").cast("int")).alias("n_with_benchmark"),
        F.round(F.avg("benchmark_quality_pct"), 1).alias("avg_quality_pct"),
        F.round(F.avg("feedback_positive_pct"), 1).alias("avg_feedback_pct"),
        F.round(F.avg("feedback_coverage_pct"), 1).alias("avg_feedback_coverage_pct"),
    )

# Databricks notebook source
# MAGIC %run ../Utils/audit_logger

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("batch_param", "manual_run")
BATCH_PARAM = dbutils.widgets.get("batch_param")


def upsert(df, table_name, merge_condition):
    """Create the table on first run, MERGE on every later run (idempotent)."""
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").saveAsTable(table_name)
    else:
        (DeltaTable.forName(spark, table_name).alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    return df.count()

# COMMAND ----------

def build_dim_repo():
    run_ctx = start_run(spark, layer="Silver-to-Gold-dim_repo", parameter=BATCH_PARAM)
    try:
        repos = (spark.table("silver_commits").select("repo_name")
                 .union(spark.table("silver_issues").select("repo_name")).distinct())
        df = (repos
              .withColumn("repo_key", F.xxhash64("repo_name"))
              .withColumn("repo_owner", F.split("repo_name", "/").getItem(0))
              .withColumn("repo_short_name", F.split("repo_name", "/").getItem(1))
              .withColumn("load_timestamp", F.current_timestamp())
              .select("repo_key", "repo_name", "repo_owner", "repo_short_name", "load_timestamp"))
        n = upsert(df, "dim_repo", "t.repo_key = s.repo_key")
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=n)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise


def build_dim_contributor():
    run_ctx = start_run(spark, layer="Silver-to-Gold-dim_contributor", parameter=BATCH_PARAM)
    try:
        commits = spark.table("silver_commits").select(
            F.coalesce("author_login_hash", F.lit("unknown")).alias("contributor_key"),
            F.col("authored_at").alias("activity_at"))
        issues = spark.table("silver_issues").select(
            F.coalesce("reporter_login_hash", F.lit("unknown")).alias("contributor_key"),
            F.col("created_at").alias("activity_at"))
        df = (commits.unionByName(issues)
              .groupBy("contributor_key")
              .agg(F.min("activity_at").alias("first_seen_at"))
              .withColumn("load_timestamp", F.current_timestamp()))
        n = upsert(df, "dim_contributor", "t.contributor_key = s.contributor_key")
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=n)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise


def build_dim_date():
    run_ctx = start_run(spark, layer="Silver-to-Gold-dim_date", parameter=BATCH_PARAM)
    try:
        bounds = spark.sql("""
            SELECT MIN(d) AS mn, MAX(d) AS mx FROM (
                SELECT to_date(authored_at) AS d FROM silver_commits
                UNION ALL SELECT to_date(created_at) FROM silver_issues
                UNION ALL SELECT to_date(closed_at) FROM silver_issues)
        """).first()
        df = (spark.sql(f"SELECT explode(sequence(to_date('{bounds.mn}'), to_date('{bounds.mx}'), interval 1 day)) AS full_date")
              .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
              .withColumn("year", F.year("full_date"))
              .withColumn("month", F.month("full_date"))
              .withColumn("month_name", F.date_format("full_date", "MMMM"))
              .withColumn("day", F.dayofmonth("full_date"))
              .withColumn("day_name", F.date_format("full_date", "EEEE"))
              .withColumn("week_of_year", F.weekofyear("full_date"))
              .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
              .withColumn("load_timestamp", F.current_timestamp()))
        n = upsert(df, "dim_date", "t.date_key = s.date_key")
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=n)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise

# COMMAND ----------

def build_fact_commits():
    run_ctx = start_run(spark, layer="Silver-to-Gold-fact_commits", parameter=BATCH_PARAM)
    try:
        df = spark.table("silver_commits").select(
            "commit_sha",
            F.xxhash64("repo_name").alias("repo_key"),
            F.coalesce("author_login_hash", F.lit("unknown")).alias("contributor_key"),
            F.date_format("authored_at", "yyyyMMdd").cast("int").alias("date_key"),
            "authored_at",
            F.hour("authored_at").alias("hour_of_day"),
            F.length("message").alias("message_length"),
            F.current_timestamp().alias("load_timestamp"))
        n = upsert(df, "fact_commits", "t.commit_sha = s.commit_sha")
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=n)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise


def build_fact_issues():
    run_ctx = start_run(spark, layer="Silver-to-Gold-fact_issues", parameter=BATCH_PARAM)
    try:
        df = spark.table("silver_issues").select(
            F.xxhash64("repo_name").alias("repo_key"),
            "issue_number",
            F.coalesce("reporter_login_hash", F.lit("unknown")).alias("contributor_key"),
            F.date_format("created_at", "yyyyMMdd").cast("int").alias("created_date_key"),
            F.date_format("closed_at", "yyyyMMdd").cast("int").alias("closed_date_key"),
            "state",
            "comment_count",
            F.when(F.col("closed_at").isNotNull(),
                   F.datediff("closed_at", "created_at")).alias("days_to_close"),
            (F.col("state") == "closed").alias("is_closed"),
            F.current_timestamp().alias("load_timestamp"))
        n = upsert(df, "fact_issues",
                   "t.repo_key = s.repo_key AND t.issue_number = s.issue_number")
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=n)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise

# COMMAND ----------

build_dim_repo()
build_dim_contributor()
build_dim_date()
build_fact_commits()
build_fact_issues()

# COMMAND ----------

for tbl, cols in [("fact_commits", "repo_key, date_key"),
                  ("fact_issues", "repo_key, created_date_key")]:
    try:
        spark.sql(f"ALTER TABLE {tbl} CLUSTER BY ({cols})")   # Liquid Clustering
    except Exception as e:
        print(f"[INFO] clustering already set or unsupported for {tbl}: {str(e)[:120]}")
    spark.sql(f"OPTIMIZE {tbl}")
print("Gold tables optimized")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT layer, COUNT(*) AS runs,
# MAGIC        ROUND(AVG(unix_timestamp(end_time) - unix_timestamp(start_time)), 1) AS avg_seconds,
# MAGIC        ROUND(SUM(unix_timestamp(end_time) - unix_timestamp(start_time)), 1) AS total_seconds
# MAGIC FROM pipeline_execution_logs
# MAGIC WHERE status = 'SUCCESS'
# MAGIC GROUP BY layer
# MAGIC ORDER BY layer;
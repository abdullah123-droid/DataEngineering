# Databricks notebook source
# MAGIC %run ../Utils/schemas

# COMMAND ----------

# MAGIC %run ../Utils/audit_logger

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, explode, col, lit

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("input_path", "/Volumes/workspace/default/raw_files/full_load_sample.json")
dbutils.widgets.text("load_type", "full")

INPUT_PATH = dbutils.widgets.get("input_path")
LOAD_TYPE = dbutils.widgets.get("load_type")

BRONZE_COMMITS_TABLE = "bronze_commits"
BRONZE_ISSUES_TABLE = "bronze_issues"
QUARANTINE_PATH = "/Volumes/workspace/default/raw_files/quarantine/"

# COMMAND ----------

def ingest_layer(spark, input_path, load_type):
    run_ctx = start_run(spark, layer="Raw-to-Bronze", parameter=input_path)
    try:
        raw_df = spark.read.schema(RAW_FILE_SCHEMA).json(input_path)

        commits_df = (
            raw_df.select(explode(col("commits")).alias("c"))
            .select("c.*")
            .withColumn("load_timestamp", current_timestamp())
            .withColumn("load_type", lit(load_type))
        )
        good_commits = commits_df.filter(col("sha").isNotNull() & col("_repo").isNotNull())
        bad_commits = commits_df.filter(col("sha").isNull() | col("_repo").isNull())
        if bad_commits.count() > 0:
            bad_commits.write.mode("append").json(QUARANTINE_PATH + "commits/")
            print(f"[WARN] Quarantined {bad_commits.count()} malformed commit records")
        good_commits.write.format("delta").mode("append").saveAsTable(BRONZE_COMMITS_TABLE)

        issues_df = (
            raw_df.select(explode(col("issues")).alias("i"))
            .select("i.*")
            .withColumn("load_timestamp", current_timestamp())
            .withColumn("load_type", lit(load_type))
        )
        good_issues = issues_df.filter(col("id").isNotNull() & col("_repo").isNotNull())
        bad_issues = issues_df.filter(col("id").isNull() | col("_repo").isNull())
        if bad_issues.count() > 0:
            bad_issues.write.mode("append").json(QUARANTINE_PATH + "issues/")
            print(f"[WARN] Quarantined {bad_issues.count()} malformed issue records")
        good_issues.write.format("delta").mode("append").saveAsTable(BRONZE_ISSUES_TABLE)

        total_rows = good_commits.count() + good_issues.count()
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=total_rows)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise

# COMMAND ----------

ingest_layer(spark, INPUT_PATH, LOAD_TYPE)
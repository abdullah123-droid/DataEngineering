# Databricks notebook source
# MAGIC %run ../Utils/audit_logger

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("batch_param", "manual_run")
BATCH_PARAM = dbutils.widgets.get("batch_param")

SILVER_COMMITS_TABLE = "silver_commits"
SILVER_ISSUES_TABLE = "silver_issues"

# COMMAND ----------

def transform_commits(spark):
    run_ctx = start_run(spark, layer="Bronze-to-Silver-Commits", parameter=BATCH_PARAM)
    try:
        bronze = spark.table("bronze_commits")
        silver_new = (
            bronze.select(
                col("sha").alias("commit_sha"),
                col("_repo").alias("repo_name"),
                col("commit.author.name").alias("author_name_hash"),
                col("commit.author.email").alias("author_email_hash"),
                to_timestamp(col("commit.author.date")).alias("authored_at"),
                col("commit.committer.name").alias("committer_name_hash"),
                to_timestamp(col("commit.committer.date")).alias("committed_at"),
                col("commit.message").alias("message"),
                col("author.login").alias("author_login_hash"),
                col("load_type"),
                current_timestamp().alias("load_timestamp"),
            ).dropDuplicates(["commit_sha"])
        )
        if not spark.catalog.tableExists(SILVER_COMMITS_TABLE):
            silver_new.write.format("delta").saveAsTable(SILVER_COMMITS_TABLE)
            rows_affected = silver_new.count()
        else:
            target = DeltaTable.forName(spark, SILVER_COMMITS_TABLE)
            (target.alias("t")
                .merge(silver_new.alias("s"), "t.commit_sha = s.commit_sha")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            rows_affected = silver_new.count()
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=rows_affected)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise

def transform_issues(spark):
    run_ctx = start_run(spark, layer="Bronze-to-Silver-Issues", parameter=BATCH_PARAM)
    try:
        bronze = spark.table("bronze_issues")
        silver_new = (
            bronze.select(
                col("_repo").alias("repo_name"),
                col("number").cast("int").alias("issue_number"),
                col("title"),
                col("user.login").alias("reporter_login_hash"),
                col("state"),
                to_timestamp(col("created_at")).alias("created_at"),
                to_timestamp(col("updated_at")).alias("updated_at"),
                to_timestamp(col("closed_at")).alias("closed_at"),
                col("comments").cast("int").alias("comment_count"),
                col("load_type"),
                current_timestamp().alias("load_timestamp"),
            ).dropDuplicates(["repo_name", "issue_number"])
        )
        if not spark.catalog.tableExists(SILVER_ISSUES_TABLE):
            silver_new.write.format("delta").saveAsTable(SILVER_ISSUES_TABLE)
            rows_affected = silver_new.count()
        else:
            target = DeltaTable.forName(spark, SILVER_ISSUES_TABLE)
            (target.alias("t")
                .merge(silver_new.alias("s"), "t.repo_name = s.repo_name AND t.issue_number = s.issue_number")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            rows_affected = silver_new.count()
        end_run(spark, run_ctx, status="SUCCESS", rows_affected=rows_affected)
    except Exception as e:
        end_run(spark, run_ctx, status="FAILURE", rows_affected=0, error_message=str(e))
        raise

# COMMAND ----------

transform_commits(spark)
transform_issues(spark)

# COMMAND ----------

transform_commits(spark)
transform_issues(spark)

# COMMAND ----------

display(spark.sql("""
    SELECT commit_sha, COUNT(*) as cnt
    FROM silver_commits
    GROUP BY commit_sha
    HAVING COUNT(*) > 1
"""))

# COMMAND ----------

display(spark.sql("SELECT * FROM pipeline_execution_logs ORDER BY start_time DESC"))
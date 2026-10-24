# Databricks notebook source
dbutils.widgets.text("input_path", "/Volumes/workspace/default/raw_files/incremental_load_sample.json")
dbutils.widgets.text("load_type", "incremental")
dbutils.widgets.text("batch_param", "incremental_run")

input_path = dbutils.widgets.get("input_path")
load_type = dbutils.widgets.get("load_type")
batch_param = dbutils.widgets.get("batch_param")

dbutils.notebook.run("../Bronze/bronze_ingest", 3600, {"input_path": input_path, "load_type": load_type})
dbutils.notebook.run("../Silver/silver_transform", 3600, {"batch_param": batch_param})
dbutils.notebook.run("../Gold/gold_transform", 3600, {"batch_param": batch_param})
print("Pipeline complete: Bronze -> Silver -> Gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT layer, parameter, status, rows_affected, start_time
# MAGIC FROM pipeline_execution_logs
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 12;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'fact_commits' AS tbl, COUNT(*) AS n FROM fact_commits
# MAGIC UNION ALL SELECT 'fact_issues', COUNT(*) FROM fact_issues
# MAGIC UNION ALL SELECT 'dim_repo', COUNT(*) FROM dim_repo
# MAGIC UNION ALL SELECT 'dim_contributor', COUNT(*) FROM dim_contributor
# MAGIC UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS orphan_repo_keys
# MAGIC FROM fact_commits f LEFT JOIN dim_repo d ON f.repo_key = d.repo_key
# MAGIC WHERE d.repo_key IS NULL;
# MAGIC
# MAGIC SELECT COUNT(*) AS orphan_contributors
# MAGIC FROM fact_commits f LEFT JOIN dim_contributor d ON f.contributor_key = d.contributor_key
# MAGIC WHERE d.contributor_key IS NULL;
# MAGIC
# MAGIC SELECT COUNT(*) AS orphan_dates
# MAGIC FROM fact_commits f LEFT JOIN dim_date d ON f.date_key = d.date_key
# MAGIC WHERE d.date_key IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT layer, status, rows_affected, start_time, end_time,
# MAGIC        ROUND((unix_timestamp(end_time) - unix_timestamp(start_time)), 1) AS seconds
# MAGIC FROM pipeline_execution_logs
# MAGIC WHERE layer LIKE 'Silver-to-Gold%'
# MAGIC ORDER BY start_time DESC;
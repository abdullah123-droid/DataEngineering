# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

LOG_TABLE_NAME = "pipeline_execution_logs"

LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("parameter", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("rows_affected", LongType(), True),
    StructField("error_message", StringType(), True),
])

def _ensure_log_table_exists(spark):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE_NAME} (
            run_id STRING,
            layer STRING,
            parameter STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            status STRING,
            rows_affected LONG,
            error_message STRING
        ) USING DELTA
    """)

def start_run(spark, layer, parameter):
    _ensure_log_table_exists(spark)
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).replace(tzinfo=None)
    return {"run_id": run_id, "layer": layer, "parameter": parameter, "start_time": start_time}

def end_run(spark, run_ctx, status, rows_affected, error_message=None):
    end_time = datetime.now(timezone.utc).replace(tzinfo=None)
    row = Row(
        run_id=run_ctx["run_id"],
        layer=run_ctx["layer"],
        parameter=run_ctx["parameter"],
        start_time=run_ctx["start_time"],
        end_time=end_time,
        status=status,
        rows_affected=rows_affected,
        error_message=error_message,
    )
    df = spark.createDataFrame([row], schema=LOG_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(LOG_TABLE_NAME)
    print(f"[AUDIT] {run_ctx['layer']} | {run_ctx['parameter']} | {status} | {rows_affected} rows")
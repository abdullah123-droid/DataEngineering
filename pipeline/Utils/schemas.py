# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, ArrayType
)

COMMIT_AUTHOR_SCHEMA = StructType([
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("date", StringType(), True),
])

COMMIT_USER_SCHEMA = StructType([
    StructField("login", StringType(), True),
    StructField("type", StringType(), True),
    StructField("user_view_type", StringType(), True),
    StructField("site_admin", BooleanType(), True),
])

COMMIT_INNER_SCHEMA = StructType([
    StructField("author", COMMIT_AUTHOR_SCHEMA, True),
    StructField("committer", COMMIT_AUTHOR_SCHEMA, True),
    StructField("message", StringType(), True),
])

COMMITS_BRONZE_SCHEMA = StructType([
    StructField("sha", StringType(), False),
    StructField("commit", COMMIT_INNER_SCHEMA, True),
    StructField("html_url", StringType(), True),
    StructField("author", COMMIT_USER_SCHEMA, True),
    StructField("committer", COMMIT_USER_SCHEMA, True),
    StructField("_repo", StringType(), False),
])

ISSUE_LABEL_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("color", StringType(), True),
])

ISSUE_USER_SCHEMA = StructType([
    StructField("login", StringType(), True),
    StructField("type", StringType(), True),
    StructField("site_admin", BooleanType(), True),
])

ISSUES_BRONZE_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("number", LongType(), False),
    StructField("title", StringType(), True),
    StructField("user", ISSUE_USER_SCHEMA, True),
    StructField("state", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("closed_at", StringType(), True),
    StructField("comments", LongType(), True),
    StructField("html_url", StringType(), True),
    StructField("labels", ArrayType(ISSUE_LABEL_SCHEMA), True),
    StructField("_repo", StringType(), False),
])

RAW_FILE_SCHEMA = StructType([
    StructField("commits", ArrayType(COMMITS_BRONZE_SCHEMA), True),
    StructField("issues", ArrayType(ISSUES_BRONZE_SCHEMA), True),
])
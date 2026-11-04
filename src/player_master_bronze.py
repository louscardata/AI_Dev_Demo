# Databricks notebook source
"""Replace the Bronze player table with the supplied complete Parquet input."""

import logging

from pyspark.sql import functions as F
from pyspark.sql import types as T


SOURCE_PATH = "/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/player_master.parquet"
TARGET_TABLE = "ai_dev_class_demo.bronze_layer.player_master"

# Decimal precision is supplied by Parquet; only scale is fixed by the dictionary.
COLUMNS = [
    "player_id", "external_player_key", "first_name", "last_name", "gender",
    "birth_date", "dominant_hand", "home_region_id", "country_code",
    "registration_date", "player_status", "rating", "rating_value",
    "confidence_score", "volatility_score", "global_percentile",
    "match_count_used", "rating_date", "rating_batch_id", "snapshot_month",
]
INTEGER_COLUMNS = {
    "player_id", "home_region_id", "match_count_used", "rating_batch_id",
}
DECIMAL_SCALES = {
    "rating": 3, "rating_value": 3, "confidence_score": 3,
    "volatility_score": 3, "global_percentile": 2,
}
REQUIRED_COLUMNS = [
    "player_id", "external_player_key", "first_name", "last_name",
    "birth_date", "registration_date", "player_status", "snapshot_month",
]

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("player_master_bronze")
logger.setLevel(logging.INFO)


def build_bronze(spark):
    logger.info("Starting ingestion: source=%s target=%s", SOURCE_PATH, TARGET_TABLE)
    source = spark.read.parquet(SOURCE_PATH)
    logger.info("Source schema: %s", source.schema.simpleString())

    if len(source.columns) != len(COLUMNS) or set(source.columns) != set(COLUMNS):
        raise ValueError(f"Source columns must match the dictionary: {COLUMNS}")

    for field in source.schema.fields:
        if field.name in INTEGER_COLUMNS:
            valid = isinstance(field.dataType, T.LongType)
        elif field.name in DECIMAL_SCALES:
            valid = (
                isinstance(field.dataType, T.DecimalType)
                and field.dataType.scale == DECIMAL_SCALES[field.name]
            )
        else:
            valid = isinstance(field.dataType, T.StringType)
        if not valid:
            raise ValueError(f"Unexpected source type: {field.name}={field.dataType}")

    source = source.select(*COLUMNS)
    counts = source.agg(
        F.count("*").alias("source_rows"),
        *[
            F.count(F.when(F.col(column).isNull(), 1)).alias(column)
            for column in REQUIRED_COLUMNS
        ],
    ).first().asDict()
    source_count = counts.pop("source_rows")
    duplicate_keys = (
        source.groupBy("player_id").count().filter(F.col("count") > 1).count()
    )
    logger.info(
        "Validation: source_rows=%s required_null_counts=%s duplicate_key_groups=%s",
        source_count, counts, duplicate_keys,
    )
    if any(counts.values()) or duplicate_keys:
        raise ValueError("Source validation failed; see counts in the job output")
    if source_count == 0:
        raise ValueError("Empty source rejected to prevent clearing the Bronze table")

    spark.sql("CREATE SCHEMA IF NOT EXISTS ai_dev_class_demo.bronze_layer")
    # Refuse implicit format conversion or schema replacement on an existing target.
    if spark.catalog.tableExists(TARGET_TABLE):
        detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").first()
        if detail["format"].lower() != "delta":
            raise ValueError("Existing target must be a Delta table")
        existing = [(f.name, f.dataType) for f in spark.table(TARGET_TABLE).schema]
        expected = [(f.name, f.dataType) for f in source.schema]
        if existing != expected:
            raise ValueError("Existing target schema differs from source; review before replacement")

    (
        source.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .option("overwriteSchema", "false")
        .saveAsTable(TARGET_TABLE)
    )

    target = spark.table(TARGET_TABLE)
    target_count = target.count()
    expected = [(f.name, f.dataType) for f in source.schema]
    actual = [(f.name, f.dataType) for f in target.schema]
    detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").first()
    if target_count != source_count or actual != expected or detail["format"].lower() != "delta":
        raise RuntimeError("Post-write verification failed; inspect the committed Delta table")
    logger.info("Completed: source_rows=%s bronze_rows=%s format=delta", source_count, target_count)


try:
    build_bronze(spark)
except Exception:
    logger.exception("Bronze ingestion failed")
    raise

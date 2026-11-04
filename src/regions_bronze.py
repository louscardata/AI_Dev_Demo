# Databricks notebook source
"""Replace the Bronze regions table with the supplied complete Parquet input."""

import logging

from pyspark.sql import functions as F
from pyspark.sql import types as T


SOURCE_PATH = "/Volumes/ai_dev_class_demo/ai_dev_schema/ai_dev_raw/regions.parquet"
TARGET_TABLE = "ai_dev_class_demo.bronze_layer.regions"
COLUMNS = [
    "id", "country_code", "region_type", "region_name",
    "state_province_code", "population", "latitude", "longitude",
]
INTEGER_COLUMNS = {"id", "population"}
DECIMAL_SCALES = {"latitude": 6, "longitude": 6}
REQUIRED_COLUMNS = ["id", "country_code", "region_name"]

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("regions_bronze")
logger.setLevel(logging.INFO)


def validate_source_schema(schema):
    if len(schema.names) != len(COLUMNS) or set(schema.names) != set(COLUMNS):
        raise ValueError(f"Source columns must match the dictionary: {COLUMNS}")
    for field in schema.fields:
        if field.name in INTEGER_COLUMNS:
            valid = isinstance(field.dataType, T.LongType)
        elif field.name in DECIMAL_SCALES:
            # Preserve the file's precision; the dictionary fixes only scale.
            valid = (
                isinstance(field.dataType, T.DecimalType)
                and field.dataType.scale == DECIMAL_SCALES[field.name]
            )
        else:
            valid = isinstance(field.dataType, T.StringType)
        if not valid:
            raise ValueError(f"Unexpected source type: {field.name}={field.dataType}")


def build_bronze(spark):
    logger.info("Starting ingestion: source=%s target=%s", SOURCE_PATH, TARGET_TABLE)
    source = spark.read.parquet(SOURCE_PATH)
    logger.info("Source schema: %s", source.schema.simpleString())
    validate_source_schema(source.schema)
    source = source.select(*COLUMNS)

    counts = source.agg(
        F.count("*").alias("source_rows"),
        *[
            F.count(F.when(F.col(column).isNull(), 1)).alias(column)
            for column in REQUIRED_COLUMNS
        ],
    ).first().asDict()
    source_count = counts.pop("source_rows")
    duplicate_keys = source.groupBy("id").count().filter(F.col("count") > 1).count()
    logger.info(
        "Validation: source_rows=%s required_null_counts=%s duplicate_key_groups=%s",
        source_count, counts, duplicate_keys,
    )
    if any(counts.values()) or duplicate_keys:
        raise ValueError("Source validation failed; see counts in the job output")
    if source_count == 0:
        raise ValueError("Empty source rejected to prevent clearing the Bronze table")

    spark.sql("CREATE SCHEMA IF NOT EXISTS ai_dev_class_demo.bronze_layer")
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

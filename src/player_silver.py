# Databricks notebook source
"""Validate complete Bronze snapshots and publish the specified player Silver table."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


SILVER_SCHEMA = "ai_dev_class_demo.silver_layer"
PLAYER_TABLE = "ai_dev_class_demo.bronze_layer.player_master"
REGION_TABLE = "ai_dev_class_demo.bronze_layer.regions"
OUTPUT_TABLE = f"{SILVER_SCHEMA}.player_silver"
OUTPUT_COLUMNS = [
    ("player_id", "BIGINT", "p"),
    ("external_player_key", "STRING", "p"),
    ("first_name", "STRING", "p"),
    ("last_name", "STRING", "p"),
    ("gender", "STRING", "p"),
    ("birth_date", "DATE", "p"),
    ("dominant_hand", "STRING", "p"),
    ("home_region_id", "BIGINT", "p"),
    ("country_code", "STRING", "r"),
    ("region_type", "STRING", "r"),
    ("region_name", "STRING", "r"),
    ("state_province_code", "STRING", "r"),
    ("population", "BIGINT", "r"),
    ("latitude", "DECIMAL(18,6)", "r"),
    ("longitude", "DECIMAL(18,6)", "r"),
    ("registration_date", "DATE", "p"),
    ("player_status", "STRING", "p"),
    ("rating", "DECIMAL(18,3)", "p"),
    ("rating_value", "DECIMAL(18,3)", "p"),
    ("confidence_score", "DECIMAL(18,3)", "p"),
    ("volatility_score", "DECIMAL(18,3)", "p"),
    ("global_percentile", "DECIMAL(18,2)", "p"),
    ("match_count_used", "BIGINT", "p"),
    ("rating_date", "DATE", "p"),
    ("rating_batch_id", "BIGINT", "p"),
    ("snapshot_month", "DATE", "p"),
]
ISSUE_SCHEMA = T.StructType([
    T.StructField("run_timestamp", T.TimestampType()),
    T.StructField("source_table", T.StringType()),
    T.StructField("record_id", T.StringType()),
    T.StructField("issue_code", T.StringType()),
    T.StructField("issue_description", T.StringType()),
])


def same_numeric_value(original, converted):
    """Compare exact numerical meaning, including string decimals, without rounding."""
    if original is None:
        return True
    if converted is None:
        return False
    try:
        before, after = Decimal(original), Decimal(converted)
        return before.is_finite() and after.is_finite() and before == after
    except (InvalidOperation, ValueError):
        return False


numeric_unchanged = F.udf(same_numeric_value, T.BooleanType())


def build_silver(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    run_timestamp = datetime.now(timezone.utc)
    players = spark.table(PLAYER_TABLE)
    regions = spark.table(REGION_TABLE)
    player_count, region_count = players.count(), regions.count()
    issues = {"player_master": [], "regions": []}

    def issue(frame, condition, source, key, code, description):
        issues[source].append(frame.filter(condition).select(
            F.lit(run_timestamp).cast("timestamp").alias("run_timestamp"),
            F.lit(source).alias("source_table"),
            F.col(key).cast("string").alias("record_id"),
            F.lit(code).alias("issue_code"),
            F.lit(description).alias("issue_description"),
        ))

    def validate(frame, source, key, required, strings, casts):
        for column in required:
            issue(frame, F.col(column).isNull(), source, key,
                  "NULL_KEY" if column == key else "NULL_REQUIRED",
                  f"{column} must not be null")
        for column in strings:
            issue(frame, F.col(column).rlike(r"^\s*$"), source, key,
                  "BLANK_REQUIRED", f"{column} must not be blank")
        duplicates = frame.withColumn("__key_count", F.count("*").over(Window.partitionBy(key)))
        issue(duplicates, F.col(key).isNotNull() & (F.col("__key_count") > 1),
              source, key, "DUPLICATE_KEY", f"{key} occurs more than once")
        for column, target_type in casts.items():
            converted = f"__cast_{column}"
            frame = frame.withColumn(converted, F.expr(f"try_cast(`{column}` AS {target_type})"))
            if target_type == "DATE":
                invalid = F.col(column).isNotNull() & (
                    F.col(converted).isNull() | ~F.col(column).rlike(r"^\d{4}-\d{2}-\d{2}$")
                )
                issue(frame, invalid, source, key, "INVALID_DATE",
                      f"{column} must be a valid ISO date")
            else:
                issue(frame, ~numeric_unchanged(F.col(column).cast("string"),
                                               F.col(converted).cast("string")),
                      source, key, "INVALID_NUMERIC_CAST",
                      f"{column} cannot become {target_type} without numerical loss")
        return frame

    player_casts = {
        name: dtype for name, dtype, origin in OUTPUT_COLUMNS
        if origin == "p" and dtype != "STRING"
    }
    region_casts = {
        name: dtype for name, dtype, origin in OUTPUT_COLUMNS
        if origin == "r" and dtype != "STRING"
    }
    region_casts["id"] = "BIGINT"
    players = validate(
        players, "player_master", "player_id",
        ["player_id", "external_player_key", "first_name", "last_name",
         "birth_date", "registration_date", "player_status", "snapshot_month"],
        ["external_player_key", "first_name", "last_name", "player_status"], player_casts,
    )
    regions = validate(regions, "regions", "id", ["id", "country_code", "region_name"],
                       ["country_code", "region_name"], region_casts)

    issue(players, F.col("__cast_birth_date") > F.col("__cast_registration_date"),
          "player_master", "player_id", "DATE_ORDER", "birth_date is after registration_date")
    issue(players, F.col("__cast_match_count_used") < 0, "player_master", "player_id",
          "NEGATIVE_MATCH_COUNT", "match_count_used must be non-negative")
    issue(players, ~F.col("__cast_global_percentile").between(0, 100),
          "player_master", "player_id", "PERCENTILE_RANGE", "global_percentile must be between 0 and 100")
    issue(players, ~F.col("rating").eqNullSafe(F.col("rating_value")),
          "player_master", "player_id", "RATING_MISMATCH", "rating and rating_value must agree, including nulls")
    issue(regions, F.col("__cast_population") < 0, "regions", "id",
          "NEGATIVE_POPULATION", "population must be non-negative")
    for column, limit in [("latitude", 90), ("longitude", 180)]:
        issue(regions, ~F.col(f"__cast_{column}").between(-limit, limit), "regions", "id",
              "COORDINATE_RANGE", f"{column} must be between {-limit} and {limit}")

    # Semi/anti joins report each player once per relationship rule even if region IDs duplicate.
    orphaned = players.filter(F.col("home_region_id").isNotNull()).join(
        regions.select("id").distinct(), F.col("home_region_id") == F.col("id"), "left_anti")
    issue(orphaned, F.lit(True), "player_master", "player_id", "ORPHAN_REGION",
          "Non-null home_region_id has no matching region")
    mismatched = players.alias("p").join(regions.alias("r"),
        (F.col("p.home_region_id") == F.col("r.id"))
        & ~F.col("p.country_code").eqNullSafe(F.col("r.country_code")), "left_semi")
    issue(mismatched, F.lit(True), "player_master", "player_id", "COUNTRY_MISMATCH",
          "Player country_code disagrees with matched region, including nulls")
    issue(players, F.col("home_region_id").isNull() & F.col("country_code").isNotNull(),
          "player_master", "player_id", "COUNTRY_WITHOUT_REGION",
          "country_code must be null when home_region_id is null")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    issue_count = 0
    for source in ("player_master", "regions"):
        table = f"{SILVER_SCHEMA}.{source}_dq_issues"
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
            run_timestamp TIMESTAMP, source_table STRING, record_id STRING,
            issue_code STRING, issue_description STRING
        ) USING DELTA""")
        existing = [(f.name, f.dataType) for f in spark.table(table).schema]
        if existing != [(f.name, f.dataType) for f in ISSUE_SCHEMA]:
            raise ValueError(f"Issue table schema differs from specification: {table}")
        findings = reduce(lambda a, b: a.unionByName(b), issues[source])
        count = findings.count()
        issue_count += count
        if count:
            findings.write.format("delta").mode("append").saveAsTable(table)
            findings.groupBy("source_table", "issue_code").count().show(truncate=False)
    print(f"Bronze players={player_count}, Bronze regions={region_count}, issues={issue_count}")
    if issue_count:
        raise ValueError("Data-quality validation failed; issues appended; player_silver was not overwritten")

    joined = players.alias("p").join(regions.alias("r"),
        F.col("p.home_region_id") == F.col("r.id"), "left")
    output = joined.select(*[
        F.col(f"{origin}.{name}" if dtype == "STRING" else f"{origin}.__cast_{name}")
        .alias(name) for name, dtype, origin in OUTPUT_COLUMNS
    ])
    expected = [(name, dtype.lower()) for name, dtype, _ in OUTPUT_COLUMNS]

    def verify(frame):
        actual = [(f.name, f.dataType.simpleString()) for f in frame.schema]
        # Spark calls BIGINT 'bigint' in simpleString().
        if actual != expected:
            raise ValueError(f"Silver schema differs from specification: {actual}")
        if frame.count() != player_count or frame.select("player_id").distinct().count() != player_count:
            raise ValueError("Silver row count or player uniqueness verification failed")

    verify(output)
    output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)
    published = spark.table(OUTPUT_TABLE)
    verify(published)
    print(f"Bronze players={player_count}, Bronze regions={region_count}, issues=0, Silver rows={published.count()}")


build_silver(spark)

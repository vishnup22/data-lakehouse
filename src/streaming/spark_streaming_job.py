from __future__ import annotations

import os
from datetime import UTC, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.streaming.data_quality import (
    REASON_INVALID_AMOUNT,
    REASON_MALFORMED_JSON,
    REASON_MISSING_ACCOUNT_ID,
    REASON_MISSING_EVENT_ID,
    REASON_MISSING_EVENT_TYPE,
    REASON_MISSING_USER_ID,
    REASON_NULL_EVENT_TIMESTAMP,
    compute_quality_metrics,
)
from src.streaming.quality_metrics import write_quality_metrics

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_events")
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT") or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv(
    "MINIO_ROOT_PASSWORD", "minioadmin"
)
DELTA_PATH = os.getenv("RAW_TABLE_PATH") or os.getenv(
    "DELTA_TABLE_PATH", "s3a://lakehouse/raw/events"
)
QUARANTINE_PATH = os.getenv("QUARANTINE_TABLE_PATH", "s3a://lakehouse/quarantine/events")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH") or os.getenv(
    "DELTA_CHECKPOINT_PATH", "s3a://lakehouse/checkpoints/raw_events"
)
QUALITY_METRICS_PATH = os.getenv("QUALITY_METRICS_PATH", "logs/quality_metrics.jsonl")
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingest_timestamp", StringType(), True),
        StructField("region", StringType(), True),
        StructField("device_type", StringType(), True),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("raw-events-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def rejection_reason_column() -> F.Column:
    data = F.col("data")
    return (
        F.when(data.isNull(), REASON_MALFORMED_JSON)
        .when(
            F.col("data.event_id").isNull() | (F.col("data.event_id") == ""),
            REASON_MISSING_EVENT_ID,
        )
        .when(
            F.col("data.user_id").isNull() | (F.col("data.user_id") == ""), REASON_MISSING_USER_ID
        )
        .when(
            F.col("data.account_id").isNull() | (F.col("data.account_id") == ""),
            REASON_MISSING_ACCOUNT_ID,
        )
        .when(
            F.col("data.event_type").isNull() | (F.col("data.event_type") == ""),
            REASON_MISSING_EVENT_TYPE,
        )
        .when(
            F.col("data.event_timestamp").isNull()
            | (F.col("data.event_timestamp") == "")
            | F.to_timestamp("data.event_timestamp").isNull(),
            REASON_NULL_EVENT_TIMESTAMP,
        )
        .when(
            F.col("data.amount").isNotNull()
            & (F.col("data.amount") != "")
            & (
                F.col("data.amount").cast("double").isNull()
                | (F.col("data.amount").cast("double") < 0)
            ),
            REASON_INVALID_AMOUNT,
        )
    )


def classify_batch(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed = (
        df.select(F.col("value").cast("string").alias("json_value"))
        .withColumn("data", F.from_json("json_value", EVENT_SCHEMA))
        .withColumn("rejection_reason", rejection_reason_column())
        .withColumn("quarantined_at", F.current_timestamp())
    )
    valid = (
        parsed.filter(F.col("rejection_reason").isNull())
        .select("data.*")
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("ingest_timestamp", F.to_timestamp("ingest_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
    )
    quarantine = (
        parsed.filter(F.col("rejection_reason").isNotNull())
        .select(
            "json_value",
            "rejection_reason",
            "quarantined_at",
            F.col("data.event_id").alias("event_id"),
            F.col("data.user_id").alias("user_id"),
            F.col("data.event_type").alias("event_type"),
        )
        .withColumn("quarantine_date", F.to_date("quarantined_at"))
    )
    return (valid, quarantine)


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    valid_df, quarantine_df = classify_batch(batch_df)
    valid_count = valid_df.count()
    bad_count = quarantine_df.count()
    if valid_count > 0:
        valid_df.write.format("delta").mode("append").partitionBy("event_date", "event_hour").save(
            DELTA_PATH
        )
    if bad_count > 0:
        quarantine_df.write.format("delta").mode("append").partitionBy("quarantine_date").save(
            QUARANTINE_PATH
        )
    metrics = compute_quality_metrics(
        valid_count=valid_count,
        bad_count=bad_count,
        batch_id=batch_id,
        timestamp=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    write_quality_metrics(metrics, QUALITY_METRICS_PATH)
    print(
        f"[quality] batch={batch_id} valid={metrics.valid_record_count} bad={metrics.bad_record_count} bad_pct={metrics.bad_record_percentage}%"
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    query = (
        raw_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

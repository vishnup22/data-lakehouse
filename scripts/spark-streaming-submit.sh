#!/usr/bin/env sh
set -eu

S3_ENDPOINT="${S3_ENDPOINT:-${MINIO_ENDPOINT:-http://minio:9000}}"
AWS_KEY="${AWS_ACCESS_KEY_ID:-${MINIO_ROOT_USER:-minioadmin}}"
AWS_SECRET="${AWS_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD:-minioadmin}}"
SPARK_MASTER_URL="${SPARK_MASTER:-spark://spark:7077}"

exec spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
  --conf spark.hadoop.fs.s3a.access.key="${AWS_KEY}" \
  --conf spark.hadoop.fs.s3a.secret.key="${AWS_SECRET}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  /workspace/src/streaming/spark_streaming_job.py

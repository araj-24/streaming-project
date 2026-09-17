from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder.appName("TelemetryStreamingPipeline").getOrCreate()

# -------------------------------------------------------------------------
# DYNAMIC CATALOG & DATABASE CONTEXT SETUP
# -------------------------------------------------------------------------
spark.sql("USE CATALOG workspace")
spark.sql("CREATE DATABASE IF NOT EXISTS default")
spark.sql("USE DATABASE default")

print("Pipeline is locked to context: workspace.default")

# -------------------------------------------------------------------------
# SETUP PATHS & SCHEMAS (UPDATE YOUR S3 BUCKET NAME HERE)
# -------------------------------------------------------------------------
S3_RAW_PATH = "s3://your-telemetry-raw-bucket/events/"
CHECKPOINT_BASE = "s3://your-telemetry-raw-bucket/_checkpoints/"

json_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# -------------------------------------------------------------------------
# BRONZE LAYER: Auto Loader (S3 -> Raw Delta Table)
# -------------------------------------------------------------------------
print("Reading raw stream via Auto Loader...")
raw_stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}bronze_schema")
    .load(S3_RAW_PATH))

bronze_df = raw_stream.withColumn("ingested_at", current_timestamp())

# Write Bronze Stream
bronze_query = (bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}bronze")
    .trigger(availableNow=True)
    .table("workspace.default.telemetry_bronze"))

bronze_query.awaitTermination()

# -------------------------------------------------------------------------
# SILVER LAYER: Cleansing & Schema Enforcement
# -------------------------------------------------------------------------
bronze_read_stream = spark.readStream.table("telemetry_bronze")

silver_df = (bronze_read_stream
    .filter(col("temperature").isNotNull() & (col("temperature") < 150.0))
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
    .select("event_id", "device_id", "temperature", "speed", "status", "event_time", "ingested_at"))

silver_query = (silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}silver")
    .trigger(availableNow=True)
    .table("workspace.default.telemetry_silver"))

silver_query.awaitTermination()

# -------------------------------------------------------------------------
# GOLD LAYER: Streaming Window Aggregations (5-Min Windows)
# -------------------------------------------------------------------------
silver_read_stream = spark.readStream.table("telemetry_silver")

gold_df = (silver_read_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("device_id")
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("speed").alias("avg_speed"),
        count("event_id").alias("total_events")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("device_id"),
        col("avg_temperature"),
        col("avg_speed"),
        col("total_events")
    ))

gold_query = (gold_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}gold")
    .trigger(availableNow=True)
    .table("workspace.default.telemetry_gold"))

gold_query.awaitTermination()
print("Pipeline execution complete.")
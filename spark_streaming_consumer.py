#!/usr/bin/env python3
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, expr, to_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

def ensure_directories():
    """Create necessary directories with appropriate permissions"""
    directories = [
        '/tmp/spark-events',
        '/tmp/spark-temp',
        '/tmp/checkpoint',
        '/tmp/spark-warehouse'
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, mode=0o777, exist_ok=True)
            except Exception as e:
                print(f"Warning: Could not create {directory}: {e}")
                
    # Set environment variable for temporary directory
    os.environ['TMPDIR'] = '/tmp/spark-temp'
    tempfile.tempdir = '/tmp/spark-temp'

def create_spark_session():
    """Create and configure Spark session with optimized settings"""
    # Ensure directories exist
    ensure_directories()
    
    return SparkSession.builder \
        .appName("EmojiProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "true") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.local.dir", "/tmp/spark-temp") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .master("local[*]") \
        .getOrCreate()

def define_schema():
    """Define schema for emoji data"""
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("emoji_type", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def process_emoji_stream(aggregation_ratio=1000):
    """
    Process streaming emoji data from Kafka
    
    Args:
        aggregation_ratio (int): Number of raw emoji counts to aggregate into one scaled count
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    schema = define_schema()

    # Read from Kafka with optimized options
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "emoji_topic") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "10000") \
        .option("fetchOffset.numRetries", "3") \
        .load()

    # Parse JSON and timestamp
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select(
            "data.user_id",
            "data.emoji_type",
            to_timestamp("data.timestamp").alias("event_time")
        )

    # Process in 2-second windows with emoji aggregation
    windowed_counts = parsed_df \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window("event_time", "2 seconds", "2 seconds"),
            "emoji_type"
        ) \
        .agg(count("*").alias("raw_count")) \
        .select(
            "window.start",
            "window.end",
            "emoji_type",
            "raw_count",
            expr(f"CEIL(raw_count / {aggregation_ratio})").alias("scaled_count"),
            lit(aggregation_ratio).alias("aggregation_ratio")
        )

    # Output stream to console
    query = windowed_counts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 100) \
        .trigger(processingTime="2 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    print("Starting Spark Streaming consumer...")
    try:
        process_emoji_stream(aggregation_ratio=1000)
    except KeyboardInterrupt:
        print("\nStopping Spark Streaming consumer...")
    except Exception as e:
        print(f"Error: {str(e)}")

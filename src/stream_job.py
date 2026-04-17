from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("MauritaniaStreamingAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_date", StringType()),
    StructField("season", StringType()),
    StructField("home_team", StringType()),
    StructField("away_team", StringType()),
    StructField("home_goals", IntegerType()),
    StructField("away_goals", IntegerType())
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "mauritania_matches") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("total_match_goals", col("home_goals") + col("away_goals"))

stats_goals = df_parsed.withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "1 minute"), col("home_team")) \
    .agg(sum("home_goals").alias("total_home_goals"))

query_console = stats_goals.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query_parquet = stats_goals.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "outputs/streaming") \
    .option("checkpointLocation", "checkpoints/streaming_analysis") \
    .start()

query_console.awaitTermination()
query_parquet.awaitTermination()

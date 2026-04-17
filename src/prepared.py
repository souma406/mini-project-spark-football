from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

spark = SparkSession.builder \
    .appName("TerminalExport") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("/workspace/data/rim_championnat_results_2007-2025.csv", header=True, inferSchema=True)

df_final = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
             .withColumn("home_goals", col("home_goals").cast("int")) \
             .withColumn("away_goals", col("away_goals").cast("int")) \
             .dropna(subset=["home_goals", "away_goals"]) \
             .withColumn("total_goals", col("home_goals") + col("away_goals")) \
             .withColumn("result", when(col("home_goals") > col("away_goals"), "Home Win")
                                   .when(col("home_goals") < col("away_goals"), "Away Win")
                                   .otherwise("Draw"))

output_path = "/home/jovyan/work/data/prepared_results.parquet"
df_final.coalesce(1).write.mode("overwrite").parquet(output_path)

print(f"--- SUCCESS: Data written to {output_path} ---")



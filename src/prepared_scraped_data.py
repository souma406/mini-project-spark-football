from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

spark = SparkSession.builder \
    .appName("ScrapedDataPrep") \
    .master("local[*]") \
    .getOrCreate()

input_path = "/workspace/data/scraped_current_season.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

df_final = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
             .withColumn("home_goals", col("home_goals").cast("int")) \
             .withColumn("away_goals", col("away_goals").cast("int")) \
             .dropna(subset=["home_goals", "away_goals"]) \
             .withColumn("total_goals", col("home_goals") + col("away_goals")) \
             .withColumn("result", when(col("home_goals") > col("away_goals"), "Home Win")
                                   .when(col("home_goals") < col("away_goals"), "Away Win")
                                   .otherwise("Draw"))

output_path = "/home/jovyan/work/data/scraped.parquet"
df_final.coalesce(1).write.mode("overwrite").parquet(output_path)

print("-" * 50)
print(f"SUCCESS: {input_path} converti en {output_path}")
print("-" * 50)
df_final.show(5)
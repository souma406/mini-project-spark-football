import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("FullMergeFootball") \
    .master("local[*]") \
    .getOrCreate()

path_scraped = "/home/jovyan/work/data/scraped.parquet"
path_prepared = "/home/jovyan/work/data/prepared_results.parquet/"
output_final = "/home/jovyan/work/data/final_mauritania_football_dataset.parquet"

if os.path.exists(path_scraped) and os.path.exists(path_prepared):
    df_scraped = spark.read.parquet(path_scraped)
    df_prepared = spark.read.parquet(path_prepared)
    
    common_cols = ["season", "date", "home_team", "away_team", "home_goals", "away_goals", "total_goals", "result"]
    
    df_final = df_prepared.select(*common_cols).unionByName(df_scraped.select(*common_cols))
    
    df_final.coalesce(1).write.mode("overwrite").parquet(output_final)
    
    print(f"NOMBRE TOTAL DE LIGNES : {df_final.count()}")
    df_final.groupBy("season").count().sort(F.desc("season")).show(10)

else:
    print("Erreur : Fichiers sources introuvables.")

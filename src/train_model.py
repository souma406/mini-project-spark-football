from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, avg, sum as _sum, lag, unix_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("Mauritania_Final_Push").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

train_df = spark.read.parquet("/home/jovyan/work/data/prepared_results.parquet")
test_df = spark.read.parquet("/home/jovyan/work/data/scraped.parquet")

train_df = train_df.toDF(*[c.lower() for c in train_df.columns])
test_df = test_df.toDF(*[c.lower() for c in test_df.columns])

train_df = train_df.withColumn("is_test", when(col("date") < "2000-01-01", False).otherwise(False))
test_df = test_df.withColumn("is_test", when(col("date") < "2000-01-01", True).otherwise(True))

full_df = train_df.unionByName(test_df, allowMissingColumns=True)

full_df = full_df.withColumn("home_pts", when(col("home_goals") > col("away_goals"), 3).when(col("home_goals") == col("away_goals"), 1).otherwise(0))
full_df = full_df.withColumn("away_pts", when(col("away_goals") > col("home_goals"), 3).when(col("away_goals") == col("home_goals"), 1).otherwise(0))
full_df = full_df.withColumn("home_win", when(col("home_pts") == 3, 1).otherwise(0))

win_home_spec = Window.partitionBy("home_team").orderBy("date").rowsBetween(-5, -1)
win_away_spec = Window.partitionBy("away_team").orderBy("date").rowsBetween(-5, -1)
cum_spec_home = Window.partitionBy("home_team").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)
cum_spec_away = Window.partitionBy("away_team").orderBy("date").rowsBetween(Window.unboundedPreceding, -1)

full_df = full_df.withColumn("rolling_home_pts", avg("home_pts").over(win_home_spec))
full_df = full_df.withColumn("rolling_away_pts", avg("away_pts").over(win_away_spec))
full_df = full_df.withColumn("home_win_rate", avg("home_win").over(win_home_spec))

full_df = full_df.withColumn("cum_home_pts", _sum("home_pts").over(cum_spec_home))
full_df = full_df.withColumn("cum_away_pts", _sum("away_pts").over(cum_spec_away))
full_df = full_df.withColumn("pts_diff", col("cum_home_pts") - col("cum_away_pts"))

full_df = full_df.withColumn("date_ts", unix_timestamp(col("date"), "yyyy-MM-dd"))
full_df = full_df.withColumn("prev_match_ts", lag("date_ts").over(Window.partitionBy("home_team").orderBy("date")))
full_df = full_df.withColumn("rest_days", (col("date_ts") - col("prev_match_ts")) / 86400)

full_df = full_df.na.fill(0)

full_df = full_df.withColumn("label", 
    when(col("home_goals") > col("away_goals"), 0.0)
    .when(col("home_goals") == col("away_goals"), 1.0)
    .otherwise(2.0))

train_final = full_df.filter(col("is_test") == False)
test_final = full_df.filter(col("is_test") == True)

indexer_home = StringIndexer(inputCol="home_team", outputCol="home_idx", handleInvalid="keep")
indexer_away = StringIndexer(inputCol="away_team", outputCol="away_idx", handleInvalid="keep")

assembler = VectorAssembler(
    inputCols=["home_idx", "away_idx", "rolling_home_pts", "rolling_away_pts", "home_win_rate", "pts_diff", "rest_days"], 
    outputCol="features"
)

rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=250, maxDepth=15, maxBins=128, seed=42)

pipeline = Pipeline(stages=[indexer_home, indexer_away, assembler, rf])
model = pipeline.fit(train_final)

predictions = model.transform(test_final)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

print("-" * 50)
print(f"FIABILITE : {accuracy * 100:.2f} %")
print("-" * 50)

predictions.select("home_team", "away_team", "pts_diff", "prediction").show(10)

model.write().overwrite().save("/home/jovyan/work/outputs/models/match_predictor_model")

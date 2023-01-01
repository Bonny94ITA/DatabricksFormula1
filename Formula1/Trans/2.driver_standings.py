# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import count, sum, when, col, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

display(race_results_df)

# COMMAND ----------

driver_standing_df = race_results_df \
                    .groupBy("race_year", "driver_name", "driver_nationality") \
                    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

display(driver_standing_df.filter("race_year = 2020"))

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

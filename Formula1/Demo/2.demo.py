# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

races_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

display(races_results_df)

# COMMAND ----------

demo_df = races_results_df.filter("race_year = 2020")

display(demo_df)

# COMMAND ----------

display(demo_df.select(count("*")))

# COMMAND ----------

display(demo_df.select(countDistinct("race_name")))

# COMMAND ----------

display(demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")))

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
                                                .withColumnRenamed("sum(points)", "total_points") \
                                                .withColumnRenamed("count(DISTINCT race_name)", "number_of_races") \
                                                .show()

# COMMAND ----------

demo_df.groupBy("driver_name") \
       .sum("points") \
       .show()

# COMMAND ----------

demo_df.groupBy("driver_name") \
       .agg(sum("points"), countDistinct("race_name").alias("number_of_races")) \
       .withColumnRenamed("sum(points)", "total_points") \
       .show()

# COMMAND ----------

demo_df = races_results_df.filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year", "driver_name") \
                         .agg(sum("points"), countDistinct("race_name").alias("number_of_races")) \
                         .withColumnRenamed("sum(points)", "total_points") \

display(demo_grouped_df)

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(50)

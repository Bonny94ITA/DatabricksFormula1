# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

races_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

display(races_results_df)

# COMMAND ----------

races_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

p_race_year = 2019
race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {p_race_year}")

display(race_results_2019_df)

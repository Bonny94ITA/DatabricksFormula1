# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

display(races_filtered_df)

# COMMAND ----------

races_filtered_df = races_df.filter(races_df["race_year"] == 2019)

display(races_filtered_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')

display(circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_filtered_df.circuit_id, "leftouter")

display(race_circuits_df)

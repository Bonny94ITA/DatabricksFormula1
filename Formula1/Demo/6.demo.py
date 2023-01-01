# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1dl2022/demo'

# COMMAND ----------

results_df = spark.read \
             .option("inferSchema", True) \
             .json("/mnt/formula1dl2022/raw2/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dl2022/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1dl2022/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1dl2022/demo/results_external")
display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

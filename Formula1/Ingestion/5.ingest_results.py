# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl2022/raw

# COMMAND ----------

# DDL Schema
results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)
                                   ])

# COMMAND ----------

results_df = spark.read \
            .option("header", True) \
            .schema(results_schema) \
            .json('dbfs:/mnt/formula1dl2022/raw/results.json')

# Alternativa per i test : .option("inferSchema", True) \

display(results_df)

# COMMAND ----------

display(results_df.describe())

# COMMAND ----------

# results_dropped_df = results_df.drop("statusId")
results_dropped_df = results_df.drop(col("statusId"))

display(results_dropped_df)

# COMMAND ----------

results_final_df = results_dropped_df \
                      .withColumnRenamed("resultId", "result_id") \
                      .withColumnRenamed("raceId", "race_id") \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumnRenamed("constructorId", "constructor_id") \
                      .withColumnRenamed("positionText", "position_text") \
                      .withColumnRenamed("positionOrder", "position_order") \
                      .withColumnRenamed("fastestLap", "fastest_lap") \
                      .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                      .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                      .withColumn("ingestion_date", current_timestamp()) \

display(results_final_df)

# COMMAND ----------

results_final_df.write \
            .mode("overwrite") \
            .partitionBy("race_id") \
            .parquet('dbfs:/mnt/formula1dl2022/processed/results')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl2022/processed/results

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/formula1dl2022/processed/results")

display(df)

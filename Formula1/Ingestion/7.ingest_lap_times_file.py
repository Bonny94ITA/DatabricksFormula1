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
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                   ])

# COMMAND ----------

lap_times_df = spark.read \
            .option("header", True) \
            .schema(lap_times_schema) \
            .csv('dbfs:/mnt/formula1dl2022/raw/lap_times')

# Alternativa per i test: .option("inferSchema", True) \
# Alternativa per prendere determinati file attraverso wildacard *: .csv('dbfs:/mnt/formula1dl2022/raw/lap_times/lap_times*.csv')

display(lap_times_df)

# COMMAND ----------

display(lap_times_df.describe())

# COMMAND ----------

lap_times_final_df = lap_times_df \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumnRenamed("raceId", "race_id") \
                      .withColumn("ingestion_date", current_timestamp()) \

display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write \
            .mode("overwrite") \
            .parquet('dbfs:/mnt/formula1dl2022/processed/lap_times')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl2022/processed/lap_times

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/formula1dl2022/processed/lap_times")

display(df)

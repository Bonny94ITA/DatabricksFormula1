# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl2022/raw

# COMMAND ----------

# DDL Schema
races_schema = StructType(fields = [ StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

races_df = spark.read \
            .option("header", True) \
            .schema(races_schema) \
            .csv('dbfs:/mnt/formula1dl2022/raw/races.csv')

# Alternativa per i test : .option("inferSchema", True) \

display(races_df)

# COMMAND ----------

display(races_df.describe())

# COMMAND ----------

races_renamed_df = races_df \
                      .withColumnRenamed("raceId", "race_id") \
                      .withColumnRenamed("year", "race_year") \
                      .withColumnRenamed("circuitId", "circuit_id") \

display(races_renamed_df)

# COMMAND ----------

races_final_df = races_renamed_df \
                    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
                    .withColumn("ingestion_date", current_timestamp())

# races_final_df = races_renamed_df \
#                     .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
#                     .withColumn("ingestion_date", current_timestamp()) \
#                     .withColumn("env", lit("Production")) \

races_final_df = races_final_df.select("race_id", "race_year", "round", "circuit_id", "name", "ingestion_date", "race_timestamp")

display(races_final_df)

# COMMAND ----------

races_final_df.write \
            .mode("overwrite") \
            .partitionBy("race_year") \
            .parquet('dbfs:/mnt/formula1dl2022/processed/races')

# Bisogna partizionare con cognizione

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl2022/processed/races

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/formula1dl2022/processed/races")

display(df)

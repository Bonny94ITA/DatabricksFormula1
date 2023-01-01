# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f'{processed_folder_path}/pit_stops')

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")

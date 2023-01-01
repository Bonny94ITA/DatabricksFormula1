# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp, concat

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl2022/raw

# COMMAND ----------

# DDL Schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING" 

# COMMAND ----------

constructors_df = spark.read \
            .option("header", True) \
            .schema(constructors_schema) \
            .json(f'{raw_folder_path}/constructors.json')

# Alternativa per i test : .option("inferSchema", True) \

display(constructors_df)

# COMMAND ----------

display(constructors_df.describe())

# COMMAND ----------

# constructors_dropped_df = constructors_df.drop("url")
constructors_dropped_df = constructors_df.drop(col("url"))

display(constructors_dropped_df)

# COMMAND ----------

constructors_final_df = constructors_dropped_df \
                      .withColumnRenamed("constructorId", "constructor_id") \
                      .withColumnRenamed("constructorRef", "constructor_ref") \
                      .withColumn("ingestion_date", current_timestamp()) \
                      .withColumn("data_source", lit(v_data_source))

display(constructors_final_df)

# COMMAND ----------

# constructors_final_df.write \
#             .mode("overwrite") \
#             .parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl2022/processed/constructors

# COMMAND ----------

df = spark.read.parquet(f'{processed_folder_path}/constructors')

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

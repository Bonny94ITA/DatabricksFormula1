# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit, current_timestamp

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
circuits_schema = StructType(fields = [ StructField("circuitId", IntegerType(), False),
                                        StructField("circuitRef", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("location", StringType(), True),
                                        StructField("country", StringType(), True),
                                        StructField("lat", DoubleType(), True),
                                        StructField("lng", DoubleType(), True),
                                        StructField("alt", IntegerType(), True),
                                        StructField("url", StringType(), True)
                                      ])

# COMMAND ----------

circuits_df = spark.read \
            .option("header", True) \
            .schema(circuits_schema) \
            .csv(f'{raw_folder_path}/circuits.csv')

# Alternativa per i test : .option("inferSchema", True) \

display(circuits_df)

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
# circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# Nelle ultime due ?? possibile concatenzare delle funzioni per fare operazioni sulle colonne es. col("circuitId").alias("circuito_id")

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
                      .withColumnRenamed("circuitId", "circuit_id") \
                      .withColumnRenamed("circuitRef", "circuit_ref") \
                      .withColumnRenamed("lat", "latitude") \
                      .withColumnRenamed("lng", "longitude") \
                      .withColumnRenamed("alt", "altitude") \
                      .withColumn("data_source", lit(v_data_source))

display(circuits_renamed_df)

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# circuits_final_df = circuits_renamed_df \
#                     .withColumn("ingestion_date", current_timestamp()) \
#                     .withColumn("env", lit("Production")) \

display(circuits_final_df)

# COMMAND ----------

# circuits_final_df.write \
#             .mode("overwrite") \
#             .parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl2022/processed/circuits

# COMMAND ----------

df = spark.read.parquet(f'{processed_folder_path}/circuits')

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")

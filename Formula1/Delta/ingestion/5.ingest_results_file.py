# Databricks notebook source
spark.read.json("/mnt/formula1dl2022/raw2/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceid
# MAGIC FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

spark.read.json("/mnt/formula1dl2022/raw2/2021-03-28/results.json").createOrReplaceTempView("results_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceid
# MAGIC FROM results_delta
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

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
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Drop duplicates

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed delta in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

#collect mette tutti i dati nella memoria del driver node; bisogna stare attenti!

# COMMAND ----------

results_final_df.select("race_id").distinct().collect()

# COMMAND ----------

# Hard Code
#   from delta.tables import DeltaTable
  
#   if (spark._jsparkSession.catalog().tableExists("f1_processed_delta.results")):
#     deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl2022/processeddelta/results") #merge
    
#     deltaTable.alias("tgt").merge(
#       results_final_df.alias("src"),
#       "tgt.result_id = src.result_id") \
#     .whenMatchedUpdateAll() \
#     .whenNotMatchedInsertAll() \
#     .execute()
#   else:
#     results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed_delta.results")
    
# Per migliorare le performance, nella condizione di merge sarebbe utile inserire anche la partizione altrimenti l'algoritmo, in questo caso, deve cercare all'interno di ogni partizione result_id e solo dopo fare l'update. Bisogna però aggiungere anche dynamicPartitionPruning

spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

from delta.tables import DeltaTable

if (spark._jsparkSession.catalog().tableExists("f1_processed_delta.results")):
  deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl2022/processeddelta/results") #merge

  deltaTable.alias("tgt").merge(
    results_deduped_df.alias("src"),
    "tgt.result_id = src.result_id AND tgt.race_id = src.race_id") \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll() \
  .execute()
else:
  results_deduped_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed_delta.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM f1_processed_delta.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# ERRORE NEL FILE SORGENTE (CI SONO DEI DUPLICATI). FIX:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed_delta.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed_delta.results
# MAGIC WHERE race_id = 540 AND driver_id = 229

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_processed_delta.results

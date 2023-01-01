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
name_schema = StructType(fields = [ StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
])

drivers_schema = StructType(fields = [ StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

drivers_df = spark.read \
            .option("header", True) \
            .schema(drivers_schema) \
            .json('dbfs:/mnt/formula1dl2022/raw/drivers.json')

# Alternativa per i test : .option("inferSchema", True) \

display(drivers_df)

# COMMAND ----------

display(drivers_df.describe())

# COMMAND ----------

# drivers_dropped_df = drivers_df.drop("url")
drivers_dropped_df = drivers_df.drop(col("url"))

display(drivers_dropped_df)

# COMMAND ----------

drivers_final_df = drivers_dropped_df \
                      .withColumnRenamed("driverId", "driver_id") \
                      .withColumnRenamed("driverRef", "driver_ref") \
                      .withColumn("ingestion_date", current_timestamp()) \
                      .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write \
            .mode("overwrite") \
            .parquet('dbfs:/mnt/formula1dl2022/processed/drivers')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1dl2022/processed/drivers

# COMMAND ----------

df = spark.read.parquet("dbfs:/mnt/formula1dl2022/processed/drivers")

display(df)

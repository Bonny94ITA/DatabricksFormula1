-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_results_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
  SELECT *
  FROM race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql;

-- COMMAND ----------

DROP TABLE race_results_sql;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UNMANGED

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_results_df.write.mode("overwrite").format("parquet").option("path", f'{presentation_folder_path}/race_results_ext_py').saveAsTable("race_results_ext_py")

-- COMMAND ----------

SELECT COUNT(1) FROM race_results_ext_py

-- COMMAND ----------

DESCRIBE EXTENDED race_results_ext_py;

-- COMMAND ----------

INSERT INTO race_results_ext_py
SELECT * FROM race_results_python WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM race_results_ext_py

-- COMMAND ----------

SHOW TABLES IN DEMO;

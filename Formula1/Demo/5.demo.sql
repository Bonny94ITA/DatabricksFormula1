-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

USE demo;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
FROM race_results_python
WHERE race_year = 2020
;

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
FROM race_results_python
WHERE race_year = 2012
;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###PERMANENT VIEW

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
SELECT *
FROM race_results_python
WHERE race_year = 2012
;

-- COMMAND ----------

SHOW TABLES;

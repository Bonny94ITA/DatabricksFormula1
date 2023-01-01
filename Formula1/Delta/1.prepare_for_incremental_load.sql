-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed_delta CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed_delta
LOCATION "/mnt/formula1dl2022/processeddelta"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation_delta CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation_delta
LOCATION "/mnt/formula1dl2022/presentationdelta"

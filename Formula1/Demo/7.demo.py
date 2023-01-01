# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl2022/demo/results_managed")
deltaTable.update("position <= 10", {"points": "21 - position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl2022/demo/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

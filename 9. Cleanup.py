# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS tj_db CASCADE

# COMMAND ----------

## DELETE Schema Objects if Necessary 
dbutils.fs.rm("dbfs:/FileStore/tables/TraderJoesRevenue/schema", True)
dbutils.fs.rm("dbfs:/FileStore/tables/TraderJoesRevenue/checkpoints", True)
dbutils.fs.rm("dbfs:/FileStore/tables/TraderJoesRevenue/Files", True)

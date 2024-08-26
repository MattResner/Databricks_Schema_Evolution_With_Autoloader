# Databricks notebook source
# MAGIC %md #Investigating the results of our Bronze Layer Ingestion
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue` LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quarter, count(*) FROM delta.`dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue` GROUP BY quarter;
# MAGIC
# MAGIC --SELECT * FROM delta.`dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue`
# MAGIC
# MAGIC --DELETE FROM delta.`dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue`

# COMMAND ----------

spark.read.json("dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue/_delta_log/*.json")\
.createOrReplaceTempView("delta_log")

display(spark.sql("select * from delta_log where metadata is not null"))

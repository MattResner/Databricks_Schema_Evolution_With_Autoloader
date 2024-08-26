# Databricks notebook source
# MAGIC %md #Running Our Auto Loader Bronze Layer
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



source_data_loc = "dbfs:/FileStore/tables/TraderJoesRevenue/Files/"
target_data_loc = "dbfs:/FileStore/tables/TraderJoesRevenue/bronze_tj_fact_revenue"
checkpoints_loc = "dbfs:/FileStore/tables/TraderJoesRevenue/checkpoints/"
schema_loc = "dbfs:/FileStore/tables/TraderJoesRevenue/schema/"

from pyspark.sql.functions import lit, current_date

# COMMAND ----------

df=(spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("cloudFiles.schemaLocation", schema_loc) \
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
  .load(source_data_loc) \
  )

#we might need to change this to trigger(once=true) to trigger only new files rather than relooking at all files
df.writeStream \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", checkpoints_loc) \
  .outputMode("append") \
  .trigger(availableNow=True) \
  .start(target_data_loc)

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

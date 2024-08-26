# Databricks notebook source
# MAGIC %md #Creating and Configuring Our Trader Joes Database
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the Database
# MAGIC CREATE DATABASE IF NOT EXISTS tj_db
# MAGIC LOCATION 'dbfs:/FileStore/tables/TraderJoesRevenue/';
# MAGIC
# MAGIC
# MAGIC -- Create our Two Tables, tj_fact_revenue and tj_dim_stores

# COMMAND ----------

# Load Databricks utilities library
dbutils.fs.mkdirs("dbfs:/FileStore/tables/TraderJoesRevenue/checkpoints") # Create Checkpoints Directory
dbutils.fs.mkdirs("dbfs:/FileStore/tables/TraderJoesRevenue/Files") # Create drop file path
dbutils.fs.mkdirs("dbfs:/FileStore/tables/TraderJoesRevenue/schema") # Create schema store location


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating Bronze Tables
# MAGIC USE tj_db;
# MAGIC CREATE TABLE IF NOT EXISTS bronze_tj_fact_revenue USING DELTA ;
# MAGIC CREATE TABLE IF NOT EXISTS bronze_tj_dim_stores USING DELTA;
# MAGIC CREATE TABLE IF NOT EXISTS table_logs USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*--Creating Silver Tables
# MAGIC USE tj_db;
# MAGIC CREATE OR REFRESH TABLE silver_tj_fact_revenue
# MAGIC CREATE OR REFRESH TABLE silver_tj_dim_stores*/

# COMMAND ----------

# MAGIC %sql
# MAGIC /*--Creating Gold Tables
# MAGIC USE tj_db;
# MAGIC CREATE OR REFRESH TABLE gold_tj_fact_revenue
# MAGIC CREATE OR REFRESH TABLE gold_tj_dim_stores*/

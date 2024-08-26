# Databricks notebook source
%sql

DROP SCHEMA IF EXISTS tj_db CASCADE

%Python
## DELETE Schema Objects
dbutils.fs.rm("dbfs:/FileStore/tables/TraderJoesRevenue/schema", True)
dbutils.fs.rm("dbfs:/FileStore/tables/TraderJoesRevenue/checkpoints", True)
dbutils.fs.rm("dbfs:/FileStore/tables/TraderJoesRevenue/Files", True)

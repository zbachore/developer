# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## External Table Location

# COMMAND ----------

dbutils.fs.ls("abfss://z-xlc-0262-thef-dv-ue2-dlc02-theframeods@zxlc0149adlsdvue2dls01.dfs.core.windows.net/frameods_dev_lpl/main/SwanRepRestore/cclClaimClasn")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists frameods_dev_lpl.test 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Cannot Create Managed Shallow Clone - Must be Created as External Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE frameods_dev_lpl.test.cclClaimClasn shallow clone frameods_dev_lpl.swanreprestore.clmclaim 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Creating Deep Clone is Not a Problem

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TABLE frameods_dev_lpl.test.cclClaimClasn clone frameods_dev_lpl.swanreprestore.clmclaim 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Shallow Clone Can be Created as "External" Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE OR REPLACE TABLE frameods_dev_lpl.test.cclClaimClasn2 
# MAGIC SHALLOW CLONE frameods_dev_lpl.swanreprestore.clmclaim 
# MAGIC LOCATION 'abfss://z-xlc-0262-thef-dv-ue2-dlc02-theframeods@zxlc0149adlsdvue2dls01.dfs.core.windows.net/frameods_dev_lpl/main/SwanRepRestore/cclClaimClasn2'

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM frameods_dev_lpl.test.cclClaimClasn2 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED frameods_dev_lpl.swanreprestore.clmclaim 

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DESCRIBE EXTENDED frameods_dev_lpl.test.cclClaimClasn2

# COMMAND ----------



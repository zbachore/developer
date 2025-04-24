# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Unity Catalog Structuring Guide

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Access Connectors and Storage Credentials (Admins)

# COMMAND ----------

# DBTITLE 1,Create Storage Credential

# Create Access connector for the storage account per Standards, if it does not exist already.



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create External Locations (Admins)

# COMMAND ----------

# DBTITLE 1,Create External Location to use as a Managed Storage Location for Managed Catalog and Tables
# MAGIC %sql 
# MAGIC -- First Create External Location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `devcatalogexternallocation` 
# MAGIC     URL 'abfss://devcatalog@design4100yearsadls.dfs.core.windows.net/'
# MAGIC     WITH (CREDENTIAL `unitycatalogcredential`)
# MAGIC     COMMENT 'This location is used for all objects that have secret data.';

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- First Create External Location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `prodcatalogexternallocation` 
# MAGIC     URL 'abfss://prodcatalog@design4100yearsadls.dfs.core.windows.net/'
# MAGIC     WITH (CREDENTIAL `unitycatalogcredential`)
# MAGIC     COMMENT 'This location is used for all objects that have secret data. This is the SPL zone';

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- First Create External Location
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `clonecatalogexternallocation` 
# MAGIC     URL 'abfss://clonecatalog@design4100yearsadls.dfs.core.windows.net/'
# MAGIC     WITH (CREDENTIAL `unitycatalogcredential`)
# MAGIC     COMMENT 'This location is used for all objects that have secret data.';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3. Create Catalogs (Admins)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG if not exists devcatalog
# MAGIC MANAGED LOCATION '<external-location-for-devcatalog>';
# MAGIC
# MAGIC CREATE CATALOG if not exists prodcatalog
# MAGIC MANAGED LOCATION '<external-location-for-prodcatalog>';
# MAGIC
# MAGIC CREATE CATALOG if not exists clonecatalog
# MAGIC MANAGED LOCATION '<external-location-for-clonecatalog>';
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Create Schemas (Without Location)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS devcatalog.myschema;
# MAGIC CREATE SCHEMA IF NOT EXISTS prodcatalog.myschema;
# MAGIC CREATE SCHEMA IF NOT EXISTS clonecatalog.myschema;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 4. Create Schemas (With Location)

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE SCHEMA IF NOT EXISTS devcatalog.myschema 
# MAGIC LOCATION '<external-location-for-devcatalog/myschema>';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS prodcatalog.myschema
# MAGIC LOCATION '<external-location-for-prodcatalog/myschema>';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS clonecatalog.myschema
# MAGIC LOCATION '<external-location-for-clonecatalog/myschema>';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 5. Create Volumes (Developers)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Volumes 
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS devcatalog.myschema.files 
# MAGIC LOCATION <external-location-for-devcatalog/myschema/files>'
# MAGIC COMMENT 'This volume holds raw data';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC show catalogs

# COMMAND ----------



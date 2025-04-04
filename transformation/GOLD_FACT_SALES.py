# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CREATE FACT TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### READING SILVER DATA

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.carochz7datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.carochz7datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.carochz7datalake.dfs.core.windows.net", "2b42600d-2c50-4f3f-bc15-52b149c061e5")
spark.conf.set("fs.azure.account.oauth2.client.secret.carochz7datalake.dfs.core.windows.net", "obq8Q~8LxpHzWC0oDxV3SQavZqkggQGT8OXMiaAz")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.carochz7datalake.dfs.core.windows.net", "https://login.microsoftonline.com/361c71e5-e564-4d15-b1a6-284d5d35bb02/oauth2/token")

# COMMAND ----------

df_silver = spark.sql("SELECT * FROM PARQUET.`abfss://silver@carochz7datalake.dfs.core.windows.net/carsales`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### READING ALL THE DIMENSIONS

# COMMAND ----------

df_dealer = spark.sql("SELECT * FROM carsdatabricksorestesnew.gold.dim_dealer")
df_branch = spark.sql("SELECT * FROM carsdatabricksorestesnew.gold.dim_branch")
df_model = spark.sql("SELECT * FROM carsdatabricksorestesnew.gold.dim_model")
df_date = spark.sql("SELECT * FROM carsdatabricksorestesnew.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BRINGING KEYS TO THE FACT TABLE

# COMMAND ----------

df_fact = df_silver.join(df_branch,df_silver['Branch_ID'] == df_branch['Branch_ID'], how = 'left' ) \
    .join(df_dealer,df_silver['Dealer_ID'] == df_dealer['Dealer_ID'], how = 'left' )\
    .join(df_model,df_silver['Model_ID'] == df_model['Model_ID'], how = 'left' )\
    .join(df_date,df_silver['Date_ID'] == df_date['Date_ID'], how = 'left' )\
    .select(df_silver['Revenue'],df_silver['Units_Sold'],df_silver['RevPerUnit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Writing Fact Tables

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('carsdatabricksorestesnew.gold.fact_sales'):
    deltatable = DeltaTable.forName(spark, "carsdatabricksorestesnew.gold.fact_sales")
    deltatable.alias("target").merge(df_fact.alias('source'), 'target.dim_branch_key = source.dim_branch_key AND target.dim_dealer_key = source.dim_dealer_key AND target.dim_model_key = source.dim_model_key AND target.dim_date_key = source.dim_date_key') \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact.write.format("delta")\
    .mode("overwrite")\
    .option('path','abfss://gold@carochz7datalake.dfs.core.windows.net/fact_sales')\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from carsdatabricksorestesnew.gold.fact_sales
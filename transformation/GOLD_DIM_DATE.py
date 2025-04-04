# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.carochz7datalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.carochz7datalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.carochz7datalake.dfs.core.windows.net", "2b42600d-2c50-4f3f-bc15-52b149c061e5")
spark.conf.set("fs.azure.account.oauth2.client.secret.carochz7datalake.dfs.core.windows.net", "obq8Q~8LxpHzWC0oDxV3SQavZqkggQGT8OXMiaAz")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.carochz7datalake.dfs.core.windows.net", "https://login.microsoftonline.com/361c71e5-e564-4d15-b1a6-284d5d35bb02/oauth2/token")


# COMMAND ----------

gold_path = "abfss://gold@carochz7datalake.dfs.core.windows.net/dim_date"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## CREATE FLAG PARAMETER 

# COMMAND ----------

dbutils.widgets.text("incremental_flag", '0')


# COMMAND ----------

incremental_flag = dbutils.widgets.get("incremental_flag")

print(incremental_flag)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## CREATING DIMENSION MODEL
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Fetch relatine columns

# COMMAND ----------


df_src = spark.sql('''
SELECT DISTINCT(Date_ID) as Date_ID FROM parquet.`abfss://silver@carochz7datalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### dim_model SINK - Initial and Incremental (JUST BRIBG SCHEMA IF TABLE NOT EXISTS)

# COMMAND ----------

if spark.catalog.tableExists('carsdatabricksorestesnew.gold.dim_date'):
    df_sink = spark.sql('''
    SELECT dim_date_key, Date_ID
    FROM PARQUET.`abfss://silver@carochz7datalake.dfs.core.windows.net/carsales`
        ''')
    
else:
    df_sink = spark.sql('''
    SELECT 1 AS dim_date_key, Date_ID
    FROM PARQUET.`abfss://silver@carochz7datalake.dfs.core.windows.net/carsales`
    WHERE 1=0
        ''')
    


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Date_ID'] == df_sink['Date_ID'], 'left').select(df_src['Date_ID'], df_sink['dim_date_key'])



# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 

# COMMAND ----------

# MAGIC %md
# MAGIC ## **df_filter_old**

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_date_key').isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** df_filter_new**

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_date_key').isNull()).select(df_src['Date_ID'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATE SUROGATE KEY

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Fetch the max Surrogate Key from existing table**
# MAGIC

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql("SELECT MAX(dim_date_key) FROM carsdatabricksorestesnew.gold.dim_date")
    max_value = max_value_df.collect()[0][0]


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Create surogate key columns and ADD the max surogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_date_key', max_value + monotonically_increasing_id() )

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### CREATE FINAL DF -df_filter_old + new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1 - UPSERT

# COMMAND ----------

# INCREMENTAL RUN
if spark.catalog.tableExists("carsdatabricksorestesnew.gold.dim_date"):
    delta_table = DeltaTable.forName(spark, "carsdatabricksorestesnew.gold.dim_date")

    delta_table.alias("target").merge(
        df_final.alias("source"),
        "target.dim_date_key = source.dim_date_key"
    ).whenMatchedUpdateAll()\
     .whenNotMatchedInsertAll()\
     .execute()

# INITIAL RUN
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable("carsdatabricksorestesnew.gold.dim_date")  # ✅ Guarda en metastore


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM carsdatabricksorestesnew.gold.dim_date
# MAGIC
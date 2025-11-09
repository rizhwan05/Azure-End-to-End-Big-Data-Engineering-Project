# Databricks notebook source
spark
import os

# COMMAND ----------

# MAGIC %md
# MAGIC # Connecting to Data Lake

# COMMAND ----------

storage_account = "olistdatastorageaccntriz"
application_id = "7098b2c6-b18e-47d6-9bfb-1131512d4a00"
directory_id = "741736bc-3e8f-4c72-83c3-6830e05b5639"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
# Use an environment variable for the Azure client secret so secrets are not stored in source control.
# Set AZURE_CLIENT_SECRET in your environment or in Databricks secrets and never commit the real value.
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
    os.environ.get("AZURE_CLIENT_SECRET", "<AZURE_CLIENT_SECRET>")
)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from lake as Dataframe

# COMMAND ----------

base_path = "abfss://olistdata@olistdatastorageaccntriz.dfs.core.windows.net/bronze/"
orders_path = base_path + "olist_orders_dataset.csv"
payments_path = base_path + "olist_order_payments_dataset.csv"
reviews_path = base_path + "olist_order_reviews_dataset.csv"
items_path = base_path + "olist_order_items_dataset.csv"
customers_path = base_path + "olist_customers_dataset.csv"
sellers_path = base_path + "olist_sellers_dataset.csv"
geolocation_path = base_path + "olist_geolocation_dataset.csv"
products_path = base_path + "olist_products_dataset.csv"

orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
payments_df = spark.read.format("csv").option("header", "true").load (payments_path)
reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
items_df = spark.read.format("csv").option("header", "true").load(items_path)
customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
products_df = spark.read.format("csv").option("header", "true").load(products_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from MongoDb and converting it to pandas dataframe

# COMMAND ----------

from pymongo import mongo_client

# COMMAND ----------

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Use an environment variable for the MongoDB connection URI. Set MONGODB_URI in your environment
# (e.g., mongodb+srv://<user>:<password>@cluster.../?appName=...) and do not commit credentials.
uri = os.environ.get("MONGODB_URI", "<MONGODB_URI>")

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

mydatabase = client["OlistDataNoSQL"]
mydatabase

# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']

mongo_data = pd.DataFrame(list(collection.find()))

# COMMAND ----------

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning Data

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, current_date, when

# COMMAND ----------

def clean_dataframe(df, name):
    print("cleaning "+ name)
    return df.dropDuplicates().na.drop('all')

orders_df = clean_dataframe(orders_df, "Orders")

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# convert date columns

orders_df = orders_df.withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))\
                     .withColumn("order_approved_at", to_date(col("order_approved_at"), "yyyy-MM-dd HH:mm:ss"))\
                     .withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date"), "yyyy-MM-dd HH:mm:ss"))\
                     .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"), "yyyy-MM-dd HH:mm:ss"))\
                     .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"))

display(orders_df)

# COMMAND ----------

payments_df = payments_df.withColumn("payment_sequential", col("payment_sequential").cast("int"))\
                         .withColumn("payment_installments", col("payment_installments").cast("int"))\
                         .withColumn("payment_value", col("payment_value").cast("float"))\
                         .withColumn("order_id", col("order_id").cast("string"))
display(payments_df)

# COMMAND ----------

review_df = reviews_df.withColumn("review_id", col("review_id").cast("string"))\
                      .withColumn("order_id", col("order_id").cast("string"))\
                      .withColumn("review_score", col("review_score").cast("int"))\
                      .withColumn("review_creation_date", to_date(col("review_creation_date"), "yyyy-MM-dd HH:mm:ss"))\
                      .withColumn("review_answer_timestamp", to_date(col("review_answer_timestamp"), "yyyy-MM-dd HH:mm:ss"))

display(reviews_df)

# COMMAND ----------

items_df = items_df.withColumn("order_id", col("order_id").cast("string"))\
                   .withColumn("order_item_id", col("order_item_id").cast("int"))\
                   .withColumn("product_id", col("product_id").cast("string"))\
                   .withColumn("seller_id", col("seller_id").cast("string"))\
                   .withColumn("shipping_limit_date", to_date(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))\
                   .withColumn("freight_value", col("freight_value").cast("double"))\
                   .withColumn("price", col("price").cast("double"))

display(items_df)

# COMMAND ----------

orders_df = orders_df.withColumn("actual_delivery_time", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
orders_df = orders_df.withColumn("estimated_delivery_time", datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")))
orders_df = orders_df.withColumn("delay_time", col("actual_delivery_time") - col("estimated_delivery_time"))

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Joining

# COMMAND ----------

orders_customers_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id, "left")

orders_payments_df = orders_customers_df.join(payments_df, orders_customers_df.order_id == payments_df.order_id, "left")

orders_items_df = orders_payments_df.join(items_df, "order_id", "left")

orders_items_products_df = orders_items_df.join(products_df, orders_items_df.product_id == products_df.product_id, "left")

final_df = orders_items_products_df.join(sellers_df, orders_items_products_df.seller_id == sellers_df.seller_id, "left")


# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop('_id', axis=1, inplace=True)

mongo_spark_data = spark.createDataFrame(mongo_data)

# COMMAND ----------

display(mongo_spark_data)

# COMMAND ----------

final_df = final_df.join(mongo_spark_data, "product_category_name", "left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns

    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)

    df_cleaned = df.drop(*columns_to_drop)

    return df_cleaned

final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://olistdata@olistdatastorageaccntriz.dfs.core.windows.net/silver")

#!/usr/bin/env python3

import sys
import json
import numpy as np
import joblib

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    StructType, StructField, DoubleType, IntegerType
)

###############################
# 1) Create Spark Session
###############################
print("Creating SparkSession...")
spark = SparkSession.builder \
    .appName("KafkaPaySimAnomaly") \
    .master("local[*]") \
    .getOrCreate()

###############################
# 2) Load Isolation Forest model
###############################
iso_model_path = "/home/monishaapatro/iso_forest.pkl"  # adjust if needed
print(f"Loading model from {iso_model_path}...")
iso_forest = joblib.load(iso_model_path)

###############################
# 3) Define schema (13 columns)
###############################
schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("type_CASH_OUT", IntegerType(), True),
    StructField("type_DEBIT", IntegerType(), True),
    StructField("type_PAYMENT", IntegerType(), True),
    StructField("type_TRANSFER", IntegerType(), True),
    StructField("delta_balanceOrig", DoubleType(), True),
    StructField("delta_balanceDest", DoubleType(), True),
    StructField("amount_div_oldbalanceOrg", DoubleType(), True),
    # isFraud is optional if you want it for evaluation
    # StructField("isFraud", IntegerType(), True)
])

###############################
# 4) Connect to Kafka
###############################
print("Connecting to Kafka...")
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "paysim_transactions") \
    .option("startingOffsets", "latest") \
    .load()

###############################
# 5) Parse the JSON messages
###############################
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

###############################
# 6) UDF: Predict with 13 columns
###############################
def predict_iso(step, amount, oldbalanceOrg, newbalanceOrig,
                oldbalanceDest, newbalanceDest,
                type_CASH_OUT, type_DEBIT, type_PAYMENT, type_TRANSFER,
                delta_balanceOrig, delta_balanceDest,
                amount_div_oldbalanceOrg):
    # shape=(1,13) to match your trained model
    features = np.array([[
        step,
        amount,
        oldbalanceOrg,
        newbalanceOrig,
        oldbalanceDest,
        newbalanceDest,
        type_CASH_OUT,
        type_DEBIT,
        type_PAYMENT,
        type_TRANSFER,
        delta_balanceOrig,
        delta_balanceDest,
        amount_div_oldbalanceOrg
    ]])
    pred = iso_forest.predict(features)[0]  # returns -1 or 1
    return 1 if pred == -1 else 0  # 1 => anomaly

from pyspark.sql.types import IntegerType
predict_udf = udf(predict_iso, IntegerType())

# Apply the UDF
df_scored = df_parsed.withColumn(
    "IF_Anomaly",
    predict_udf(
        col("step"),
        col("amount"),
        col("oldbalanceOrg"),
        col("newbalanceOrig"),
        col("oldbalanceDest"),
        col("newbalanceDest"),
        col("type_CASH_OUT"),
        col("type_DEBIT"),
        col("type_PAYMENT"),
        col("type_TRANSFER"),
        col("delta_balanceOrig"),
        col("delta_balanceDest"),
        col("amount_div_oldbalanceOrg")
    )
)

###############################
# 7) Filter anomalies or just show them
###############################
df_anomalies = df_scored.filter(col("IF_Anomaly") == 1)

###############################
# 8) Use CSV sink
###############################
# Use CSV sink:
query = df_anomalies.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/home/monishaapatro/flagged_anomalies") \
    .option("checkpointLocation", "/home/monishaapatro/checkpoints/flagged_anomalies") \
    .start()
query.awaitTermination()

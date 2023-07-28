#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import get_json_object, month, avg, col, lit, sum
from datetime import datetime


from influxdb import InfluxDBClient, DataFrameClient
client = InfluxDBClient(host='localhost', port=8086, database='flight', username="user", password="pass")
client.switch_database('flight')

cluster_seeds = ['localhost:9042', 'localhost:9043']
# initialize the SparkSession

spark = SparkSession \
    .builder \
    .appName("Flight Streaming Analysis") \
    .config("spark.cassandra.connection.host", ','.join(cluster_seeds)) \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

# DF that cyclically reads events from Kafka
df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "live-data") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df = df_kafka.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df = df.select(
    get_json_object(df.value, '$.FL_DATE').alias('FL_DATE'),
    get_json_object(df.value, '$.OP_UNIQUE_CARRIER').alias('OP_UNIQUE_CARRIER'),
    get_json_object(df.value, '$.OP_CARRIER_FL_NUM').alias('OP_CARRIER_FL_NUM').cast("int"),
    get_json_object(df.value, '$.ORIGIN').alias('ORIGIN'),
    get_json_object(df.value, '$.DEST').alias('DEST'),
    get_json_object(df.value, '$.DEP_TIME').alias('DEP_TIME').cast("int"),
    get_json_object(df.value, '$.DEP_DELAY').alias('DEP_DELAY').cast("int"),
    get_json_object(df.value, '$.TAXI_OUT').alias('TAXI_OUT').cast("int"),
    get_json_object(df.value, '$.WHEELS_OFF').alias('WHEELS_OFF').cast("int"),
    get_json_object(df.value, '$.WHEELS_ON').alias('WHEELS_ON').cast("int"),
    get_json_object(df.value, '$.TAXI_IN').alias('TAXI_IN').cast("int"),
    get_json_object(df.value, '$.ARR_TIME').alias('ARR_TIME').cast("int"),
    get_json_object(df.value, '$.ARR_DELAY').alias('ARR_DELAY').cast("int"),
    get_json_object(df.value, '$.AIR_TIME').alias('AIR_TIME').cast("int"),
    get_json_object(df.value, '$.DISTANCE').alias('DISTANCE').cast("int"),
    get_json_object(df.value, '$.CARRIER_DELAY').alias('CARRIER_DELAY').cast("int"),
    get_json_object(df.value, '$.WEATHER_DELAY').alias('WEATHER_DELAY').cast("int"),
    get_json_object(df.value, '$.NAS_DELAY').alias('NAS_DELAY').cast("int"),
    get_json_object(df.value, '$.SECURITY_DELAY').alias('SECURITY_DELAY').cast("int"),
    get_json_object(df.value, '$.LATE_AIRCRAFT_DELAY').alias('LATE_AIRCRAFT_DELAY').cast("int")
)

df = df.withColumnRenamed("OP_UNIQUE_CARRIER", "OP_CARRIER") \
    .withColumn("ACTUAL_ELAPSED_TIME", col("AIR_TIME") + col("TAXI_IN") + col("TAXI_OUT")) \
    .withColumn("CRS_DEP_TIME", col("DEP_TIME") - col("DEP_DELAY")) \
    .withColumn("CRS_ARR_TIME", col("ARR_TIME") - col("ARR_DELAY")) \
    .withColumn("CRS_ELAPSED_TIME", col("ACTUAL_ELAPSED_TIME") - (col("DEP_DELAY") + col("ARR_DELAY"))) \
    .withColumn("CANCELLED", lit(None)) \
    .withColumn("CANCELLATION_CODE", lit(None)) \
    .withColumn("DIVERTED", lit(None))


"""
========================================================================================
Delay Analysis per Carrier, per Month
========================================================================================
"""
delay_df = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .drop_duplicates() \
    .withColumn("MONTH", month("FL_DATE")) \
    .groupBy("OP_CARRIER", "MONTH") \
    .agg(avg("DEP_DELAY").alias("DEP_DELAY"), avg("ARR_DELAY").alias("ARR_DELAY")) \
    .select("OP_CARRIER", "MONTH", "DELAY")


def writing(df, i) :
    df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "streamingkeyspace") \
    .option("table", "delay_data") \
    .mode("append")\
    .save()

delay_query = delay_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(writing) \
    .outputMode("update") \
    .start()


"""
========================================================================================
Analysis for Grafana
========================================================================================
"""

def saveToInflux(row):
    timestamp = datetime.now()
    json_body = [
        {
            "measurement": "raw_flights",
            "tags": {
                "brushId": row["OP_CARRIER_FL_NUM"],
                "ORIGIN_ID": row["ORIGIN"],
                "DEST_ID": row["DEST"],
            },
            "time": timestamp,
            "fields": {
                "FL_DATE": row["FL_DATE"],
                "OP_CARRIER": row["OP_CARRIER"],
                "OP_CARRIER_FL_NUM": row["OP_CARRIER_FL_NUM"],
                "ORIGIN": row["ORIGIN"],
                "DEST": row["DEST"],
                "CRS_DEP_TIME": row["DEP_DELAY"],
                "DEP_TIME": row["DEP_TIME"],
                "DEP_DELAY": row["DEP_DELAY"],
                "TAXI_OUT": row["TAXI_OUT"],
                "WHEELS_OFF": row["WHEELS_OFF"],
                "WHEELS_ON": row["WHEELS_ON"],
                "TAXI_IN": row["TAXI_IN"],
                "CRS_ARR_TIME": row["CRS_ARR_TIME"],
                "ARR_TIME": row["ARR_TIME"],
                "ARR_DELAY": row["ARR_DELAY"],
                "CANCELLED": row["CANCELLED"],
                "CANCELLATION_CODE": row["CANCELLATION_CODE"],
                "DIVERTED": row["DIVERTED"],
                "CRS_ELAPSED_TIME": row["CRS_ELAPSED_TIME"],
                "ACTUAL_ELAPSED_TIME": row["ACTUAL_ELAPSED_TIME"],
                "AIR_TIME": row["AIR_TIME"],
                "DISTANCE": row["DISTANCE"],
                "CARRIER_DELAY": row["CARRIER_DELAY"],
                "WEATHER_DELAY": row["WEATHER_DELAY"],
                "NAS_DELAY": row["NAS_DELAY"],
                "SECURITY_DELAY": row["SECURITY_DELAY"],
                "LATE_AIRCRAFT_DELAY": row["LATE_AIRCRAFT_DELAY"],

            }
        }
    ]
    client.write_points(json_body)

def saveToInfluxGraph(row):
    timestamp = datetime.now()
    json_body = [
        {
            "measurement": "edges_graph",
            "tags": {
                "AIR_TIME": row["AIR_TIME"]
            },
            "time": timestamp,
            "fields": {
                "id": int(float(timestamp.timestamp()) * 1000000),
                "source": row["ORIGIN"],
                "target": row["DEST"],
                "mainStat": row["AIR_TIME"]
            }
        }
    ]
    client.write_points(json_body)
    json_body = [
        {
            "measurement": "nodes_graph",
            "tags": {
                "AIR_TIME": row["AIR_TIME"]
            },
            "time": timestamp,
            "fields": {
                "id": row["ORIGIN"],
            }
        }
    ]
    client.write_points(json_body)
    json_body = [
        {
            "measurement": "nodes_graph",
            "tags": {
                "AIR_TIME": row["AIR_TIME"]
            },
            "time": timestamp,
            "fields": {
                "id": row["DEST"],
            }
        }
    ]
    client.write_points(json_body)


graph_df = df.select("ORIGIN", "DEST", "AIR_TIME") \
    .drop_duplicates() \
    .filter(col("AIR_TIME").isNotNull()) \
    .groupBy("ORIGIN", "DEST") \
    .agg(sum(col("AIR_TIME")).alias("AIR_TIME")) \
    .where(col("AIR_TIME") >= 2000)

"""
========================================================================================
Writing Streaming
========================================================================================
"""

influx_query = df.writeStream \
    .foreach(saveToInflux) \
    .start()

graph_query = graph_df.writeStream \
    .foreach(saveToInfluxGraph) \
    .outputMode("update") \
    .start()

delay_query.awaitTermination()
influx_query.awaitTermination()
graph_query.awaitTermination()




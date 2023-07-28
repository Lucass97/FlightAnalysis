#!/usr/bin/env python3
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, month, avg, year, dayofweek, count, when, sum, last_day, next_day, dayofyear, \
    dayofmonth, datediff, row_number, lit, max


cluster_seeds = ['localhost:9042', 'localhost:9043']

spark = SparkSession \
    .builder \
    .appName("Flight Batch Analysis") \
    .config("spark.cassandra.connection.host", ','.join(cluster_seeds)) \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()


df = spark.read.option("header", True).csv("hdfs://localhost:9000/input/flight_data.csv").cache()

df.printSchema()

"""
========================================================================================
Delay Analysis per Carrier
========================================================================================
"""

delay_total_df = df.select("OP_CARRIER", "DEP_DELAY", "ARR_DELAY") \
    .groupBy("OP_CARRIER") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("OP_CARRIER", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

delay_year_df = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("OP_CARRIER", "YEAR", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

delay_year_month_df = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .withColumn("YEAR", year("FL_DATE")) \
    .withColumn("MONTH", month("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR", "MONTH") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("OP_CARRIER", "YEAR", "MONTH", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

delay_dayofweek_df = df.select("OP_CARRIER", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .withColumn("DAY_OF_WEEK", dayofweek("FL_DATE")) \
    .groupBy("OP_CARRIER", "DAY_OF_WEEK") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("OP_CARRIER", "DAY_OF_WEEK", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

"""
========================================================================================
Delay Analysis per Source-Dest
========================================================================================
"""

delay_total_src_dest_df = df.select("ORIGIN", "DEST", "DEP_DELAY", "ARR_DELAY") \
    .groupBy("ORIGIN", "DEST") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("ORIGIN", "DEST", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

delay_year_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "YEAR") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("ORIGIN", "DEST", "YEAR", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

delay_year_month_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .withColumn("YEAR", year("FL_DATE")) \
    .withColumn("MONTH", month("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "YEAR", "MONTH") \
    .agg( avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("ORIGIN", "DEST", "YEAR", "MONTH", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

delay_dayofweek_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "DEP_DELAY", "ARR_DELAY") \
    .withColumn("DAY_OF_WEEK", dayofweek("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "DAY_OF_WEEK") \
    .agg(avg("DEP_DELAY").alias("AVG_DEP_DELAY"), avg("ARR_DELAY").alias("AVG_ARR_DELAY")) \
    .select("ORIGIN", "DEST", "DAY_OF_WEEK", "AVG_DEP_DELAY", "AVG_ARR_DELAY")

"""
========================================================================================
Cancellation & Diverted Analysis per Carrier
========================================================================================
"""

cancellation_diverted_total_df = df.select("OP_CARRIER", "CANCELLED", "DIVERTED") \
    .groupBy("OP_CARRIER") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("OP_CARRIER", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("OP_CARRIER")

cancellation_diverted_year_df = df.select("OP_CARRIER", "FL_DATE", "CANCELLED", "DIVERTED") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("OP_CARRIER", "YEAR", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("YEAR", "OP_CARRIER")

cancellation_diverted_year_month_df = df.select("OP_CARRIER", "FL_DATE", "CANCELLED", "DIVERTED") \
    .withColumn("YEAR", year("FL_DATE")) \
    .withColumn("MONTH", month("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR", "MONTH") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("OP_CARRIER", "YEAR", "MONTH", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("YEAR", "MONTH", "OP_CARRIER",)

cancellation_diverted_dayofweek_df = df.select("OP_CARRIER", "FL_DATE", "CANCELLED", "DIVERTED") \
    .withColumn("DAY_OF_WEEK", dayofweek("FL_DATE")) \
    .groupBy("OP_CARRIER", "DAY_OF_WEEK") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("OP_CARRIER", "DAY_OF_WEEK", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("DAY_OF_WEEK", "OP_CARRIER")

"""
========================================================================================
Cancellation & Diverted Analysis per Origin Destination
========================================================================================
"""

cancellation_diverted_total_src_dest_df = df.select("ORIGIN", "DEST", "CANCELLED", "DIVERTED") \
    .groupBy("ORIGIN", "DEST") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("ORIGIN", "DEST", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("ORIGIN", "DEST")

cancellation_diverted_year_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "DIVERTED") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "YEAR") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("ORIGIN", "DEST", "YEAR", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("YEAR", "ORIGIN", "DEST")

cancellation_diverted_year_month_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "DIVERTED") \
    .withColumn("YEAR", year("FL_DATE")) \
    .withColumn("MONTH", month("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "YEAR", "MONTH") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("ORIGIN", "DEST", "YEAR", "MONTH", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("YEAR","MONTH", "ORIGIN", "DEST")

cancellation_diverted_dayofweek_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "DIVERTED") \
    .withColumn("DAY_OF_WEEK", dayofweek("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "DAY_OF_WEEK") \
    .agg(count(when(col("CANCELLED") == 1, 1)).alias("CANC_COUNT"), count(when(col("DIVERTED") == 1, 1)).alias("DIV_COUNT"), count("*").alias("COUNT")) \
    .withColumn("DIV_PERC", (col("DIV_COUNT") / col("COUNT") * 100.0)) \
    .withColumn("CANC_PERC", (col("CANC_COUNT") / col("COUNT") * 100.0)) \
    .select("ORIGIN", "DEST", "DAY_OF_WEEK", "CANC_PERC", "CANC_COUNT", "DIV_PERC", "DIV_COUNT") \
    .orderBy("DAY_OF_WEEK", "ORIGIN", "DEST")

"""
========================================================================================
Distance Analysis per Carrier
========================================================================================
"""

dist_total_df = df.select("OP_CARRIER", "DISTANCE") \
    .groupBy("OP_CARRIER") \
    .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
    .select("OP_CARRIER", "TOTAL_DISTANCE") \
    .orderBy("OP_CARRIER")

dist_year_df = df.select("OP_CARRIER", "FL_DATE", "DISTANCE") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR") \
    .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
    .select("OP_CARRIER", "YEAR", "TOTAL_DISTANCE") \
    .orderBy("YEAR", "OP_CARRIER")

dist_year_month_df = df.select("OP_CARRIER", "FL_DATE", "DISTANCE") \
    .withColumn("YEAR", year("FL_DATE")) \
    .withColumn("MONTH", month("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR", "MONTH") \
    .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
    .select("OP_CARRIER", "YEAR", "MONTH", "TOTAL_DISTANCE") \
    .orderBy("YEAR", "MONTH", "OP_CARRIER")

dist_dayofweek_df = df.select("OP_CARRIER", "FL_DATE", "DISTANCE") \
    .withColumn("DAY_OF_WEEK", dayofweek("FL_DATE")) \
    .groupBy("OP_CARRIER", "DAY_OF_WEEK") \
    .agg(sum(col("DISTANCE")).alias("TOTAL_DISTANCE")) \
    .select("OP_CARRIER", "DAY_OF_WEEK", "TOTAL_DISTANCE") \
    .orderBy("DAY_OF_WEEK", "OP_CARRIER")

"""
========================================================================================
Max consec days of Delay Analysis per Carrier
========================================================================================
"""

max_consec_delay_year_df = df.select("OP_CARRIER", "FL_DATE", "ARR_DELAY") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("OP_CARRIER", "YEAR", "FL_DATE") \
    .agg(avg(col("ARR_DELAY")).alias("ARR_DELAY")) \
    .filter(col("ARR_DELAY") > 0) \
    .withColumn("ROW_NUMBER", row_number().over(Window.partitionBy("OP_CARRIER", "YEAR").orderBy("OP_CARRIER", "YEAR", "FL_DATE"))) \
    .withColumn("GRP", datediff(col("FL_DATE"), lit("1900-1-1")) - col("ROW_NUMBER")) \
    .withColumn("GIORNI", row_number().over(Window.partitionBy("OP_CARRIER", "YEAR", "GRP").orderBy("OP_CARRIER", "YEAR", "FL_DATE"))) \
    .groupBy("OP_CARRIER", "YEAR") \
    .agg(max(col("GIORNI")).alias("MAX_GIORNI")) \
    .select("OP_CARRIER", "YEAR", "MAX_GIORNI")

max_consec_delay_year_src_dest_df = df.select("ORIGIN", "DEST", "FL_DATE", "ARR_DELAY") \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "YEAR", "FL_DATE") \
    .agg(avg(col("ARR_DELAY")).alias("ARR_DELAY")) \
    .filter(col("ARR_DELAY") > 0) \
    .withColumn("ROW_NUMBER", row_number().over(Window.partitionBy("ORIGIN", "DEST", "YEAR").orderBy("ORIGIN", "DEST", "YEAR", "FL_DATE"))) \
    .withColumn("GRP", datediff(col("FL_DATE"), lit("1900-1-1")) - col("ROW_NUMBER")) \
    .withColumn("GIORNI", row_number().over(Window.partitionBy("ORIGIN", "DEST", "YEAR", "GRP").orderBy("ORIGIN", "DEST", "YEAR", "FL_DATE"))) \
    .groupBy("ORIGIN", "DEST", "YEAR") \
    .agg(max(col("GIORNI")).alias("MAX_GIORNI")) \
    .select("ORIGIN", "DEST", "YEAR", "MAX_GIORNI")

"""
========================================================================================
Group by Source-Dest and Cancellation Code 
========================================================================================
"""
src_dest_canc_code_df = df.select("ORIGIN", "DEST", "FL_DATE", "CANCELLED", "CANCElLATION_CODE") \
    .filter(col("CANCELLED").isNotNull()) \
    .filter(col("CANCELLED") == 1) \
    .withColumn("YEAR", year("FL_DATE")) \
    .groupBy("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE") \
    .agg(count(col("CANCELLED")).alias("NUM_CANCELLED")) \
    .select("ORIGIN", "DEST", "YEAR", "CANCELLATION_CODE", "NUM_CANCELLED")

"""
========================================================================================
Writing Result to Cassandra
========================================================================================
"""

delay_total_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_total") \
    .mode("append") \
    .save()

delay_year_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_year") \
    .mode("append") \
    .save()

delay_year_month_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_year_month") \
    .mode("append") \
    .save()

delay_dayofweek_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_dayofweek") \
    .mode("append") \
    .save()

"""
========================================================================================
"""

delay_total_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_total_src_dest") \
    .mode("append") \
    .save()

delay_year_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_year_src_dest") \
    .mode("append") \
    .save()

delay_year_month_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_year_month_src_dest") \
    .mode("append") \
    .save()

delay_dayofweek_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "delay_dayofweek_src_dest") \
    .mode("append") \
    .save()

"""
========================================================================================
"""

cancellation_diverted_total_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_total") \
    .mode("append") \
    .save()

cancellation_diverted_year_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_year") \
    .mode("append") \
    .save()

cancellation_diverted_year_month_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_year_month") \
    .mode("append") \
    .save()

cancellation_diverted_dayofweek_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_dayofweek") \
    .mode("append") \
    .save()

"""
========================================================================================
"""

cancellation_diverted_total_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_total_src_dest") \
    .mode("append") \
    .save()

cancellation_diverted_year_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_year_src_dest") \
    .mode("append") \
    .save()

cancellation_diverted_year_month_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_year_month_src_dest") \
    .mode("append") \
    .save()

cancellation_diverted_dayofweek_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "cancellation_diverted_dayofweek_src_dest") \
    .mode("append") \
    .save()

"""
========================================================================================
"""

dist_total_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "dist_total") \
    .mode("append") \
    .save()

dist_year_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "dist_year") \
    .mode("append") \
    .save()

dist_year_month_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "dist_year_month") \
    .mode("append") \
    .save()

dist_dayofweek_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "dist_dayofweek") \
    .mode("append") \
    .save()

"""
========================================================================================
"""

max_consec_delay_year_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "max_consec_delay_year") \
    .mode("append") \
    .save()

max_consec_delay_year_src_dest_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "max_consec_delay_year_src_dest") \
    .mode("append") \
    .save()

src_dest_canc_code_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "batchkeyspace") \
    .option("table", "src_dest_canc_code") \
    .mode("append") \
    .save()

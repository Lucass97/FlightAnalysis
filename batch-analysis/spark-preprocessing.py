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

df_2009 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2009.csv")
df_2010 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2010.csv")
df_2011 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2011.csv")
df_2012 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2012.csv")
df_2013 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2013.csv")
df_2014 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2014.csv")
df_2015 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2015.csv")
df_2016 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2016.csv")
df_2017 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2017.csv")
df_2018 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2018.csv")
df_2019 = spark.read.option("header", True).csv("hdfs://localhost:9000/input/2019.csv")


df_2009 = df_2009.drop("Unnamed: 27").cache()
df_2010 = df_2010.drop("Unnamed: 27").cache()
df_2011 = df_2011.drop("Unnamed: 27").cache()
df_2012 = df_2012.drop("Unnamed: 27").cache()
df_2013 = df_2013.drop("Unnamed: 27").cache()
df_2014 = df_2014.drop("Unnamed: 27").cache()
df_2015 = df_2015.drop("Unnamed: 27").cache()
df_2016 = df_2016.drop("Unnamed: 27").cache()
df_2017 = df_2017.drop("Unnamed: 27").cache()
df_2018 = df_2018.drop("Unnamed: 27").cache()

df_2019 = df_2019.withColumnRenamed("OP_UNIQUE_CARRIER", "OP_CARRIER") \
    .withColumn("ACTUAL_ELAPSED_TIME", col("AIR_TIME") + col("TAXI_IN") + col("TAXI_OUT")) \
    .withColumn("CRS_DEP_TIME", col("DEP_TIME") - col("DEP_DELAY")) \
    .withColumn("CRS_ARR_TIME", col("ARR_TIME") - col("ARR_DELAY")) \
    .withColumn("CRS_ELAPSED_TIME", col("ACTUAL_ELAPSED_TIME") - (col("DEP_DELAY") + col("ARR_DELAY"))) \
    .withColumn("CANCELLED", lit(None)) \
    .withColumn("CANCELLATION_CODE", lit(None)) \
    .withColumn("DIVERTED", lit(None))

df_2019 = df_2019.drop(col("_c20")).cache()


print("2009: %d" % len(df_2009.columns))
print("2010: %d" % len(df_2010.columns))
print("2011: %d" % len(df_2011.columns))
print("2012: %d" % len(df_2012.columns))
print("2013: %d" % len(df_2013.columns))
print("2014: %d" % len(df_2014.columns))
print("2015: %d" % len(df_2015.columns))
print("2016: %d" % len(df_2016.columns))
print("2017: %d" % len(df_2017.columns))
print("2018: %d" % len(df_2018.columns))
print("2019: %d" % len(df_2019.columns))


df = df_2009.union(df_2010) \
    .unionByName(df_2011, allowMissingColumns=False) \
    .unionByName(df_2012, allowMissingColumns=False) \
    .unionByName(df_2013, allowMissingColumns=False) \
    .unionByName(df_2014, allowMissingColumns=False) \
    .unionByName(df_2015, allowMissingColumns=False) \
    .unionByName(df_2016, allowMissingColumns=False) \
    .unionByName(df_2017, allowMissingColumns=False) \
    .unionByName(df_2018, allowMissingColumns=False) \
    .unionByName(df_2019, allowMissingColumns=False)

df = df.dropna(how="all").dropDuplicates()
df.coalesce(1).write.mode('overwrite').option('header', 'true').csv('hdfs://localhost:9000/input/flight_data.csv')



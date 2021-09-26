#!/bin/bash
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.github.jnr:jnr-posix:3.1.7 --master local[12] spark-streaming.py

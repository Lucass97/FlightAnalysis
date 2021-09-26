#!/bin/bash
$SPARK_HOME/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,com.github.jnr:jnr-posix:3.1.7 --master local[12] --driver-memory 6g spark-ml.py

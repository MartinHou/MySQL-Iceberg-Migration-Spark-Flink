#!/bin/bash

export SPARK_HOME=/path/to/your/spark/home

# 获取当前日期和时间
current_time=$(date +"%Y-%m-%d %H:00:00")

# 获取下一个小时的日期和时间
next_hour=$(date -d "$current_time + 1 hour" +"%Y-%m-%d %H:00:00")

$SPARK_HOME/bin/spark-submit --master spark://<master-url>:7077 /path/to/your/my_spark_job.py $current_time $next_hour

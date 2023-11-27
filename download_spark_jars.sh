#!/bin/bash
  
set -e

target=$1

jars=(
cloudfs-hadoop-with-dependencies-1.1.26.jar
hive-standalone-metastore-3.1.3-5.jar
hive-common-2.3.9.jar
hive-serde-2.3.9.jar
libthrift-0.12.0.jar
iceberg-spark-runtime-3.3_2.12-1.1.0-CLOUD-SNAPSHOT.jar
)

for jar in ${jars[@]}; do
  echo "Download $jar"
  curl -L "https://artifactory.momenta.works/artifactory/generic-momenta/ddinfra/datalake/jars/$jar" -o $1/$jar
done

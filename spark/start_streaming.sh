#!/bin/bash

echo "🚀 Démarrage du traitement Spark Streaming..."

# Attendre que les services soient prêts
echo "⏳ Attente des services Kafka et Cassandra..."
sleep 30

# Lancer le job Spark
/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.driver.memory=1g" \
    --conf "spark.cassandra.connection.host=cassandra" \
    --conf "spark.cassandra.connection.port=9042" \
    /opt/spark-apps/spark_streaming_to_cassandra.py

echo "✅ Job Spark terminé"
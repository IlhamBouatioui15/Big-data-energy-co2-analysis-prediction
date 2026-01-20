#!/bin/bash

echo "🚀 Démarrage du traitement Spark Streaming..."

# Créer le répertoire pour les JARs
mkdir -p /opt/spark-apps/jars

# Télécharger uniquement les dépendances Kafka
echo "📦 Téléchargement des dépendances Kafka..."

wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P /opt/spark-apps/jars/
wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar -P /opt/spark-apps/jars/
wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar -P /opt/spark-apps/jars/
wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/spark-apps/jars/

echo "✅ Dépendances téléchargées"

# Attendre que les services soient prêts
echo "⏳ Attente des services Kafka et Cassandra..."
sleep 30

# Liste tous les JARs téléchargés
JARS=$(ls /opt/spark-apps/jars/*.jar | tr '\n' ',')
JARS=${JARS%,}  # Retirer la dernière virgule

# Lancer le job Spark
echo "🚀 Lancement du job Spark..."
/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars "$JARS" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.executor.memory=2g" \
    --conf "spark.driver.memory=1g" \
    /opt/spark-apps/spark_streaming_to_cassandra_v2.py

echo "✅ Job Spark terminé"
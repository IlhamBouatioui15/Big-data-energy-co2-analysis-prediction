#!/bin/bash

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Setup Pipeline Energie - Spark + Cassandra  ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════╝${NC}"
echo ""

# ====================================
# 1. Démarrer les conteneurs
# ====================================
echo -e "${YELLOW}[1/5] Démarrage des conteneurs Docker...${NC}"
docker-compose up -d

# ====================================
# 2. Attendre que Cassandra soit prêt
# ====================================
echo -e "${YELLOW}[2/5] Attente de Cassandra (cela peut prendre 1-2 minutes)...${NC}"
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if docker exec cassandra cqlsh -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; then
        echo -e "${GREEN} Cassandra est prêt !${NC}"
        break
    fi
    attempt=$((attempt + 1))
    echo -n "."
    sleep 5
done

if [ $attempt -eq $max_attempts ]; then
    echo -e "${RED} Cassandra n'a pas démarré à temps${NC}"
    exit 1
fi

# ====================================
# 3. Initialiser Cassandra
# ====================================
echo -e "${YELLOW}[3/5] Initialisation du keyspace et des tables Cassandra...${NC}"
docker exec -i cassandra cqlsh < init_cassandra.cql

if [ $? -eq 0 ]; then
    echo -e "${GREEN} Cassandra initialisé avec succès${NC}"
else
    echo -e "${RED} Erreur lors de l'initialisation de Cassandra${NC}"
    exit 1
fi

# ====================================
# 4. Vérifier les tables créées
# ====================================
echo -e "${YELLOW}[4/5] Vérification des tables...${NC}"
docker exec cassandra cqlsh -e "USE energie; DESCRIBE TABLES;"

# ====================================
# 5. Installer les dépendances Python
# ====================================
echo -e "${YELLOW}[5/5] Installation des dépendances Python...${NC}"

# Créer requirements.txt s'il n'existe pas
cat > requirements.txt << EOF
kafka-python==2.0.2
requests==2.31.0
python-dotenv==1.0.0
pyspark==3.5.0
EOF

echo -e "${GREEN} Fichier requirements.txt créé${NC}"

# ====================================
# Afficher les informations de connexion
# ====================================
echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║            Setup terminé avec succès !         ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE} Accès aux services :${NC}"
echo "   • Kafka UI:         http://localhost:9092"
echo "   • MinIO Console:    http://localhost:9001 (admin/admin123)"
echo "   • Grafana:          http://localhost:3000 (admin/admin123)"
echo "   • Spark UI:         http://localhost:8080"
echo "   • Airflow:          http://localhost:8081 (admin/admin)"
echo "   • Kafka Connect:    http://localhost:8083"
echo ""
echo -e "${BLUE}  Cassandra:${NC}"
echo "   • Port CQL:         9042"
echo "   • Keyspace:         energie"
echo "   • Tables:           mix_energie, carbone_energie"
echo ""
echo -e "${BLUE} Pour lancer le pipeline complet :${NC}"
echo "   1. Terminal 1 - Producer:"
echo "      cd producer && python producer.py"
echo ""
echo "   2. Terminal 2 - Spark Streaming:"
echo "      docker exec -it spark-master /opt/spark/bin/spark-submit \\"
echo "        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \\"
echo "        /opt/spark-apps/spark_streaming_to_cassandra.py"
echo ""
echo -e "${BLUE} Pour vérifier les données dans Cassandra :${NC}"
echo "   docker exec -it cassandra cqlsh"
echo "   USE energie;"
echo "   SELECT * FROM mix_energie LIMIT 10;"
echo "   SELECT * FROM carbone_energie LIMIT 10;"
echo ""
echo -e "${BLUE} Pour voir les statistiques en temps réel :${NC}"
echo "   docker exec -it cassandra cqlsh -e \"USE energie; SELECT region, COUNT(*) as count FROM mix_energie GROUP BY region;\""
echo ""
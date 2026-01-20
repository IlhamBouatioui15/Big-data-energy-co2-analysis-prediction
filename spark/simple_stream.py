from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

print("=== DEBUT SPARK STREAMING SANS HIVE ===")

# Créer la session Spark SANS HIVE
spark = SparkSession.builder \
    .appName("EnergyStreamNoHive") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

print(" Session Spark créée (sans Hive)")

# SCHÉMA pour les données de l'API
API_SCHEMA = StructType([
    StructField("zone", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("powerProductionBreakdown", StructType([
        StructField("nuclear", DoubleType(), True),
        StructField("coal", DoubleType(), True),
        StructField("gas", DoubleType(), True),
        StructField("wind", DoubleType(), True),
        StructField("solar", DoubleType(), True),
        StructField("hydro", DoubleType(), True)
    ]), True)
])

print(" Lecture depuis Kafka...")

# Lecture depuis Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "mix_energie") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print(" Transformation des données...")

# Transformation avec le bon schéma + gestion des nulls
processed_df = df \
    .select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), API_SCHEMA).alias("data"),
        col("timestamp")
    ) \
    .filter(col("data").isNotNull()) \
    .select(
        coalesce(col("data.zone"), lit("Inconnu")).alias("region"),
        to_timestamp(col("data.datetime")).alias("date_heure"),
        coalesce(col("data.powerProductionBreakdown.nuclear"), lit(0.0)).alias("nucleaire"),
        coalesce(col("data.powerProductionBreakdown.coal"), lit(0.0)).alias("charbon"),
        coalesce(col("data.powerProductionBreakdown.gas"), lit(0.0)).alias("gaz"),
        coalesce(col("data.powerProductionBreakdown.wind"), lit(0.0)).alias("eolien"),
        coalesce(col("data.powerProductionBreakdown.solar"), lit(0.0)).alias("solaire"),
        coalesce(col("data.powerProductionBreakdown.hydro"), lit(0.0)).alias("hydraulique")
    ) \
    .withColumn("processing_time", current_timestamp()) \
    .filter(col("date_heure").isNotNull())

# FONCTION POUR TRAITER CHAQUE BATCH
def process_batch(batch_df, batch_id):
    """Traite chaque batch et affiche les données"""
    try:
        if not batch_df.isEmpty():
            count = batch_df.count()
            print(f" Batch {batch_id}: {count} lignes traitées!")
            
            # Afficher les données
            print(" Données reçues :")
            batch_df.select("region", "date_heure", "nucleaire", "eolien", "solaire", "processing_time").show(truncate=False)
            
            # Ici vous pouvez ajouter d'autres destinations :
            # - Sauvegarder dans ClickHouse
            # - Sauvegarder dans Postgres
            # - Écrire dans MinIO
            # - etc.
            
    except Exception as e:
        print(f" Erreur dans le batch {batch_id}: {e}")

print(" Démarrage du streaming...")

# Écriture avec traitement de batch
query = processed_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-streaming-checkpoint") \
    .trigger(processingTime="30 seconds") \
    .start()

print(" En attente de données Kafka...")
print(" Les données seront affichées dans la console")

# Fonction pour vérifier l'état périodiquement
def check_streaming_status():
    """Vérifie l'état du streaming périodiquement"""
    try:
        if query.isActive:
            print(" Streaming actif - En attente de données...")
        else:
            print(" Streaming non actif")
    except Exception as e:
        print(f" Erreur vérification statut: {e}")

# Boucle principale avec monitoring
try:
    while query.isActive:
        query.awaitTermination(60)  # Attendre 60 secondes
        check_streaming_status()
        
except KeyboardInterrupt:
    print(" Arrêt demandé par l'utilisateur...")
    query.stop()

print(" Arrêt du programme Spark Streaming")
spark.stop()
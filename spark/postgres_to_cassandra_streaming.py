import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "energie_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

print("=" * 70)
print(" POSTGRESQL → SPARK STREAMING → CASSANDRA")
print("=" * 70)

# Créer la session Spark
spark = SparkSession.builder \
    .appName("PostgresToCassandraStreaming") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print(f" Spark Session créée")
print(f" PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
print(f"  Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")

# URL de connexion PostgreSQL
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
connection_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ====== FONCTION D'ÉCRITURE DANS CASSANDRA ======
def write_to_cassandra(df, keyspace, table):
    """Écrit un DataFrame dans Cassandra"""
    try:
        if df.count() > 0:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=table, keyspace=keyspace) \
                .save()
            print(f" {df.count()} lignes écrites → cassandra.{keyspace}.{table}")
            return True
        else:
            print(f"  Aucune nouvelle donnée")
            return False
    except Exception as e:
        print(f" Erreur écriture Cassandra: {e}")
        import traceback
        traceback.print_exc()
        return False

# ====== FONCTION DE LECTURE INCRÉMENTALE DEPUIS POSTGRESQL ======
def get_last_processing_time_from_cassandra(table_name):
    """Récupère le dernier processing_time traité depuis Cassandra via ingestion_timestamp"""
    try:
        last_record = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=table_name, keyspace="energie") \
            .load() \
            .agg(max("ingestion_timestamp").alias("max_ingestion_timestamp")) \
            .collect()[0]["max_ingestion_timestamp"]
        
        if last_record:
            print(f" Dernière ingestion en Cassandra ({table_name}): {last_record}")
            return last_record
        else:
            print(f" Aucune donnée en Cassandra ({table_name}), chargement complet")
            return None
    except Exception as e:
        print(f"  Impossible de lire Cassandra ({table_name}): {e}")
        return None

def read_incremental_from_postgres(table_name, last_processing_time):
    """Lit les nouvelles données depuis PostgreSQL"""
    try:
        if last_processing_time:
            # Lecture incrémentale basée sur processing_time
            query = f"(SELECT * FROM {table_name} WHERE processing_time > '{last_processing_time}' ORDER BY processing_time) AS incremental"
        else:
            # PREMIÈRE LECTURE: Charger toutes les données
            query = f"(SELECT * FROM {table_name} ORDER BY processing_time) AS full_load"
        
        df = spark.read.jdbc(
            url=jdbc_url,
            table=query,
            properties=connection_properties
        )
        
        count = df.count()
        if count > 0:
            print(f" {count} nouvelles lignes lues depuis postgres.{table_name}")
            # Afficher un échantillon
            df.select("zone", "datetime", "processing_time").show(5, truncate=False)
        else:
            print(f"  Aucune nouvelle ligne dans postgres.{table_name}")
        
        return df
    except Exception as e:
        print(f" Erreur lecture PostgreSQL ({table_name}): {e}")
        import traceback
        traceback.print_exc()
        return None

# ====== TRAITEMENT MIX ÉNERGÉTIQUE ======
def process_mix_energie():
    """Traite et transfère mix_energie de PostgreSQL vers Cassandra"""
    print("\n" + "=" * 70)
    print(" Traitement: mix_energie_table (PostgreSQL → Cassandra)")
    print("=" * 70)
    
    # Récupérer le dernier processing_time traité
    last_processing_time = get_last_processing_time_from_cassandra("mix_energie")
    
    # Lire depuis PostgreSQL
    df_postgres = read_incremental_from_postgres("mix_energie_table", last_processing_time)
    
    if df_postgres and df_postgres.count() > 0:
        # Transformer pour correspondre au schéma Cassandra
        df_cassandra = df_postgres.select(
            col("zone").alias("region"),
            col("datetime").alias("timestamp"),
            coalesce(col("nuclear"), lit(0.0)).alias("nuclear"),
            coalesce(col("wind"), lit(0.0)).alias("wind"),
            coalesce(col("solar"), lit(0.0)).alias("solar"),
            coalesce(col("hydro"), lit(0.0)).alias("hydro"),
            coalesce(col("gas"), lit(0.0)).alias("gas"),
            coalesce(col("coal"), lit(0.0)).alias("coal"),
            coalesce(col("biomass"), lit(0.0)).alias("biomass"),
            lit(0.0).alias("geothermal"),  # Pas dans PostgreSQL
            lit(0.0).alias("oil"),  # Pas dans PostgreSQL
            coalesce(col("consumption_total"), lit(0.0)).alias("total_consumption"),
            coalesce(col("production_total"), lit(0.0)).alias("total_production"),
            lit(0.0).alias("total_import"),  # Pas dans PostgreSQL
            lit(0.0).alias("total_export"),  # Pas dans PostgreSQL
            coalesce(col("fossil_free_percentage"), lit(0.0)).alias("fossil_free_percentage"),
            coalesce(col("renewable_percentage"), lit(0.0)).alias("renewable_percentage"),
            col("processing_time").cast("string").alias("ingestion_timestamp"),
            lit("postgres-sync").alias("ingestion_id")
        )
        
        # Afficher un échantillon avant écriture
        print(" Aperçu des données à écrire:")
        df_cassandra.select("region", "timestamp", "nuclear", "wind", "solar", "ingestion_timestamp").show(3, truncate=False)
        
        # Écrire dans Cassandra
        return write_to_cassandra(df_cassandra, "energie", "mix_energie")
    else:
        print("  Aucune nouvelle donnée à transférer")
        return False

# ====== TRAITEMENT INTENSITÉ CARBONE ======
def process_carbone_energie():
    """Traite et transfère carbone_energie de PostgreSQL vers Cassandra"""
    print("\n" + "=" * 70)
    print(" Traitement: carbone_energie_table (PostgreSQL → Cassandra)")
    print("=" * 70)
    
    # Récupérer le dernier processing_time traité
    last_processing_time = get_last_processing_time_from_cassandra("carbone_energie")
    
    # Lire depuis PostgreSQL
    df_postgres = read_incremental_from_postgres("carbone_energie_table", last_processing_time)
    
    if df_postgres and df_postgres.count() > 0:
        # Transformer pour correspondre au schéma Cassandra
        df_cassandra = df_postgres.select(
            col("zone").alias("region"),
            col("datetime").alias("timestamp"),
            coalesce(col("carbon_intensity"), lit(0.0)).alias("carbon_intensity"),
            coalesce(col("emission_factor_type"), lit("unknown")).alias("emission_factor_type"),
            lit(False).alias("is_estimated"),  # Pas dans PostgreSQL
            lit("real_time").alias("estimation_method"),
            col("processing_time").cast("string").alias("ingestion_timestamp"),
            lit("postgres-sync").alias("ingestion_id")
        )
        
        # Afficher un échantillon avant écriture
        print(" Aperçu des données à écrire:")
        df_cassandra.select("region", "timestamp", "carbon_intensity", "ingestion_timestamp").show(3, truncate=False)
        
        # Écrire dans Cassandra
        return write_to_cassandra(df_cassandra, "energie", "carbone_energie")
    else:
        print("  Aucune nouvelle donnée à transférer")
        return False

# ====== TEST DE CONNEXION INITIAL ======
def test_connections():
    """Teste les connexions PostgreSQL et Cassandra"""
    print("\n" + "=" * 70)
    print("🔌 TEST DES CONNEXIONS")
    print("=" * 70)
    
    # Test PostgreSQL
    try:
        df_test = spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT COUNT(*) as count FROM mix_energie_table) AS test",
            properties=connection_properties
        )
        count = df_test.collect()[0]["count"]
        print(f" PostgreSQL: {count} enregistrements dans mix_energie_table")
    except Exception as e:
        print(f" PostgreSQL: Erreur de connexion - {e}")
        return False
    
    # Test Cassandra
    try:
        df_cassandra = spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="mix_energie", keyspace="energie") \
            .load()
        count_cassandra = df_cassandra.count()
        print(f" Cassandra: {count_cassandra} enregistrements dans mix_energie")
    except Exception as e:
        print(f" Cassandra: Erreur de connexion - {e}")
        return False
    
    print("Toutes les connexions sont OK")
    return True

# ====== BOUCLE PRINCIPALE (STREAMING) ======
print("\n" + "=" * 70)
print(" DÉMARRAGE DU STREAMING POSTGRESQL → CASSANDRA")
print("=" * 70)

# Test initial
if not test_connections():
    print("\n Échec du test de connexion. Arrêt du programme.")
    spark.stop()
    exit(1)

print("\ Intervalle de synchronisation: 30 secondes")
print(" Tables: mix_energie_table, carbone_energie_table")
print("\n En attente de données (Ctrl+C pour arrêter)...\n")

iteration = 0
try:
    while True:
        iteration += 1
        print(f"\n{'=' * 70}")
        print(f" ITÉRATION #{iteration} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
        # Traiter mix_energie
        processed_mix = process_mix_energie()
        
        # Traiter carbone_energie
        processed_carbone = process_carbone_energie()
        
        # Afficher le résumé
        if processed_mix or processed_carbone:
            print(f"\n Itération #{iteration} terminée avec succès")
        else:
            print(f"\n Itération #{iteration} terminée (aucune nouvelle donnée)")
        
        # Attendre 30 secondes
        print(f"\n Prochaine synchronisation dans 30 secondes...")
        time.sleep(30)

except KeyboardInterrupt:
    print("\n\n" + "=" * 70)
    print(" ARRÊT DEMANDÉ PAR L'UTILISATEUR")
    print("=" * 70)
    print(f" Total d'itérations: {iteration}")
    print(" Arrêt propre du streaming")
    spark.stop()

except Exception as e:
    print(f"\n ERREUR CRITIQUE: {e}")
    import traceback
    traceback.print_exc()
    spark.stop()
    raise
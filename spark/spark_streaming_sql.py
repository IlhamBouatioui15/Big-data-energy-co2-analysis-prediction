from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CRÉER SESSION SPARK
# ============================================
logger.info(" Création de la session Spark...")

spark = SparkSession.builder \
    .appName("Energy-Kafka-Spark-PostgreSQL") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cores.max", "2") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
logger.info(" Session Spark créée et connectée au cluster")

# ============================================
# FONCTION: ÉCRIRE DANS POSTGRESQL
# ============================================
def write_to_postgres(df, table_name):
    """Écrit un DataFrame dans PostgreSQL"""
    try:
        if df.isEmpty():
            logger.warning(f" DataFrame vide pour {table_name}")
            return
            
        postgres_url = "jdbc:postgresql://postgres:5432/energie_db"
        properties = {
            "user": "admin",
            "password": "admin123",
            "driver": "org.postgresql.Driver"
        }
        
        # Compter avant écriture
        count = df.count()
        
        df.write.mode("append").jdbc(
            url=postgres_url,
            table=table_name,
            properties=properties
        )
        
        logger.info(f" PostgreSQL: {count} lignes écrites dans {table_name}")
        
    except Exception as e:
        logger.error(f" Erreur PostgreSQL {table_name}: {e}")
        import traceback
        traceback.print_exc()

# ============================================
# TRAITEMENT: MIX ÉNERGÉTIQUE
# ============================================
def process_mix_energie(batch_df, batch_id):
    """Traite le batch mix_energie et écrit dans PostgreSQL"""
    try:
        if batch_df.isEmpty():
            logger.info(f"⏭ MIX batch {batch_id}: vide, skip")
            return
        
        logger.info(f" Traitement MIX batch {batch_id}...")
        
        # Parse JSON et extraction des champs
        processed_df = batch_df \
            .select(
                col("value").cast("string").alias("json_data"),
                col("timestamp")
            ) \
            .withColumn("zone", get_json_object(col("json_data"), "$.zone")) \
            .withColumn("datetime", get_json_object(col("json_data"), "$.datetime")) \
            .withColumn("production_total", 
                       get_json_object(col("json_data"), "$.powerProductionTotal").cast("double")) \
            .withColumn("consumption_total",
                       get_json_object(col("json_data"), "$.powerConsumptionTotal").cast("double")) \
            .withColumn("nuclear",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.nuclear").cast("double"), lit(0.0))) \
            .withColumn("wind",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.wind").cast("double"), lit(0.0))) \
            .withColumn("solar",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.solar").cast("double"), lit(0.0))) \
            .withColumn("hydro",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.hydro").cast("double"), lit(0.0))) \
            .withColumn("gas",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.gas").cast("double"), lit(0.0))) \
            .withColumn("coal",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.coal").cast("double"), lit(0.0))) \
            .withColumn("biomass",
                       coalesce(get_json_object(col("json_data"), "$.powerProductionBreakdown.biomass").cast("double"), lit(0.0))) \
            .withColumn("renewable_percentage",
                       coalesce(get_json_object(col("json_data"), "$.renewablePercentage").cast("double"), lit(0.0))) \
            .withColumn("fossil_free_percentage",
                       coalesce(get_json_object(col("json_data"), "$.fossilFreePercentage").cast("double"), lit(0.0))) \
            .withColumn("processing_time", current_timestamp()) \
            .filter(col("zone").isNotNull() & col("production_total").isNotNull()) \
            .select(
                col("zone"),
                to_timestamp(col("datetime")).alias("datetime"),
                col("production_total"),
                col("consumption_total"),
                col("nuclear"),
                col("wind"),
                col("solar"),
                col("hydro"),
                col("gas"),
                col("coal"),
                col("biomass"),
                col("renewable_percentage"),
                col("fossil_free_percentage"),
                col("processing_time")
            )
        
        if not processed_df.isEmpty():
            # Afficher échantillon
            count = processed_df.count()
            logger.info(f" MIX batch {batch_id}: {count} lignes transformées")
            
            print("\n ÉCHANTILLON MIX ÉNERGÉTIQUE:")
            processed_df.select("zone", "datetime", "production_total", "nuclear", "wind", "solar").show(3, truncate=False)
            
            # Écrire dans PostgreSQL
            write_to_postgres(processed_df, "mix_energie_table")
        else:
            logger.warning(f" MIX batch {batch_id}: Aucune donnée après filtrage")
        
    except Exception as e:
        logger.error(f" Erreur MIX batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# ============================================
# TRAITEMENT: INTENSITÉ CARBONE
# ============================================
def process_carbone_energie(batch_df, batch_id):
    """Traite le batch carbone_energie et écrit dans PostgreSQL"""
    try:
        if batch_df.isEmpty():
            logger.info(f" CARBONE batch {batch_id}: vide, skip")
            return
        
        logger.info(f" Traitement CARBONE batch {batch_id}...")
        
        # Parse JSON et extraction des champs
        processed_df = batch_df \
            .select(
                col("value").cast("string").alias("json_data"),
                col("timestamp")
            ) \
            .withColumn("zone", get_json_object(col("json_data"), "$.zone")) \
            .withColumn("datetime", get_json_object(col("json_data"), "$.datetime")) \
            .withColumn("carbon_intensity",
                       get_json_object(col("json_data"), "$.carbonIntensity").cast("double")) \
            .withColumn("emission_factor_type",
                       get_json_object(col("json_data"), "$.emissionFactorType")) \
            .withColumn("processing_time", current_timestamp()) \
            .filter(col("zone").isNotNull() & col("carbon_intensity").isNotNull()) \
            .select(
                col("zone"),
                to_timestamp(col("datetime")).alias("datetime"),
                col("carbon_intensity"),
                col("emission_factor_type"),
                col("processing_time")
            )
        
        if not processed_df.isEmpty():
            # Afficher échantillon
            count = processed_df.count()
            logger.info(f" CARBONE batch {batch_id}: {count} lignes transformées")
            
            print("\n ÉCHANTILLON INTENSITÉ CARBONE:")
            processed_df.show(3, truncate=False)
            
            # Écrire dans PostgreSQL
            write_to_postgres(processed_df, "carbone_energie_table")
        else:
            logger.warning(f" CARBONE batch {batch_id}: Aucune donnée après filtrage")
        
    except Exception as e:
        logger.error(f" Erreur CARBONE batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()

# ============================================
# MAIN: LANCER LES STREAMS
# ============================================
def main():
    try:
        logger.info("=" * 60)
        logger.info(" DÉMARRAGE DU PIPELINE SPARK STREAMING → POSTGRESQL")
        logger.info("=" * 60)
        logger.info("Source: Kafka (mix_energie, carbone_energie)")
        logger.info(" Destination: PostgreSQL")
        logger.info(" Traitement: Spark Streaming + Spark SQL")
        logger.info("=" * 60)
        
        # ===== STREAM 1: MIX ÉNERGÉTIQUE =====
        logger.info("\n Configuration stream MIX ÉNERGÉTIQUE...")
        mix_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "mix_energie") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        query_mix = mix_stream.writeStream \
            .outputMode("append") \
            .foreachBatch(process_mix_energie) \
            .option("checkpointLocation", "/tmp/checkpoint_mix") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        logger.info(" Stream MIX démarré (trigger: 30 sec)")
        
        # ===== STREAM 2: INTENSITÉ CARBONE =====
        logger.info(" Configuration stream INTENSITÉ CARBONE...")
        carbone_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "carbone_energie") \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        query_carbone = carbone_stream.writeStream \
            .outputMode("append") \
            .foreachBatch(process_carbone_energie) \
            .option("checkpointLocation", "/tmp/checkpoint_carbone") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        logger.info(" Stream CARBONE démarré (trigger: 30 sec)")
        
        # ===== ATTENDRE LES STREAMS =====
        logger.info("\n" + "=" * 60)
        logger.info(" PIPELINE ACTIF - En attente de données Kafka...")
        logger.info("=" * 60)
        
        query_mix.awaitTermination()
        query_carbone.awaitTermination()
        
    except Exception as e:
        logger.error(f" Erreur pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        logger.info(" Arrêt de Spark")
        spark.stop()

if __name__ == "__main__":
    main()
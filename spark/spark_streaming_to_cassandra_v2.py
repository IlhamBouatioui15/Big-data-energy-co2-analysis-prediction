import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Configuration
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
TOPIC_MIX = os.getenv("TOPIC_MIX", "mix_energie")
TOPIC_CARBONE = os.getenv("TOPIC_CARBONE", "carbone_energie")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

# Créer la session Spark
spark = SparkSession.builder \
    .appName("EnergyStreamingToCassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session créée avec succès")
print(f"📡 Kafka Server: {KAFKA_SERVER}")
print(f"🗄️  Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")

# Connexion Cassandra
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
cassandra_session = cluster.connect('energie')
print("✅ Connexion Cassandra établie")

# ====== SCHÉMAS ======
schema_mix = StructType([
    StructField("zone", StringType(), True),
    StructField("datetime", StringType(), True),
    StructField("updatedAt", StringType(), True),
    StructField("powerConsumptionBreakdown", StructType([
        StructField("nuclear", DoubleType(), True),
        StructField("geothermal", DoubleType(), True),
        StructField("biomass", DoubleType(), True),
        StructField("coal", DoubleType(), True),
        StructField("wind", DoubleType(), True),
        StructField("solar", DoubleType(), True),
        StructField("hydro", DoubleType(), True),
        StructField("gas", DoubleType(), True),
        StructField("oil", DoubleType(), True),
        StructField("unknown", DoubleType(), True),
        StructField("hydro discharge", DoubleType(), True),
        StructField("battery discharge", DoubleType(), True)
    ]), True),
    StructField("powerConsumptionTotal", DoubleType(), True),
    StructField("powerProductionTotal", DoubleType(), True),
    StructField("powerImportTotal", DoubleType(), True),
    StructField("powerExportTotal", DoubleType(), True),
    StructField("fossilFreePercentage", DoubleType(), True),
    StructField("renewablePercentage", DoubleType(), True),
    StructField("_metadata", StructType([
        StructField("ingestion_timestamp", StringType(), True),
        StructField("ingestion_id", StringType(), True)
    ]), True)
])

schema_carbone = StructType([
    StructField("zone", StringType(), True),
    StructField("carbonIntensity", DoubleType(), True),
    StructField("datetime", StringType(), True),
    StructField("emissionFactorType", StringType(), True),
    StructField("isEstimated", BooleanType(), True),
    StructField("estimationMethod", StringType(), True),
    StructField("_metadata", StructType([
        StructField("ingestion_timestamp", StringType(), True),
        StructField("ingestion_id", StringType(), True)
    ]), True)
])

# ====== FONCTIONS POUR INSÉRER DANS CASSANDRA ======
def write_mix_to_cassandra(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    # Ajouter un UUID unique pour ingestion_id
    batch_df = batch_df.withColumn("ingestion_id", udf(lambda: str(uuid.uuid4()), StringType())())

    insert_query = """
        INSERT INTO mix_energie (
            region, timestamp, nuclear, wind, solar, hydro, gas, coal,
            biomass, geothermal, oil, total_consumption, total_production,
            total_import, total_export, fossil_free_percentage,
            renewable_percentage, ingestion_timestamp, ingestion_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = cassandra_session.prepare(insert_query)

    count = 0
    for row in batch_df.collect():
        try:
            cassandra_session.execute(prepared, (
                row.region,
                row.timestamp,
                row.nuclear,
                row.wind,
                row.solar,
                row.hydro,
                row.gas,
                row.coal,
                row.biomass,
                row.geothermal,
                row.oil,
                row.total_consumption,
                row.total_production,
                row.total_import,
                row.total_export,
                row.fossil_free_percentage,
                row.renewable_percentage,
                row.ingestion_timestamp,
                row.ingestion_id
            ))
            count += 1
        except Exception as e:
            print(f"❌ Erreur insertion mix: {e}")
    print(f"✅ Batch {batch_id}: {count} lignes insérées dans mix_energie")

def write_carbone_to_cassandra(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    batch_df = batch_df.withColumn("ingestion_id", udf(lambda: str(uuid.uuid4()), StringType())())

    insert_query = """
        INSERT INTO carbone_energie (
            region, timestamp, carbon_intensity, emission_factor_type,
            is_estimated, estimation_method, ingestion_timestamp, ingestion_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = cassandra_session.prepare(insert_query)

    count = 0
    for row in batch_df.collect():
        try:
            cassandra_session.execute(prepared, (
                row.region,
                row.timestamp,
                row.carbon_intensity,
                row.emission_factor_type,
                row.is_estimated,
                row.estimation_method,
                row.ingestion_timestamp,
                row.ingestion_id
            ))
            count += 1
        except Exception as e:
            print(f"❌ Erreur insertion carbone: {e}")
    print(f"✅ Batch {batch_id}: {count} lignes insérées dans carbone_energie")

# ====== LIRE DEPUIS KAFKA ======
df_mix_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC_MIX) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_mix = df_mix_raw.select(
    from_json(col("value").cast("string"), schema_mix).alias("data")
).select("data.*")

df_mix_processed = df_mix.select(
    to_timestamp(col("datetime")).alias("timestamp"),
    col("zone").alias("region"),
    col("powerConsumptionBreakdown.nuclear").alias("nuclear"),
    col("powerConsumptionBreakdown.wind").alias("wind"),
    col("powerConsumptionBreakdown.solar").alias("solar"),
    col("powerConsumptionBreakdown.hydro").alias("hydro"),
    col("powerConsumptionBreakdown.gas").alias("gas"),
    col("powerConsumptionBreakdown.coal").alias("coal"),
    col("powerConsumptionBreakdown.biomass").alias("biomass"),
    col("powerConsumptionBreakdown.geothermal").alias("geothermal"),
    col("powerConsumptionBreakdown.oil").alias("oil"),
    col("powerConsumptionTotal").alias("total_consumption"),
    col("powerProductionTotal").alias("total_production"),
    col("powerImportTotal").alias("total_import"),
    col("powerExportTotal").alias("total_export"),
    col("fossilFreePercentage").alias("fossil_free_percentage"),
    col("renewablePercentage").alias("renewable_percentage"),
    col("_metadata.ingestion_timestamp").alias("ingestion_timestamp")
)

df_carbone_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC_CARBONE) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_carbone = df_carbone_raw.select(
    from_json(col("value").cast("string"), schema_carbone).alias("data")
).select("data.*")

df_carbone_processed = df_carbone.select(
    to_timestamp(col("datetime")).alias("timestamp"),
    col("zone").alias("region"),
    col("carbonIntensity").alias("carbon_intensity"),
    col("emissionFactorType").alias("emission_factor_type"),
    col("isEstimated").alias("is_estimated"),
    col("estimationMethod").alias("estimation_method"),
    col("_metadata.ingestion_timestamp").alias("ingestion_timestamp")
)

# ====== DÉMARRER LES STREAMS ======
query_mix = df_mix_processed.writeStream \
    .foreachBatch(write_mix_to_cassandra) \
    .option("checkpointLocation", "/tmp/checkpoint/mix_energie") \
    .start()

query_carbone = df_carbone_processed.writeStream \
    .foreachBatch(write_carbone_to_cassandra) \
    .option("checkpointLocation", "/tmp/checkpoint/carbone_energie") \
    .start()

print("🚀 Streams Spark démarrés")

try:
    query_mix.awaitTermination()
    query_carbone.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Arrêt des streams...")
    query_mix.stop()
    query_carbone.stop()
    cassandra_session.shutdown()
    cluster.shutdown()
    spark.stop()
    print("✅ Streams arrêtés proprement")

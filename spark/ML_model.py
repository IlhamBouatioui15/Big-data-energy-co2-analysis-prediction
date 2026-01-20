import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))

# Créer la session Spark (sans connecteur Cassandra)
spark = SparkSession.builder \
    .appName("ModelTrainingSession") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Connexion Cassandra
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
cassandra_session = cluster.connect('energie')

print(" Connexion Cassandra établie")

df_mix = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="mix_energie", keyspace="energie") \
    .load()

df_carbone = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="carbone_energie", keyspace="energie") \
    .load()


df = df_mix.join(
    df_carbone,
    on=["region", "timestamp"],
    how="left"
)

df = df.withColumn("hour", hour("timestamp")) \
       .withColumn("day_of_week", dayofweek("timestamp")) \
       .withColumn("month", month("timestamp")) \
       .withColumn("is_weekend", col("day_of_week").isin([1,7]).cast("int"))

#Nettoyage de données
df = df.dropna(subset=["total_consumption"])

#Feature selection
pdf = df.select(
    "region", "hour", "day_of_week", "month", "is_weekend",
    "nuclear", "wind", "solar", "hydro", "gas", "coal",
    "biomass", "geothermal", "oil",
    "total_production",
    "fossil_free_percentage", "renewable_percentage",
    "carbon_intensity",
    "total_consumption"
).toPandas()

#Encodage et split

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBRegressor

le = LabelEncoder()
pdf["region"] = le.fit_transform(pdf["region"])

X = pdf.drop("total_consumption", axis=1)
y = pdf["total_consumption"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False
)


model = XGBRegressor(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    objective="reg:squarederror"
)

model.fit(X_train, y_train)

#Evaluation
from sklearn.metrics import mean_absolute_error, r2_score

y_pred = model.predict(X_test)

print("MAE:", mean_absolute_error(y_test, y_pred))
print("R2:", r2_score(y_test, y_pred))

#feature importances
model.feature_importances_



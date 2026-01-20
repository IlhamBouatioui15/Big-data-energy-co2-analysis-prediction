# Configuration Spark
SPARK_CONFIG = {
    "app_name": "EnergyStreamProcessor",
    "master": "spark://spark-master:7077",
    "config": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.initialPartitionNum": "100",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true"
    }
}

# Configuration Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": "kafka:29092",
    "topics": ["mix_energie", "carbone_energie"],
    "starting_offsets": "latest"
}

# Configuration ClickHouse
CLICKHOUSE_CONFIG = {
    "host": "clickhouse",
    "port": 8123,
    "username": "default",
    "password": "",
    "database": "energie"
}
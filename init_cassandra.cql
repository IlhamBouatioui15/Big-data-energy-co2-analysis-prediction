-- ====================================
-- Script d'initialisation Cassandra
-- ====================================

-- Créer le keyspace
CREATE KEYSPACE IF NOT EXISTS energie 
WITH replication = {
    'class': 'SimpleStrategy', 
    'replication_factor': 1
};

-- Utiliser le keyspace
USE energie;

-- ====================================
-- Table pour le mix énergétique
-- ====================================
CREATE TABLE IF NOT EXISTS mix_energie (
    region text,
    timestamp timestamp,
    nuclear double,
    wind double,
    solar double,
    hydro double,
    gas double,
    coal double,
    biomass double,
    geothermal double,
    oil double,
    total_consumption double,
    total_production double,
    total_import double,
    total_export double,
    fossil_free_percentage double,
    renewable_percentage double,
    ingestion_timestamp text,
    ingestion_id text,
    PRIMARY KEY ((region), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy'}
  AND default_time_to_live = 31536000;

-- Index secondaires pour les requêtes
CREATE INDEX IF NOT EXISTS idx_mix_timestamp ON mix_energie (timestamp);
CREATE INDEX IF NOT EXISTS idx_mix_ingestion ON mix_energie (ingestion_timestamp);

-- ====================================
-- Table pour l'intensité carbone
-- ====================================
CREATE TABLE IF NOT EXISTS carbone_energie (
    region text,
    timestamp timestamp,
    carbon_intensity double,
    emission_factor_type text,
    is_estimated boolean,
    estimation_method text,
    ingestion_timestamp text,
    ingestion_id text,
    PRIMARY KEY ((region), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy'}
  AND default_time_to_live = 31536000;

-- Index secondaires
CREATE INDEX IF NOT EXISTS idx_carbone_timestamp ON carbone_energie (timestamp);
CREATE INDEX IF NOT EXISTS idx_carbone_ingestion ON carbone_energie (ingestion_timestamp);

-- ====================================
-- Afficher les tables créées
-- ====================================
DESCRIBE TABLES;
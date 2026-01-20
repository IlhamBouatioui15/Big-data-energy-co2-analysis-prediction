import os
import pandas as pd
import joblib
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

print(" Système de Prédiction de Consommation Énergétique")
print("=" * 60)

# ====== CONFIGURATION ======
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
MODEL_PATH = "/opt/spark-apps/models"

# ====== CHARGEMENT DU MODÈLE ======
print(" Chargement du modèle...")

# Trouver le modèle le plus récent
import glob
model_files = glob.glob(f"{MODEL_PATH}/xgboost_energy_model_*.pkl")
if not model_files:
    raise FileNotFoundError(" Aucun modèle trouvé. Entraînez d'abord le modèle !")

latest_model = max(model_files, key=os.path.getctime)
model = joblib.load(latest_model)
print(f"    Modèle chargé: {latest_model}")

# Charger le label encoder
le_region = joblib.load(f"{MODEL_PATH}/label_encoder_region.pkl")
print(f"    Label encoder chargé")

# ====== CONNEXION CASSANDRA ======
print(" Connexion à Cassandra...")
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
session = cluster.connect('energie')
print("    Connexion établie")

# ====== FONCTION DE PRÉDICTION ======
def predict_consumption(region, target_datetime=None):
    """
    Prédit la consommation énergétique pour une région et une date données.
    
    Args:
        region: Code de la région (ex: 'FR', 'MA', 'ES')
        target_datetime: datetime object (si None, utilise datetime.now())
    
    Returns:
        dict avec la prédiction et les features utilisées
    """
    if target_datetime is None:
        target_datetime = datetime.now()
    
    print(f"\n Prédiction pour {region} à {target_datetime}")
    
    # Récupérer les dernières données de la région
    query = f"""
        SELECT * FROM mix_energie 
        WHERE region = '{region}' 
        LIMIT 1
    """
    
    rows = list(session.execute(query))
    
    if not rows:
        print(f"    Aucune donnée trouvée pour {region}")
        return None
    
    # Utiliser les dernières valeurs connues
    last_data = rows[0]
    
    # Créer les features
    features = {
        'region_encoded': le_region.transform([region])[0],
        'hour': target_datetime.hour,
        'day_of_week': target_datetime.weekday(),
        'month': target_datetime.month,
        'day': target_datetime.day,
        'is_weekend': int(target_datetime.weekday() in [5, 6]),
        'is_peak_hour': int(target_datetime.hour in [8, 9, 10, 18, 19, 20]),
        'nuclear': last_data.nuclear or 0,
        'wind': last_data.wind or 0,
        'solar': last_data.solar or 0,
        'hydro': last_data.hydro or 0,
        'gas': last_data.gas or 0,
        'coal': last_data.coal or 0,
        'biomass': last_data.biomass or 0,
        'geothermal': last_data.geothermal or 0,
        'oil': last_data.oil or 0,
        'total_production': last_data.total_production or 0,
        'total_import': last_data.total_import or 0,
        'total_export': last_data.total_export or 0,
        'fossil_free_percentage': last_data.fossil_free_percentage or 0,
        'renewable_percentage': last_data.renewable_percentage or 0,
    }
    
    # Récupérer l'intensité carbone
    query_carbone = f"""
        SELECT carbon_intensity FROM carbone_energie 
        WHERE region = '{region}' 
        LIMIT 1
    """
    carbone_rows = list(session.execute(query_carbone))
    features['carbon_intensity'] = carbone_rows[0].carbon_intensity if carbone_rows else 0
    
    # Calculer les ratios
    features['renewable_ratio'] = features['renewable_percentage'] / 100
    features['fossil_ratio'] = (100 - features['fossil_free_percentage']) / 100
    features['production_consumption_ratio'] = features['total_production'] / (last_data.total_consumption + 1)
    
    # Créer DataFrame
    X = pd.DataFrame([features])
    
    # Prédiction
    prediction = model.predict(X)[0]
    
    result = {
        'region': region,
        'datetime': target_datetime,
        'predicted_consumption': round(prediction, 2),
        'features_used': features,
        'model_used': os.path.basename(latest_model)
    }
    
    print(f"    Consommation prédite: {prediction:.2f} MW")
    
    return result

# ====== PRÉDICTIONS MULTIPLES ======
def predict_next_hours(region, n_hours=24):
    """
    Prédit la consommation pour les n prochaines heures.
    
    Args:
        region: Code de la région
        n_hours: Nombre d'heures à prédire
    
    Returns:
        DataFrame avec les prédictions
    """
    print(f"\n Prédiction pour les {n_hours} prochaines heures - {region}")
    
    predictions = []
    current_time = datetime.now()
    
    for i in range(n_hours):
        target_time = current_time + timedelta(hours=i)
        result = predict_consumption(region, target_time)
        
        if result:
            predictions.append({
                'datetime': target_time,
                'predicted_consumption': result['predicted_consumption']
            })
    
    df_predictions = pd.DataFrame(predictions)
    return df_predictions

# ====== COMPARAISON RÉGIONS ======
def compare_regions(regions, target_datetime=None):
    """
    Compare les prédictions de consommation pour plusieurs régions.
    
    Args:
        regions: Liste de codes régions
        target_datetime: datetime object
    
    Returns:
        DataFrame avec les comparaisons
    """
    if target_datetime is None:
        target_datetime = datetime.now()
    
    print(f"\n Comparaison des régions à {target_datetime}")
    
    comparisons = []
    
    for region in regions:
        result = predict_consumption(region, target_datetime)
        if result:
            comparisons.append({
                'region': region,
                'predicted_consumption': result['predicted_consumption']
            })
    
    df_comparison = pd.DataFrame(comparisons)
    df_comparison = df_comparison.sort_values('predicted_consumption', ascending=False)
    
    print("\n Résultats:")
    print(df_comparison.to_string(index=False))
    
    return df_comparison

# ====== EXEMPLES D'UTILISATION ======
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("EXEMPLES DE PRÉDICTIONS")
    print("=" * 60)
    
    # Exemple 1: Prédiction pour maintenant
    print("\n--- Exemple 1: Prédiction immédiate ---")
    result = predict_consumption('MA')
    
    # Exemple 2: Prédiction pour demain à 10h
    print("\n--- Exemple 2: Prédiction pour demain 10h ---")
    tomorrow_10am = datetime.now().replace(hour=10, minute=0) + timedelta(days=1)
    result = predict_consumption('FR', tomorrow_10am)
    
    # Exemple 3: Prédictions pour les 6 prochaines heures
    print("\n--- Exemple 3: Prédictions 6h ---")
    df_forecast = predict_next_hours('MA', n_hours=6)
    print(df_forecast)
    
    # Exemple 4: Comparaison de régions
    print("\n--- Exemple 4: Comparaison de régions ---")
    regions_to_compare = ['MA', 'FR', 'ES', 'DE']
    df_comp = compare_regions(regions_to_compare)
    
    # Fermer la connexion
    session.shutdown()
    cluster.shutdown()
    
    print("\n Prédictions terminées !")
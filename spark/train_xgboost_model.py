import os
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor
import matplotlib.pyplot as plt
import joblib
from datetime import datetime

print(" Démarrage de l'entraînement du modèle XGBoost...")

# ====== CONFIGURATION ======
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
MODEL_PATH = "/opt/spark-apps/models"

# Créer le dossier pour sauvegarder les modèles
os.makedirs(MODEL_PATH, exist_ok=True)

# ====== CONNEXION CASSANDRA ======
print(" Connexion à Cassandra...")
cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
session = cluster.connect('energie')
print(" Connexion établie")

# ====== CHARGEMENT DES DONNÉES ======
print(" Chargement des données depuis Cassandra...")

# Charger mix_energie
query_mix = "SELECT * FROM mix_energie"
rows_mix = session.execute(query_mix)
df_mix = pd.DataFrame(list(rows_mix))
print(f"   Mix énergétique: {len(df_mix)} lignes")

# Charger carbone_energie
query_carbone = "SELECT * FROM carbone_energie"
rows_carbone = session.execute(query_carbone)
df_carbone = pd.DataFrame(list(rows_carbone))
print(f"   Intensité carbone: {len(df_carbone)} lignes")

# Fermer la connexion
session.shutdown()
cluster.shutdown()

# ====== FUSION DES DONNÉES ======
print(" Fusion des datasets...")
df = pd.merge(
    df_mix,
    df_carbone[['region', 'timestamp', 'carbon_intensity', 'is_estimated']],
    on=['region', 'timestamp'],
    how='left'
)
print(f"   Dataset fusionné: {len(df)} lignes")

# ====== FEATURE ENGINEERING ======
print(" Feature engineering...")

# Extraire les features temporelles
df['hour'] = df['timestamp'].dt.hour
df['day_of_week'] = df['timestamp'].dt.dayofweek
df['month'] = df['timestamp'].dt.month
df['day'] = df['timestamp'].dt.day
df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
df['is_peak_hour'] = df['hour'].isin([8, 9, 10, 18, 19, 20]).astype(int)

# Calculer des ratios
df['renewable_ratio'] = df['renewable_percentage'] / 100
df['fossil_ratio'] = (100 - df['fossil_free_percentage']) / 100
df['production_consumption_ratio'] = df['total_production'] / (df['total_consumption'] + 1)

# ====== NETTOYAGE DES DONNÉES ======
print(" Nettoyage des données...")

# Supprimer les lignes avec valeurs manquantes critiques
df = df.dropna(subset=['total_consumption', 'timestamp', 'region'])

# Remplacer les NaN dans les features par 0
numeric_cols = ['nuclear', 'wind', 'solar', 'hydro', 'gas', 'coal', 
                'biomass', 'geothermal', 'oil', 'carbon_intensity']
df[numeric_cols] = df[numeric_cols].fillna(0)

print(f"   Données après nettoyage: {len(df)} lignes")

# ====== ENCODAGE ======
print(" Encodage des variables catégorielles...")

le_region = LabelEncoder()
df['region_encoded'] = le_region.fit_transform(df['region'])

# Sauvegarder le label encoder
joblib.dump(le_region, f"{MODEL_PATH}/label_encoder_region.pkl")
print(f"   Régions uniques: {df['region'].nunique()}")

# ====== SÉLECTION DES FEATURES ======
feature_columns = [
    'region_encoded', 'hour', 'day_of_week', 'month', 'day',
    'is_weekend', 'is_peak_hour',
    'nuclear', 'wind', 'solar', 'hydro', 'gas', 'coal',
    'biomass', 'geothermal', 'oil',
    'total_production', 'total_import', 'total_export',
    'fossil_free_percentage', 'renewable_percentage',
    'carbon_intensity',
    'renewable_ratio', 'fossil_ratio', 'production_consumption_ratio'
]

target_column = 'total_consumption'

# Vérifier que toutes les colonnes existent
available_features = [col for col in feature_columns if col in df.columns]
print(f"   Features disponibles: {len(available_features)}/{len(feature_columns)}")

X = df[available_features]
y = df[target_column]

# ====== SPLIT TRAIN/TEST ======
print("  Split train/test...")

# Trier par timestamp pour split temporel
df = df.sort_values('timestamp')
X = df[available_features]
y = df[target_column]

# Split 80/20
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False  # shuffle=False pour respecter l'ordre temporel
)

print(f"   Train set: {len(X_train)} lignes")
print(f"   Test set: {len(X_test)} lignes")

# ====== ENTRAÎNEMENT DU MODÈLE ======
print(" Entraînement du modèle XGBoost...")

model = XGBRegressor(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    objective='reg:squarederror',
    random_state=42,
    n_jobs=-1
)

model.fit(
    X_train, y_train,
    eval_set=[(X_train, y_train), (X_test, y_test)],
    verbose=False
)

print(" Modèle entraîné !")

# ====== ÉVALUATION ======
print("\n ÉVALUATION DU MODÈLE")
print("=" * 50)

y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

# Métriques Train
mae_train = mean_absolute_error(y_train, y_pred_train)
rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
r2_train = r2_score(y_train, y_pred_train)
mape_train = np.mean(np.abs((y_train - y_pred_train) / y_train)) * 100

print("\n TRAIN SET:")
print(f"   MAE:  {mae_train:.2f} MW")
print(f"   RMSE: {rmse_train:.2f} MW")
print(f"   R²:   {r2_train:.4f}")
print(f"   MAPE: {mape_train:.2f}%")

# Métriques Test
mae_test = mean_absolute_error(y_test, y_pred_test)
rmse_test = np.sqrt(mean_squared_error(y_test, y_pred_test))
r2_test = r2_score(y_test, y_pred_test)
mape_test = np.mean(np.abs((y_test - y_pred_test) / y_test)) * 100

print("\n TEST SET:")
print(f"   MAE:  {mae_test:.2f} MW")
print(f"   RMSE: {rmse_test:.2f} MW")
print(f"   R²:   {r2_test:.4f}")
print(f"   MAPE: {mape_test:.2f}%")

# ====== FEATURE IMPORTANCE ======
print("\n FEATURE IMPORTANCE (Top 10):")
print("=" * 50)

feature_importance = pd.DataFrame({
    'feature': available_features,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

for idx, row in feature_importance.head(10).iterrows():
    print(f"   {row['feature']:30s} {row['importance']:.4f}")

# ====== SAUVEGARDE DU MODÈLE ======
print(f"\n Sauvegarde du modèle...")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
model_filename = f"{MODEL_PATH}/xgboost_energy_model_{timestamp}.pkl"
metadata_filename = f"{MODEL_PATH}/model_metadata_{timestamp}.txt"

# Sauvegarder le modèle
joblib.dump(model, model_filename)
print(f"    Modèle sauvegardé: {model_filename}")

# Sauvegarder les métadonnées
with open(metadata_filename, 'w') as f:
    f.write(f"Model trained on: {datetime.now()}\n")
    f.write(f"Training samples: {len(X_train)}\n")
    f.write(f"Test samples: {len(X_test)}\n")
    f.write(f"Features: {len(available_features)}\n")
    f.write(f"MAE (test): {mae_test:.2f}\n")
    f.write(f"RMSE (test): {rmse_test:.2f}\n")
    f.write(f"R² (test): {r2_test:.4f}\n")
    f.write(f"MAPE (test): {mape_test:.2f}%\n")
    f.write(f"\nFeatures used:\n")
    for feat in available_features:
        f.write(f"  - {feat}\n")

print(f"    Métadonnées sauvegardées: {metadata_filename}")

# Sauvegarder les feature importances
feature_importance.to_csv(f"{MODEL_PATH}/feature_importance_{timestamp}.csv", index=False)

print("\n" + "=" * 50)
print(" ENTRAÎNEMENT TERMINÉ AVEC SUCCÈS !")
print("=" * 50)

# ====== VISUALISATIONS (optionnel) ======
try:
    print("\n Génération des visualisations...")
    
    # 1. Prédictions vs Réel
    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.scatter(y_test, y_pred_test, alpha=0.5)
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
    plt.xlabel('Consommation Réelle (MW)')
    plt.ylabel('Consommation Prédite (MW)')
    plt.title('Prédictions vs Réel (Test Set)')
    plt.grid(True)
    
    # 2. Résidus
    plt.subplot(1, 2, 2)
    residuals = y_test - y_pred_test
    plt.scatter(y_pred_test, residuals, alpha=0.5)
    plt.axhline(y=0, color='r', linestyle='--', lw=2)
    plt.xlabel('Consommation Prédite (MW)')
    plt.ylabel('Résidus (MW)')
    plt.title('Analyse des Résidus')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig(f"{MODEL_PATH}/model_evaluation_{timestamp}.png", dpi=150)
    print(f"    Graphiques sauvegardés: model_evaluation_{timestamp}.png")
    
    # 3. Feature Importance
    plt.figure(figsize=(10, 8))
    feature_importance.head(15).plot(x='feature', y='importance', kind='barh')
    plt.xlabel('Importance')
    plt.title('Top 15 Features Importantes')
    plt.tight_layout()
    plt.savefig(f"{MODEL_PATH}/feature_importance_{timestamp}.png", dpi=150)
    print(f"    Feature importance sauvegardée")
    
except Exception as e:
    print(f"     Erreur lors de la création des graphiques: {e}")

print("\n Fichiers générés:")
print(f"   - {model_filename}")
print(f"   - {metadata_filename}")
print(f"   - {MODEL_PATH}/label_encoder_region.pkl")
print(f"   - {MODEL_PATH}/feature_importance_{timestamp}.csv")
print(f"   - {MODEL_PATH}/model_evaluation_{timestamp}.png")
print(f"   - {MODEL_PATH}/feature_importance_{timestamp}.png")
# Big Data Energy & CO2: Analysis and Prediction

[![Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)](https://jupyter.org/)
[![Machine Learning](https://img.shields.io/badge/Spark_MLlib-blue?style=for-the-badge)](https://spark.apache.org/mllib/)

## Présentation du Projet
Ce projet utilise les technologies du **Big Data** pour analyser l'impact de la consommation mondiale d'énergie et les émissions de CO2. En exploitant la puissance de calcul d'**Apache Spark**, nous traitons des volumes de données importants pour identifier les tendances climatiques et construire des modèles prédictifs robustes.

## Problématique
Dans un contexte de transition énergétique mondiale, l’analyse massive de données joue un rôle critique pour optimiser la production et la consommation d’énergie. Ce projet s'attaque au défi de la réduction des émissions de CO₂ et de l’amélioration de l’efficacité des systèmes énergétiques. Grâce au **Big Data** et à l’**Intelligence Artificielle**, nous cherchons à transformer des données brutes en leviers décisionnels pour soutenir des solutions durables et anticiper les besoins futurs des infrastructures énergétiques.

---

## Objectifs du Projet
*   **Analyse de Performance :** Évaluer l'intensité carbone et l'efficacité énergétique de plus de 200 zones géographiques, avec un focus particulier sur le **Maroc (MA)**.
*   **Identification des Pollueurs :** Détecter les sources de pollution majeures au sein du mix énergétique mondial (charbon, gaz, pétrole).
*   **Optimisation du Mix :** Analyser la corrélation entre la capacité installée (GW) et la production réelle pour identifier les gisements d'optimisation des énergies renouvelables.
*   **Modélisation Prédictive :** Prévoir l'évolution de l'intensité carbone (**gCO2eq/kWh**) et les besoins énergétiques futurs pour guider les politiques de transition.

---

## Sources de Données
Le projet exploite des datasets globaux (type *Electricity Maps* / *Our World in Data*) couvrant :
*   **Couverture Mondiale :** 200+ zones géographiques (incluant le Maroc).
*   **Intensité Carbone :** Émissions de CO2 mesurées en **gCO2eq/kWh**.
*   **Mix Énergétique :** Répartition détaillée par source (Solaire, Éolien, Hydraulique, Nucléaire, Fossile).
*   **Capacité Installée :** Puissance nominale en **GW** pour chaque type de technologie de production.

---

## Architecture du Système
L'architecture repose sur un pipeline de données distribué, garantissant scalabilité et performance.

![Architecture du Projet](https://raw.githubusercontent.com/IlhamBouatioui15/Big-data-energy-co2-analysis-prediction/main/architecture.jpeg)



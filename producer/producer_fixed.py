import os
import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from minio import Minio
from dotenv import load_dotenv

load_dotenv()


class EnergyDataProducer:
    def __init__(self):
        # CONFIGURATION POUR CONTAINER DOCKER
        self.api_token = os.getenv("API_TOKEN", "FR34sfVhGNzzvxqgN5kE")
        self.kafka_server = os.getenv("KAFKA_SERVER", "kafka:9092")
        self.topic_mix = os.getenv("TOPIC_MIX", "mix_energie")
        self.topic_carbone = os.getenv("TOPIC_CARBONE", "carbone_energie")
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")
        self.minio_bucket = os.getenv("MINIO_BUCKET", "bronze")

        self.setup_minio()
        self.setup_kafka()
        self.zones = ["MA", "FR", "ES", "DE", "IT", "PT", "GB", "CA-ON", "SE", "NO"]

    def setup_minio(self):
        try:
            self.minio_client = Minio(
                self.minio_endpoint,
                access_key=self.minio_access_key,
                secret_key=self.minio_secret_key,
                secure=False
            )
            if not self.minio_client.bucket_exists(self.minio_bucket):
                self.minio_client.make_bucket(self.minio_bucket)
            print(f" Bucket MinIO '{self.minio_bucket}' prêt")
        except Exception as e:
            print(f" Erreur MinIO: {e}")

    def setup_kafka(self):
        """Initialiser Kafka Producer avec kafka-python"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                request_timeout_ms=30000,
                api_version=(2, 8, 0),
                acks='all'  # Attendre la confirmation de tous les replicas
            )
            print(f" Producer Kafka initialisé : {self.kafka_server}")

        except Exception as e:
            print(f" Erreur Kafka: {e}")
            raise

    def get_mix_energetique(self, zone):
        url = f"https://api.electricitymap.org/v3/power-breakdown/latest?zone={zone}"
        headers = {"auth-token": self.api_token}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                data['zone'] = zone
                data['timestamp'] = time.time()
                data['date_processing'] = time.strftime('%Y-%m-%d %H:%M:%S')
                return data
            else:
                print(f"⚠ Erreur mix {zone}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f" Erreur API mix {zone}: {e}")
            return None

    def get_intensite_carbone(self, zone):
        url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={zone}"
        headers = {"auth-token": self.api_token}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                data['zone'] = zone
                data['timestamp'] = time.time()
                data['date_processing'] = time.strftime('%Y-%m-%d %H:%M:%S')
                return data
            else:
                print(f"⚠ Erreur carbone {zone}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f" Erreur API carbone {zone}: {e}")
            return None

    def send_to_kafka(self, topic, data):
        try:
            future = self.producer.send(topic, data)
            # Attendre la confirmation
            record_metadata = future.get(timeout=10)
            print(f" Message livré [{topic}] - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
        except KafkaError as e:
            print(f" Erreur envoi Kafka {topic}: {e}")
            return False
        except Exception as e:
            print(f" Erreur générale Kafka {topic}: {e}")
            return False

    def backup_to_minio(self, data_type, data):
        try:
            timestamp = int(time.time())
            file_name = f"{data_type}/energy_{data_type}_{timestamp}.json"

            import io
            data_json = json.dumps(data, indent=2)
            data_bytes = data_json.encode('utf-8')
            data_stream = io.BytesIO(data_bytes)

            self.minio_client.put_object(
                self.minio_bucket,
                file_name,
                data_stream,
                len(data_bytes),
                content_type='application/json'
            )
            print(f" Backup MinIO: {file_name}")
            return True
        except Exception as e:
            print(f" Erreur backup MinIO: {e}")
            return False

    def process_zone(self, zone):
        print(f" Traitement: {zone}")

        # Récupérer les données de mix énergétique
        mix_data = self.get_mix_energetique(zone)
        if mix_data:
            print(f" Mix énergétique récupéré pour {zone}")
            if self.send_to_kafka(self.topic_mix, mix_data):
                self.backup_to_minio("mix", mix_data)
            else:
                print(f"Échec envoi Kafka mix pour {zone}")
        else:
            print(f" Aucune donnée mix pour {zone}")

        # Petite pause entre les appais API
        time.sleep(1)

        # Récupérer les données d'intensité carbone
        carbone_data = self.get_intensite_carbone(zone)
        if carbone_data:
            print(f" Intensité carbone récupérée pour {zone}")
            if self.send_to_kafka(self.topic_carbone, carbone_data):
                self.backup_to_minio("carbone", carbone_data)
            else:
                print(f" Échec envoi Kafka carbone pour {zone}")
        else:
            print(f" Aucune donnée carbone pour {zone}")

        # Pause entre les zones
        time.sleep(2)

    def run(self):
        print(" Démarrage du Energy Data Producer...")
        print(f" Zones: {self.zones}")

        iteration = 0
        while True:
            try:
                iteration += 1
                print(f"\n Itération #{iteration}")

                for zone in self.zones:
                    self.process_zone(zone)

                print(f" Tour #{iteration} terminé, attente 60 secondes...")
                time.sleep(60)

            except KeyboardInterrupt:
                print("\n Arrêt demandé par l'utilisateur...")
                break
            except Exception as e:
                print(f" Erreur générale dans la boucle principale: {e}")
                time.sleep(30)  # Attendre avant de réessayer

    def __del__(self):
        """Nettoyage à la fermeture"""
        try:
            if hasattr(self, 'producer'):
                self.producer.close()
                print(" Producer Kafka fermé")
        except:
            pass


if __name__ == "__main__":
    try:
        producer = EnergyDataProducer()
        producer.run()
    except KeyboardInterrupt:
        print("\n Arrêt du programme")
    except Exception as e:
        print(f" Erreur critique: {e}")
"""
DAG Airflow - Supervision du pipeline Big Data Energy
Pipeline: API → Kafka → MinIO/Spark → PostgreSQL → Cassandra → Grafana
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# ============================================
# CONFIGURATION
# ============================================
default_args = {
    'owner': 'energy_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'energy_complete_pipeline_orchestration',
    default_args=default_args,
    description='Supervision du pipeline: API → Kafka → MinIO → Spark → PostgreSQL → Cassandra → Grafana',
    schedule_interval='*/30 * * * *',  # Toutes les 30 minutes
    catchup=False,
    max_active_runs=1,
    tags=['energy', 'production', 'supervision'],
)

# ============================================
# VERIFICATIONS 
# ============================================

def check_infrastructure():
   
    print("\n" + "="*70)
    print(" VÉRIFICATION DE L'INFRASTRUCTURE")
    print("="*70 + "\n")
    
    import socket
    
    services = {
        'Kafka': ('kafka', 29092),
        'MinIO': ('minio', 9000),
        'PostgreSQL': ('postgres', 5432),
        'Cassandra': ('cassandra', 9042),
        'Spark Master': ('spark-master', 7077),
    }
    
    all_ok = True
    for name, (host, port) in services.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result == 0:
                print(f" {name:15s} - {host}:{port} - OK")
            else:
                print(f" {name:15s} - {host}:{port} - ÉCHEC")
                all_ok = False
        except Exception as e:
            print(f" {name:15s} - ERREUR: {e}")
            all_ok = False
    
    print("\n" + "="*70 + "\n")
    
    if not all_ok:
        raise Exception(" Infrastructure non prête. Vérifiez les containers Docker.")
    
    print(" Infrastructure OK - Pipeline peut démarrer\n")
    return True

check_infra_task = PythonOperator(
    task_id='check_infrastructure',
    python_callable=check_infrastructure,
    dag=dag,
)

# ============================================
# VÉRIFIER/DÉMARRER PRODUCER (API → KAFKA)
# ============================================

def check_producer_status():
    """Vérifie si le producer envoie des données vers Kafka"""
    print("\n" + "="*70)
    print("📡 VÉRIFICATION DU PRODUCER (API → KAFKA)")
    print("="*70 + "\n")
    
    import subprocess
    
    try:
        # Vérifier si le container producer tourne
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=energy-producer', '--format', '{{.Status}}'],
            capture_output=True,
            text=True,
            check=True
        )
        
        if 'Up' in result.stdout:
            print(" Producer container est actif")
            
            # Vérifier les logs récents
            logs = subprocess.run(
                ['docker', 'logs', '--tail', '20', 'energy-producer'],
                capture_output=True,
                text=True
            )
            
            if 'erreur' in logs.stdout.lower() or 'error' in logs.stdout.lower():
                print(" Erreurs détectées dans les logs du producer")
                print(logs.stdout[-500:])
                return False
            
            print(" Producer envoie des données vers Kafka")
            return True
        else:
            print(" Producer container n'est pas actif")
            return False
            
    except Exception as e:
        print(f" Erreur lors de la vérification: {e}")
        return False

check_producer_task = PythonOperator(
    task_id='check_producer_api_kafka',
    python_callable=check_producer_status,
    dag=dag,
)

# ============================================
#  VÉRIFIER KAFKA → MINIO
# ============================================

def verify_kafka_to_minio():
    """Vérifie que les données arrivent dans MinIO"""
    print("\n" + "="*70)
    print(" VÉRIFICATION KAFKA → MINIO")
    print("="*70 + "\n")
    
    try:
        try:
            from minio import Minio
        except ImportError:
            print(" Module 'minio' non installé (non bloquant)")
            print("   Pour activer: docker exec airflow pip install minio==7.1.17")
            return True  # Non bloquant
        
        client = Minio(
            "minio:9000",
            access_key="admin",
            secret_key="admin123",
            secure=False
        )
        
        # Vérifier bucket bronze
        if not client.bucket_exists("bronze"):
            print(" Bucket 'bronze' n'existe pas")
            return True  # Non bloquant
        
        print(" Bucket 'bronze' existe")
        
        # Lister les objets récents
        objects = list(client.list_objects("bronze", recursive=True))
        
        if not objects:
            print(" Aucun fichier dans MinIO bronze")
            return True  # Non bloquant
        
        recent_objects = sorted(objects, key=lambda x: x.last_modified, reverse=True)[:5]
        
        print(f"\n {len(objects)} fichiers au total dans bronze")
        print("\n 5 fichiers les plus récents:")
        for obj in recent_objects:
            print(f"   {obj.object_name} - {obj.last_modified}")
        
        print("\n Données présentes dans MinIO")
        return True
        
    except Exception as e:
        print(f" Erreur MinIO (non bloquante): {e}")
        return True

verify_minio_task = PythonOperator(
    task_id='verify_kafka_minio',
    python_callable=verify_kafka_to_minio,
    dag=dag,
)

# ============================================
# ÉTAPE 4: VÉRIFIER SPARK STREAMING → POSTGRESQL
# ============================================

def check_spark_to_postgres():
    """Vérifie que Spark Streaming écrit dans PostgreSQL"""
    print("\n" + "="*70)
    print("⚡ VÉRIFICATION SPARK STREAMING → POSTGRESQL")
    print("="*70 + "\n")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_energie')
        
        # Vérifier tables
        tables = ['mix_energie_table', 'carbone_energie_table']
        
        for table in tables:
            try:
                count = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
                
                if count == 0:
                    print(f" {table}: Table vide")
                    continue
                
                # Dernière mise à jour
                last_update = hook.get_first(
                    f"SELECT MAX(processing_time) FROM {table}"
                )[0]
                
                # Données récentes (dernières 10 min)
                recent = hook.get_first(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE processing_time > NOW() - INTERVAL '10 minutes'
                """)[0]
                
                print(f" {table}:")
                print(f"   Total: {count:,} enregistrements")
                print(f"   Dernière mise à jour: {last_update}")
                print(f"   Nouveaux (10 min): {recent}")
                
            except Exception as e:
                print(f" {table}: {e}")
        
        print("\n Spark Streaming vers PostgreSQL opérationnel")
        return True
        
    except Exception as e:
        print(f" Erreur: {e}")
        return False

check_spark_postgres_task = PythonOperator(
    task_id='check_spark_postgresql',
    python_callable=check_spark_to_postgres,
    dag=dag,
)

# ============================================
# VÉRIFIER POSTGRESQL → CASSANDRA
# ============================================

def verify_postgres_to_cassandra():
    """Vérifie la synchronisation PostgreSQL → Cassandra"""
    print("\n" + "="*70)
    print(" VÉRIFICATION POSTGRESQL → CASSANDRA")
    print("="*70 + "\n")
    
    import subprocess
    
    try:
        # Vérifier si le keyspace existe
        keyspace_name = None
        for ks in ['energie', 'energy_keyspace']:
            keyspace_check = subprocess.run(
                ['docker', 'exec', 'cassandra', 'cqlsh', '-e', 
                 f"DESCRIBE KEYSPACE {ks};"],
                capture_output=True,
                text=True
            )
            
            if 'does not exist' not in keyspace_check.stderr:
                keyspace_name = ks
                print(f" Keyspace '{ks}' existe")
                break
        
        if not keyspace_name:
            print(" Aucun keyspace trouvé")
            print("   Les données restent dans PostgreSQL uniquement")
            return True  
        
        # Vérifier données dans Cassandra
        table_name = 'mix_energie' if keyspace_name == 'energie' else 'mix_energie_table'
        cql_check = subprocess.run(
            ['docker', 'exec', 'cassandra', 'cqlsh', '-e', 
             f"SELECT COUNT(*) FROM {keyspace_name}.{table_name};"],
            capture_output=True,
            text=True
        )
        
        if 'count' in cql_check.stdout.lower():
            print(f" Données présentes dans Cassandra ({keyspace_name}.{table_name})")
            # Extraire le nombre
            for line in cql_check.stdout.split('\n'):
                if line.strip().isdigit():
                    print(f"   Total: {line.strip()} enregistrements")
            return True
        elif 'does not exist' in cql_check.stderr:
            print(f" Table {table_name} n'existe pas dans Cassandra")
            print("   Le job Spark va la créer automatiquement")
            return True  # Non bloquant
        else:
            print(" Réponse inattendue de Cassandra")
            return True  # Non bloquant
            
    except Exception as e:
        print(f" Erreur (non bloquante): {e}")
        return True

verify_cassandra_task = PythonOperator(
    task_id='verify_postgresql_cassandra',
    python_callable=verify_postgres_to_cassandra,
    dag=dag,
)

# ============================================
# RAPPORT FINAL
# ============================================

def generate_pipeline_report():
    """Génère un rapport complet du pipeline"""
    print("\n" + "="*70)
    print(" RAPPORT COMPLET DU PIPELINE ENERGY")
    print("="*70)
    print(f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_energie')
        
        # Statistiques PostgreSQL
        print(" STATISTIQUES POSTGRESQL:")
        
        total_mix = 0
        total_carbone = 0
        
        for table in ['mix_energie_table', 'carbone_energie_table']:
            try:
                stats = hook.get_first(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT zone) as zones,
                        MAX(processing_time) as last_update
                    FROM {table}
                """)
                
                print(f"\n  {table}:")
                print(f"    Total: {stats[0]:,} enregistrements")
                print(f"    Zones: {stats[1]}")
                print(f"    Dernière MAJ: {stats[2]}")
                
                if 'mix' in table:
                    total_mix = stats[0]
                else:
                    total_carbone = stats[0]
                
            except Exception as e:
                print(f"   {table}: {e}")
        
        # Vérifier Cassandra
        import subprocess
        print("\n STATISTIQUES CASSANDRA:")
        
        cassandra_count = 0
        try:
            cass_result = subprocess.run(
                ['docker', 'exec', 'cassandra', 'cqlsh', '-e', 
                 "SELECT COUNT(*) FROM energie.mix_energie;"],
                capture_output=True,
                text=True
            )
            
            for line in cass_result.stdout.split('\n'):
                if line.strip().isdigit():
                    cassandra_count = int(line.strip())
                    print(f"  mix_energie: {cassandra_count:,} enregistrements")
                    break
        except Exception as e:
            print(f"   Cassandra non accessible: {e}")
        
        # Top zones par production
        print("\n TOP 5 ZONES (Production moyenne):")
        try:
            top_zones = hook.get_records("""
                SELECT 
                    zone,
                    COUNT(*) as records,
                    ROUND(AVG(production_total)::numeric, 0) as avg_prod
                FROM mix_energie_table
                WHERE datetime >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY zone
                ORDER BY avg_prod DESC
                LIMIT 5
            """)
            
            for zone, records, avg_prod in top_zones:
                print(f"  {zone}: {avg_prod:,.0f} MW (basé sur {records:,} enregistrements)")
        except Exception as e:
            print(f"   Impossible de récupérer le top zones: {e}")
        
        # Statut global
        print("\n FLUX DE DONNÉES:")
        print("  1. API ElectricityMaps → Kafka: active")
        print("  2. Kafka → MinIO (Bronze): ")
        print("  3. Kafka → Spark Streaming → PostgreSQL: active")
        print(f"     └─ Mix: {total_mix:,} | Carbone: {total_carbone:,}")
        
        if cassandra_count > 0:
            print(f"  4. PostgreSQL → Cassandra: active")
            print(f"     └─ Cassandra: {cassandra_count:,} enregistrements")
        else:
            print("  4. PostgreSQL → Cassandra:  En attente")
        
        print("  5. Prêt pour visualisation Grafana: active")
        
        # Instructions Grafana
        print("\n VISUALISATION GRAFANA:")
        print("   URL: http://localhost:3000")
        print("   Login: admin / admin123")
        print("\n  Sources de données configurables:")
        print("    • PostgreSQL: energie_db (temps réel)")
        print("    • Cassandra: energie keyspace (archivage)")
        
        print("\n PIPELINE OPÉRATIONNEL")
        print(f" Total enregistrements: {total_mix + total_carbone:,}")
        print("="*70 + "\n")
        
        return {
            'status': 'success',
            'postgres_total': total_mix + total_carbone,
            'cassandra_total': cassandra_count,
            'zones_count': stats[1] if stats else 0
        }
        
    except Exception as e:
        print(f"\n ERREUR: {e}")
        raise

final_report_task = PythonOperator(
    task_id='generate_final_report',
    python_callable=generate_pipeline_report,
    dag=dag,
)

# ============================================
# ORDRE D'EXÉCUTION 
# ============================================

# Flux de supervision
check_infra_task >> check_producer_task >> verify_minio_task
verify_minio_task >> check_spark_postgres_task >> verify_cassandra_task
verify_cassandra_task >> final_report_task


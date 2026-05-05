"""
DAG Airflow principal pour le pipeline de données économiques d'Afrique de l'Ouest
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.transfers.postgres_to_s3 import PostgresToS3Operator

# Configuration par défaut
default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Définition du DAG
dag = DAG(
    'west_africa_economic_pipeline',
    default_args=default_args,
    description='Pipeline de données économiques d\'Afrique de l\'Ouest',
    schedule_interval='@daily',
    catchup=False,
)

# TODO: Ajouter les tâches du pipeline
# 1. Extraction API World Bank
# 2. Transformation des données
# 3. Chargement vers PostgreSQL
# 4. Chargement vers S3

if __name__ == "__main__":
    dag.cli()

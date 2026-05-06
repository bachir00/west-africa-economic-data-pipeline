from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.smtp.operators.smtp import EmailOperator
import os
import sys
import logging

# Add plugins directory to path (works on both Windows and Linux)
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_root, 'plugins'))

from extractors.world_bank_extractor import WorldBankExtractor
from transformation.data_transformer import DataTransformer
from loaders.s3_loader import S3Loader
from loaders.postgres_loader import PostgresLoader

logger = logging.getLogger(__name__)

# Config
S3_BUCKET = os.getenv('S3_BUCKET', 'west-africa-economic-data')
# POSTGRES_CONN = os.getenv('POSTGRES_CONN','')
POSTGRES_CONN="postgresql+psycopg2://airflow:airflow@postgres:5432/west_africa_economic_data"

# Debug: Vérifier que POSTGRES_CONN est défini
if not POSTGRES_CONN:
    logger.warning("⚠️ POSTGRES_CONN is empty! Check docker-compose environment variables")
else:
    logger.info(f"✅ POSTGRES_CONN is set: {POSTGRES_CONN[:40]}...")

default_args = {
    'owner': 'bachir',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'west_africa_economic_pipeline',
    default_args=default_args,
    description='ETL pipeline for West Africa economic indicators',
    schedule='@monthly',
    catchup=False,
    tags=['data-engineering', 'west-africa', 'etl', 'world-bank'],
)


def extract(**context):
    """Extract data from World Bank API."""
    logger.info("Starting extraction from World Bank API...")
    
    extractor = WorldBankExtractor(start_year=2019, end_year=2020)
    df = extractor.extract_all()
    
    # Push to XCom for next task
    context['ti'].xcom_push(key='raw_data', value=df.to_json())
    
    logger.info(f"Extraction complete: {len(df)} records")
    return len(df)


def transform(**context):
    """Transform and validate extracted data."""
    import pandas as pd
    
    raw_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = pd.read_json(raw_json)
    
    logger.info("Starting transformation...")
    
    transformer = DataTransformer(df)
    transformed_df = (
        transformer
        .clean_nulls()
        .add_derived_metrics()
        .validate()
        .get_dataframe()
    )
    
    # Note: Categorical column 'data_quality_flag' will be serialized as string in JSON
    # It will be converted back to appropriate type in load_to_postgres
    
    context['ti'].xcom_push(
        key='transformed_data', 
        value=transformed_df.to_json()
    )
    
    logger.info(f"Transformation complete: {len(transformed_df)} records")
    return len(transformed_df)


def load_to_s3(**context):
    """Load data to AWS S3."""
    import pandas as pd
    
    raw_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    transformed_json = context['ti'].xcom_pull(
        key='transformed_data', 
        task_ids='transform'
    )
    
    raw_df = pd.read_json(raw_json)
    transformed_df = pd.read_json(transformed_json)
    
    loader = S3Loader(bucket_name=S3_BUCKET)
    
    raw_path = loader.upload_raw(raw_df)
    processed_path = loader.upload_processed(transformed_df)
    
    logger.info(f"S3 upload complete: {raw_path} | {processed_path}")
    return processed_path


def load_to_postgres(**context):
    """Load transformed data to PostgreSQL."""
    import pandas as pd
    import os
    from datetime import datetime
    
    transformed_json = context['ti'].xcom_pull(
        key='transformed_data', 
        task_ids='transform'
    )
    df = pd.read_json(transformed_json)
    
    # FIX: Convert ingestion_timestamp back to datetime (JSON serialization converts to int64)
    if 'ingestion_timestamp' in df.columns:
        df['ingestion_timestamp'] = pd.to_datetime(df['ingestion_timestamp'])
        logger.info(f"Converted ingestion_timestamp to datetime")
    
    # Debug: Check if SQL file exists
    sql_path = '/opt/airflow/sql/create_tables.sql'
    logger.info(f"Checking SQL file at: {sql_path}")
    logger.info(f"File exists: {os.path.exists(sql_path)}")
    
    loader = PostgresLoader(conn_string=POSTGRES_CONN)
    
    # Skip schema creation if file doesn't exist (tables already created)
    if os.path.exists(sql_path):
        logger.info("Creating schema...")
        loader.create_schema(sql_path)
    else:
        logger.warning(f"SQL file not found at {sql_path}, skipping schema creation")
    
    logger.info(f"Upserting {len(df)} records...")
    logger.info(f"DataFrame dtypes:\n{df.dtypes}")
    loader.upsert(df, 'west_africa_economic_data')
    
    logger.info(f"PostgreSQL load complete: {len(df)} records")
    return len(df)


def validate_pipeline(**context):
    """Final validation - check data landed correctly."""
    from sqlalchemy import create_engine, text
    
    engine = create_engine(POSTGRES_CONN)
    
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) as total, "
            "COUNT(DISTINCT country_code) as countries, "
            "MIN(year) as min_year, MAX(year) as max_year "
            "FROM west_africa_economic_data"
        ))
        row = result.fetchone()
    
    logger.info(
        f"Pipeline validation: {row.total} records, "
        f"{row.countries} countries, "
        f"years {row.min_year}-{row.max_year}"
    )
    
    assert row.countries == 10, f"Expected 10 countries, got {row.countries}"
    assert row.total > 0, "No records found in database"
    
    return {
        'total_records': row.total,
        'countries': row.countries,
        'year_range': f"{row.min_year}-{row.max_year}"
    }


# Define tasks
t_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

t_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# t_load_s3 = PythonOperator(
#     task_id='load_to_s3',
#     python_callable=load_to_s3,
#     dag=dag,
# )

t_load_postgres = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

t_validate = PythonOperator(
    task_id='validate_pipeline',
    python_callable=validate_pipeline,
    dag=dag,
)

# Pipeline flow
# t_extract >> t_transform >> [t_load_s3, t_load_postgres] >> t_validate
t_extract >> t_transform >> [ t_load_postgres] >> t_validate
#!/usr/bin/env python3
"""
Test du pipeline complet: extract -> transform -> load
Simule exactement ce que le DAG fait
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add plugins to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'plugins'))

from extractors.world_bank_extractor import WorldBankExtractor
from transformation.data_transformer import DataTransformer
from loaders.postgres_loader import PostgresLoader

def test_full_pipeline():
    """Test du pipeline complet"""
    
    POSTGRES_CONN = os.getenv(
        'POSTGRES_CONN',
        'postgresql+psycopg2://airflow:airflow@postgres:5432/west_africa_economic_data'
    )
    
    try:
        # 1. EXTRACT
        logger.info("=" * 60)
        logger.info("1️⃣ EXTRACT")
        logger.info("=" * 60)
        extractor = WorldBankExtractor(start_year=2019, end_year=2020)
        raw_df = extractor.extract_all()
        logger.info(f"✅ Extracted {len(raw_df)} records")
        logger.info(f"Columns: {list(raw_df.columns)}")
        logger.info(f"First row:\n{raw_df.head(1)}")
        
        # 2. TRANSFORM
        logger.info("=" * 60)
        logger.info("2️⃣ TRANSFORM")
        logger.info("=" * 60)
        transformer = DataTransformer(raw_df)
        transformed_df = (
            transformer
            .clean_nulls()
            .add_derived_metrics()
            .validate()
            .get_dataframe()
        )
        logger.info(f"✅ Transformed {len(transformed_df)} records")
        logger.info(f"Columns: {list(transformed_df.columns)}")
        logger.info(f"Data types:\n{transformed_df.dtypes}")
        logger.info(f"First row:\n{transformed_df.head(1)}")
        
        # 3. CHECK XCom format (comme le DAG le fait)
        logger.info("=" * 60)
        logger.info("3️⃣ XCOM SIMULATION (JSON serialization)")
        logger.info("=" * 60)
        xcom_json = transformed_df.to_json()
        logger.info(f"✅ Serialized to JSON: {len(xcom_json)} bytes")
        xcom_df = pd.read_json(xcom_json)
        logger.info(f"✅ Deserialized from JSON: {len(xcom_df)} records")
        logger.info(f"Data types after deserialization:\n{xcom_df.dtypes}")
        
        # FIX: Convert ingestion_timestamp back to datetime (JSON serialization converts to int64)
        if 'ingestion_timestamp' in xcom_df.columns:
            xcom_df['ingestion_timestamp'] = pd.to_datetime(xcom_df['ingestion_timestamp'])
            logger.info(f"✅ Converted ingestion_timestamp to datetime")
        
        logger.info(f"Final data types:\n{xcom_df.dtypes}")
        
        # 4. LOAD
        logger.info("=" * 60)
        logger.info("4️⃣ LOAD TO POSTGRES")
        logger.info("=" * 60)
        loader = PostgresLoader(conn_string=POSTGRES_CONN)
        
        # Try create_schema
        sql_path = '/opt/airflow/sql/create_tables.sql'
        if os.path.exists(sql_path):
            logger.info(f"Creating schema from {sql_path}...")
            try:
                loader.create_schema(sql_path)
                logger.info("✅ Schema created")
            except Exception as e:
                logger.warning(f"⚠️ Schema creation error (may already exist): {str(e)}")
        else:
            logger.warning(f"SQL file not found: {sql_path}")
        
        # Try upsert with the corrected DataFrame
        logger.info(f"Upserting {len(xcom_df)} records...")
        logger.info(f"Data types before upsert:\n{xcom_df.dtypes}")
        loader.upsert(xcom_df, 'west_africa_economic_data')
        logger.info("✅ Upsert completed")
        
        # 5. VALIDATION
        logger.info("=" * 60)
        logger.info("5️⃣ VALIDATION")
        logger.info("=" * 60)
        with loader.engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM west_africa_economic_data")
            count = result.fetchone()[0]
            logger.info(f"✅ Total records: {count}")
        
        logger.info("\n" + "=" * 60)
        logger.info("✨ FULL PIPELINE TEST PASSED ✨")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ PIPELINE FAILED: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_full_pipeline()
    sys.exit(0 if success else 1)

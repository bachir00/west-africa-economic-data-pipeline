#!/usr/bin/env python3
"""
Test standalone du PostgresLoader sans Airflow
Pour isoler les problèmes et tester la connexion directement
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

from loaders.postgres_loader import PostgresLoader

def test_postgres_loader():
    """Test du PostgresLoader avec données de test"""
    
    # Configuration
    POSTGRES_CONN = os.getenv(
        'POSTGRES_CONN',
        'postgresql+psycopg2://airflow:airflow@postgres:5432/west_africa_economic_data'
    )
    
    logger.info(f"🔌 Connection String: {POSTGRES_CONN}")
    
    try:
        # 1. Initialiser le loader
        logger.info("📝 Initialising PostgresLoader...")
        loader = PostgresLoader(POSTGRES_CONN)
        logger.info("✅ PostgresLoader initialised successfully")
        
        # 2. Test de la connexion en créant un petit test
        logger.info("🧪 Testing database connection...")
        with loader.engine.connect() as conn:
            result = conn.execute("SELECT 1")
            logger.info(f"✅ Database connection successful: {result.fetchone()}")
        
        # 3. Créer des données de test
        logger.info("📊 Creating test data...")
        test_data = pd.DataFrame({
            'country_code': ['MLI', 'SEN', 'BFA'],
            'country_name': ['Mali', 'Senegal', 'Burkina Faso'],
            'year': [2020, 2020, 2020],
            'gdp_current_usd': [17.9e9, 16.6e9, 15.9e9],
            'population_total': [20.25e6, 16.74e6, 20.32e6],
            'inflation_rate': [1.3, 2.5, 3.0],
            'unemployment_rate': [8.5, 5.2, 4.8],
            'internet_users_percent': [15.2, 25.3, 18.1],
            'gdp_per_capita': [882, 990, 783],
            'digital_adoption_score': [35.0, 42.0, 38.0],
            'economic_health_score': [52.0, 58.0, 50.0],
            'data_quality_flag': [1, 1, 1],
            'ingestion_timestamp': [datetime.now()] * 3
        })
        logger.info(f"✅ Test data created: {len(test_data)} rows")
        logger.info(f"Columns: {list(test_data.columns)}")
        
        # 4. Test du load
        logger.info("📤 Testing load() method...")
        loader.load(test_data, 'west_africa_economic_data', if_exists='append')
        logger.info("✅ load() method successful")
        
        # 5. Vérifier les données
        logger.info("🔍 Verifying loaded data...")
        with loader.engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) as count FROM west_africa_economic_data")
            count = result.fetchone()[0]
            logger.info(f"✅ Total records in database: {count}")
            
            # Show sample rows
            result = conn.execute(
                "SELECT country_code, country_name, year, gdp_current_usd "
                "FROM west_africa_economic_data ORDER BY ingestion_timestamp DESC LIMIT 3"
            )
            logger.info("📋 Sample rows:")
            for row in result:
                logger.info(f"   {row}")
        
        logger.info("\n✨ ALL TESTS PASSED ✨")
        return True
        
    except Exception as e:
        logger.error(f"❌ TEST FAILED: {str(e)}", exc_info=True)
        return False

if __name__ == '__main__':
    success = test_postgres_loader()
    sys.exit(0 if success else 1)

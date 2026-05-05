import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

class PostgresLoader:
    
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.engine = create_engine(conn_string)
    
    def create_schema(self, sql_file_path: str):
        """Create tables from SQL file."""
        with open(sql_file_path, 'r') as f:
            sql = f.read()
        
        with self.engine.connect() as conn:
            conn.execute(sql)
            conn.commit()
        
        logger.info("Schema created successfully")
    
    def load(
        self, 
        df: pd.DataFrame, 
        table_name: str,
        if_exists: str = 'append'
    ):
        """Load DataFrame into PostgreSQL table."""
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists=if_exists,
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Loaded {len(df)} records into {table_name}")
    
    def upsert(self, df: pd.DataFrame, table_name: str):
        """Upsert records to avoid duplicates."""
        temp_table = f"temp_{table_name}"
        
        # Load to temp table first
        self.load(df, temp_table, if_exists='replace')
        
        # Upsert from temp to main table
        upsert_sql = f"""
            INSERT INTO {table_name}
            SELECT * FROM {temp_table}
            ON CONFLICT (country_code, year)
            DO UPDATE SET
                gdp_current_usd = EXCLUDED.gdp_current_usd,
                population_total = EXCLUDED.population_total,
                inflation_rate = EXCLUDED.inflation_rate,
                unemployment_rate = EXCLUDED.unemployment_rate,
                internet_users_percent = EXCLUDED.internet_users_percent,
                gdp_per_capita = EXCLUDED.gdp_per_capita,
                economic_health_score = EXCLUDED.economic_health_score,
                ingestion_timestamp = EXCLUDED.ingestion_timestamp;
            
            DROP TABLE {temp_table};
        """
        
        with self.engine.connect() as conn:
            conn.execute(upsert_sql)
            conn.commit()
        
        logger.info(f"Upserted {len(df)} records into {table_name}")
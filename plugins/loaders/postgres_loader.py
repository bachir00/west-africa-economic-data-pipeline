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
        from sqlalchemy import text
        
        temp_table = f"temp_{table_name}"
        
        # Charge dans la table temporaire
        self.load(df, temp_table, if_exists='replace')
        
        # Upsert en excluant la colonne id (auto-générée par SERIAL)
        # On liste explicitement les colonnes du DataFrame seulement
        df_columns = ", ".join(df.columns.tolist())
        
        excluded_updates = ", ".join([
            f"{col} = EXCLUDED.{col}" 
            for col in df.columns 
            if col not in ('country_code', 'year')
        ])
        
        upsert_sql = text(f"""
            INSERT INTO {table_name} ({df_columns})
            SELECT {df_columns}
            FROM {temp_table}
            ON CONFLICT (country_code, year)
            DO UPDATE SET {excluded_updates}
        """)
        
        drop_sql = text(f"DROP TABLE IF EXISTS {temp_table}")
        
        with self.engine.connect() as conn:
            conn.execute(upsert_sql)
            conn.execute(drop_sql)
            conn.commit()
        
        logger.info(f"Upserted {len(df)} records into {table_name}")




import sys
from dotenv import load_dotenv
sys.path.insert(0, '../plugins')
import os 
load_dotenv()

from loaders.postgres_loader import PostgresLoader
import pandas as pd

# Chaîne de connexion PostgreSQL
conn_string = os.getenv("POSTGRES_CONN", "")

# Initialiser le loader
loader = PostgresLoader(conn_string)

# Créer les tables depuis le SQL
loader.create_schema('./sql/create_tables.sql')

# Créer des données de test
test_data = pd.DataFrame({
    'country_code': ['SEN', 'CIV', 'MLI'],
    'country_name': ['Senegal', 'Côte d\'Ivoire', 'Mali'],
    'year': [2023, 2023, 2023],
    'gdp_current_usd': [27890000000, 74894000000, 18462000000],
    'population_total': [17914000, 27478000, 22593000],
    'inflation_rate': [1.82, 2.21, 1.83],
    'unemployment_rate': [3.2, 3.5, 4.1],
    'internet_users_percent': [68.5, 54.2, 47.8],
    'gdp_per_capita': [1557, 2721, 817],
    'digital_adoption_score': [65.2, 58.1, 51.3],
    'economic_health_score': [72.5, 68.3, 61.8],
    'data_quality_flag': ['GOOD', 'GOOD', 'FAIR']
})

# Charger les données
loader.load(test_data, 'west_africa_economic_data')

print("✅ Données chargées avec succès !")
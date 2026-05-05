import requests
import pandas as pd
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

WEST_AFRICA_COUNTRIES = {
    'SN': 'Senegal',
    'CI': 'Cote d\'Ivoire',
    'GH': 'Ghana',
    'NG': 'Nigeria',
    'ML': 'Mali',
    'BF': 'Burkina Faso',
    'GN': 'Guinea',
    'TG': 'Togo',
    'BJ': 'Benin',
    'MR': 'Mauritania'
}

INDICATORS = {
    'NY.GDP.MKTP.CD': 'gdp_current_usd',
    'SP.POP.TOTL': 'population_total',
    'FP.CPI.TOTL.ZG': 'inflation_rate',
    'SL.UEM.TOTL.ZS': 'unemployment_rate',
    'IT.NET.USER.ZS': 'internet_users_percent'
}

class WorldBankExtractor:
    
    BASE_URL = "https://api.worldbank.org/v2"
    
    def __init__(self, start_year: int = 2010, end_year: int = 2023):
        self.start_year = start_year
        self.end_year = end_year
    
    def fetch_indicator(
        self, 
        country_code: str, 
        indicator: str
    ) -> List[Dict]:
        """Fetch a single indicator for a country from World Bank API."""
        url = (
            f"{self.BASE_URL}/country/{country_code}/indicator/{indicator}"
            f"?format=json&date={self.start_year}:{self.end_year}&per_page=100"
        )
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if len(data) < 2 or not data[1]:
                logger.warning(f"No data for {country_code} - {indicator}")
                return []
            
            return data[1]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {indicator} for {country_code}: {e}")
            return []
    
    def extract_all(self) -> pd.DataFrame:
        """Extract all indicators for all West African countries."""
        all_records = []
        
        for country_code, country_name in WEST_AFRICA_COUNTRIES.items():
            logger.info(f"Extracting data for {country_name}...")
            
            country_data = {}
            
            for indicator_code, indicator_name in INDICATORS.items():
                records = self.fetch_indicator(country_code, indicator_code)
                
                for record in records:
                    year = record.get('date')
                    value = record.get('value')
                    
                    if year not in country_data:
                        country_data[year] = {
                            'country_code': country_code,
                            'country_name': country_name,
                            'year': int(year)
                        }
                    
                    country_data[year][indicator_name] = value
            
            all_records.extend(country_data.values())
        
        df = pd.DataFrame(all_records)
        logger.info(f"Extracted {len(df)} records total")
        return df


import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataTransformer:
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
    
    def clean_nulls(self) -> 'DataTransformer':
        """Handle null values appropriately."""
        numeric_cols = [
            'gdp_current_usd', 
            'population_total',
            'inflation_rate', 
            'unemployment_rate',
            'internet_users_percent'
        ]
        
        for col in numeric_cols:
            if col in self.df.columns:
                # Fill nulls with column median per country
                self.df[col] = self.df.groupby('country_code')[col].transform(
                    lambda x: x.fillna(x.median())
                )
        
        logger.info("Null values handled")
        return self
    
    def add_derived_metrics(self) -> 'DataTransformer':
        """Add calculated metrics for richer analysis."""
        
        # GDP per capita
        if all(c in self.df.columns for c in ['gdp_current_usd', 'population_total']):
            self.df['gdp_per_capita'] = (
                self.df['gdp_current_usd'] / self.df['population_total']
            ).round(2)
        
        # Digital adoption score (normalized internet users)
        if 'internet_users_percent' in self.df.columns:
            self.df['digital_adoption_score'] = (
                self.df['internet_users_percent'] / 100
            ).round(4)
        
        # Economic health score (composite)
        self.df['economic_health_score'] = self._calculate_health_score()
        
        # Metadata
        self.df['ingestion_timestamp'] = datetime.utcnow()
        self.df['data_quality_flag'] = self._flag_data_quality()
        
        logger.info("Derived metrics added")
        return self
    
    def _calculate_health_score(self) -> pd.Series:
        """Simple composite economic health score (0-100)."""
        score = pd.Series(50.0, index=self.df.index)
        
        if 'inflation_rate' in self.df.columns:
            # Lower inflation = better score
            inflation_penalty = self.df['inflation_rate'].clip(0, 50) * 0.5
            score -= inflation_penalty
        
        if 'unemployment_rate' in self.df.columns:
            # Lower unemployment = better score
            unemployment_penalty = self.df['unemployment_rate'].clip(0, 50) * 0.3
            score -= unemployment_penalty
        
        if 'internet_users_percent' in self.df.columns:
            # Higher internet usage = better score
            internet_bonus = self.df['internet_users_percent'].clip(0, 100) * 0.2
            score += internet_bonus
        
        return score.clip(0, 100).round(2)
    
    def _flag_data_quality(self) -> pd.Series:
        """Flag records with potential data quality issues."""
        numeric_cols = [
            'gdp_current_usd', 
            'population_total',
            'inflation_rate'
        ]
        
        available_cols = [c for c in numeric_cols if c in self.df.columns]
        null_count = self.df[available_cols].isnull().sum(axis=1)
        
        return pd.cut(
            null_count,
            bins=[-1, 0, 1, len(available_cols)],
            labels=['HIGH', 'MEDIUM', 'LOW']
        )
    
    def validate(self) -> 'DataTransformer':
        """Basic data validation checks."""
        assert len(self.df) > 0, "DataFrame is empty after transformation"
        assert 'country_code' in self.df.columns, "Missing country_code column"
        assert 'year' in self.df.columns, "Missing year column"
        
        invalid_years = self.df[
            (self.df['year'] < 1990) | (self.df['year'] > 2024)
        ]
        if len(invalid_years) > 0:
            logger.warning(f"Found {len(invalid_years)} records with invalid years")
        
        logger.info(f"Validation passed. {len(self.df)} records ready to load.")
        return self
    
    def get_dataframe(self) -> pd.DataFrame:
        return self.df
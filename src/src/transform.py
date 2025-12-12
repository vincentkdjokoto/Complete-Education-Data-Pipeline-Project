#!/usr/bin/env python3
"""
Education Data Transformer
Cleans and transforms raw education data
"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Optional
import yaml
import re

logger = logging.getLogger(__name__)

class EducationDataTransformer:
    """Transforms and cleans education data"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
    
    def clean_enrollment_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean enrollment rate data"""
        logger.info("Cleaning enrollment data")
        
        # Make a copy
        df_clean = df.copy()
        
        # Standardize column names
        df_clean.columns = [self._standardize_col_name(col) for col in df_clean.columns]
        
        # Filter relevant dimensions
        if 'indicator' in df_clean.columns:
            df_clean = df_clean[df_clean['indicator'].str.contains('ENRL', na=False)]
        
        # Clean country codes
        if 'location' in df_clean.columns:
            df_clean['country_code'] = df_clean['location'].str[:3]
            df_clean['country_name'] = df_clean['location'].apply(self._map_country_code)
        
        # Clean year column
        if 'time' in df_clean.columns:
            df_clean['year'] = pd.to_numeric(df_clean['time'], errors='coerce')
            df_clean = df_clean[df_clean['year'].between(2000, 2023)]
        
        # Clean values
        if 'value' in df_clean.columns:
            df_clean['enrollment_rate'] = pd.to_numeric(df_clean['value'], errors='coerce')
            # Remove outliers
            df_clean = df_clean[df_clean['enrollment_rate'].between(0, 200)]
        
        # Add metadata
        df_clean['data_source'] = 'OECD'
        df_clean['extraction_date'] = datetime.now().date()
        
        logger.info(f"Cleaned enrollment data: {len(df_clean)} records")
        return df_clean
    
    def clean_graduation_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean graduation rate data"""
        logger.info("Cleaning graduation data")
        
        df_clean = df.copy()
        df_clean.columns = [self._standardize_col_name(col) for col in df_clean.columns]
        
        if 'indicator' in df_clean.columns:
            df_clean = df_clean[df_clean['indicator'].str.contains('GRAD', na=False)]
        
        if 'location' in df_clean.columns:
            df_clean['country_code'] = df_clean['location'].str[:3]
            df_clean['country_name'] = df_clean['location'].apply(self._map_country_code)
        
        if 'time' in df_clean.columns:
            df_clean['year'] = pd.to_numeric(df_clean['time'], errors='coerce')
            df_clean = df_clean[df_clean['year'].between(2000, 2023)]
        
        if 'value' in df_clean.columns:
            df_clean['graduation_rate'] = pd.to_numeric(df_clean['value'], errors='coerce')
            df_clean = df_clean[df_clean['graduation_rate'].between(0, 120)]
        
        # Calculate completion rates
        if 'graduation_rate' in df_clean.columns:
            df_clean['completion_rate'] = df_clean['graduation_rate'] / 100
        
        df_clean['data_source'] = 'OECD'
        df_clean['extraction_date'] = datetime.now().date()
        
        logger.info(f"Cleaned graduation data: {len(df_clean)} records")
        return df_clean
    
    def clean_spending_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean education spending data"""
        logger.info("Cleaning spending data")
        
        df_clean = df.copy()
        df_clean.columns = [self._standardize_col_name(col) for col in df_clean.columns]
        
        if 'indicator' in df_clean.columns:
            df_clean = df_clean[df_clean['indicator'].str.contains('FIN', na=False)]
        
        if 'location' in df_clean.columns:
            df_clean['country_code'] = df_clean['location'].str[:3]
            df_clean['country_name'] = df_clean['location'].apply(self._map_country_code)
        
        if 'time' in df_clean.columns:
            df_clean['year'] = pd.to_numeric(df_clean['time'], errors='coerce')
            df_clean = df_clean[df_clean['year'].between(2000, 2023)]
        
        if 'value' in df_clean.columns:
            df_clean['spending_usd'] = pd.to_numeric(df_clean['value'], errors='coerce')
            # Remove extreme outliers
            q_low = df_clean['spending_usd'].quantile(0.01)
            q_high = df_clean['spending_usd'].quantile(0.99)
            df_clean = df_clean[df_clean['spending_usd'].between(q_low, q_high)]
        
        # Calculate per capita spending if population data available
        if 'spending_usd' in df_clean.columns:
            # This would need actual population data - placeholder
            df_clean['spending_per_capita'] = df_clean['spending_usd']
        
        df_clean['data_source'] = 'OECD'
        df_clean['extraction_date'] = datetime.now().date()
        df_clean['currency'] = 'USD'
        
        logger.info(f"Cleaned spending data: {len(df_clean)} records")
        return df_clean
    
    def _standardize_col_name(self, col_name: str) -> str:
        """Standardize column names"""
        if not isinstance(col_name, str):
            col_name = str(col_name)
        
        # Convert to lowercase, replace spaces with underscores
        col_name = col_name.lower().strip()
        col_name = re.sub(r'[^\w]', '_', col_name)
        col_name = re.sub(r'_+', '_', col_name)
        
        # Common mappings
        mappings = {
            'time_period': 'year',
            'ref_area': 'country',
            'obs_value': 'value',
            'location': 'country_code'
        }
        
        return mappings.get(col_name, col_name)
    
    def _map_country_code(self, code: str) -> str:
        """Map OECD country codes to country names"""
        country_map = {
            'USA': 'United States',
            'GBR': 'United Kingdom',
            'DEU': 'Germany',
            'FRA': 'France',
            'JPN': 'Japan',
            'CAN': 'Canada',
            'AUS': 'Australia',
            'OECD': 'OECD Average',
            'EU': 'European Union'
        }
        
        if isinstance(code, str) and code in country_map:
            return country_map[code]
        return code
    
    def create_country_metadata(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Create country metadata table"""
        logger.info("Creating country metadata")
        
        all_countries = set()
        
        for df in dfs:
            if 'country_code' in df.columns:
                all_countries.update(df['country_code'].unique())
            if 'country_name' in df.columns:
                all_countries.update(df['country_name'].unique())
        
        # Create metadata DataFrame
        metadata = []
        for country in sorted(all_countries):
            if pd.notna(country) and str(country).strip():
                metadata.append({
                    'country_code': str(country)[:3] if len(str(country)) <= 3 else '',
                    'country_name': str(country),
                    'region': self._get_region(str(country)),
                    'income_group': self._get_income_group(str(country)),
                    'data_available': True,
                    'last_updated': datetime.now().date()
                })
        
        df_metadata = pd.DataFrame(metadata)
        logger.info(f"Created metadata for {len(df_metadata)} countries")
        return df_metadata
    
    def _get_region(self, country: str) -> str:
        """Get region for a country (simplified)"""
        regions = {
            'USA': 'North America',
            'CAN': 'North America',
            'GBR': 'Europe',
            'DEU': 'Europe',
            'FRA': 'Europe',
            'JPN': 'Asia',
            'AUS': 'Oceania'
        }
        
        if country in regions:
            return regions[country]
        
        # Check if it's a known European country
        european_codes = ['DEU', 'FRA', 'GBR', 'ITA', 'ESP']
        if country in european_codes:
            return 'Europe'
        
        return 'Other'
    
    def _get_income_group(self, country: str) -> str:
        """Get income group for a country (simplified)"""
        high_income = ['USA', 'GBR', 'DEU', 'FRA', 'JPN', 'CAN', 'AUS', 'OECD']
        
        if country in high_income:
            return 'High Income'
        return 'Not Specified'
    
    def save_clean_data(self, data_dict: Dict[str, pd.DataFrame], output_dir: str = "data/processed"):
        """Save cleaned data to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for name, df in data_dict.items():
            filename = f"{output_dir}/{name}_clean_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved clean {name} data: {len(df)} records")
        
        return f"{output_dir}/clean_data_{timestamp}"

def main():
    """Main transformation function"""
    logger.info("Starting data transformation pipeline")
    
    try:
        # For demo purposes, create sample data
        # In real use, this would load from extraction output
        import os
        
        transformer = EducationDataTransformer()
        
        # Check for raw data files
        raw_dir = "data/raw"
        if os.path.exists(raw_dir):
            raw_files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
            
            if raw_files:
                logger.info(f"Found {len(raw_files)} raw data files")
                # Load and clean each file
                # Implementation depends on actual file structure
                pass
        
        logger.info("Transformation completed")
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()

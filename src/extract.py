#!/usr/bin/env python3
"""
OECD Education Data Extractor
Extracts education statistics from OECD API
"""
import requests
import pandas as pd
import json
from datetime import datetime
import time
import logging
from typing import Dict, List, Optional
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OECDDataExtractor:
    """Extracts education data from OECD API"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = self.config['data_sources']['oecd_stats']['url']
        self.datasets = self.config['data_sources']['oecd_stats']['datasets']
        
    def fetch_oecd_dataset(self, dataset: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """
        Fetch data from OECD API
        
        Args:
            dataset: OECD dataset code
            params: Additional query parameters
            
        Returns:
            pandas DataFrame with the data
        """
        if params is None:
            params = {}
        
        # OECD API endpoint
        url = f"{self.base_url}{dataset}"
        
        # Default parameters for education data
        default_params = {
            "dimensionAtObservation": "AllDimensions",
            "detail": "dataonly"
        }
        
        # Merge with provided params
        query_params = {**default_params, **params}
        
        try:
            logger.info(f"Fetching dataset: {dataset}")
            response = requests.get(url, params=query_params, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Extract data points
            observations = data.get('dataSets', [{}])[0].get('observations', {})
            dimensions = data.get('structure', {}).get('dimensions', {}).get('observation', [])
            
            # Transform to DataFrame
            df = self._parse_oecd_json(observations, dimensions)
            
            logger.info(f"Successfully fetched {len(df)} records for {dataset}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching {dataset}: {e}")
            raise
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing response for {dataset}: {e}")
            raise
    
    def _parse_oecd_json(self, observations: Dict, dimensions: List) -> pd.DataFrame:
        """Parse OECD JSON response into DataFrame"""
        records = []
        
        for obs_key, obs_value in observations.items():
            # Parse observation key (dimension indices)
            indices = [int(idx) for idx in obs_key.split(':')]
            
            record = {}
            
            # Map dimensions to values
            for i, dim in enumerate(dimensions):
                if i < len(indices):
                    idx = indices[i]
                    if idx < len(dim['values']):
                        dim_name = dim.get('name', f'dim_{i}')
                        dim_value = dim['values'][idx].get('name', '')
                        record[dim_name] = dim_value
            
            # Add observation value
            if obs_value:
                record['value'] = obs_value[0]
            
            records.append(record)
        
        return pd.DataFrame(records)
    
    def extract_enrollment_data(self) -> pd.DataFrame:
        """Extract enrollment rate data"""
        logger.info("Extracting enrollment data")
        params = {
            "startPeriod": "2000",
            "endPeriod": "2023"
        }
        return self.fetch_oecd_dataset("EDU_ENRL", params)
    
    def extract_graduation_data(self) -> pd.DataFrame:
        """Extract graduation rate data"""
        logger.info("Extracting graduation data")
        params = {
            "startPeriod": "2000",
            "endPeriod": "2023"
        }
        return self.fetch_oecd_dataset("EDU_GRAD", params)
    
    def extract_spending_data(self) -> pd.DataFrame:
        """Extract education spending data"""
        logger.info("Extracting spending data")
        params = {
            "startPeriod": "2000",
            "endPeriod": "2023",
            "measure": "USD"
        }
        return self.fetch_oecd_dataset("EDU_FIN", params)
    
    def extract_all_data(self) -> Dict[str, pd.DataFrame]:
        """Extract all education datasets"""
        logger.info("Starting extraction of all education datasets")
        
        datasets = {}
        
        try:
            datasets['enrollment'] = self.extract_enrollment_data()
            time.sleep(1)  # Be polite to the API
            
            datasets['graduation'] = self.extract_graduation_data()
            time.sleep(1)
            
            datasets['spending'] = self.extract_spending_data()
            
            logger.info("Successfully extracted all datasets")
            return datasets
            
        except Exception as e:
            logger.error(f"Error in extraction: {e}")
            raise
    
    def save_raw_data(self, data_dict: Dict[str, pd.DataFrame], output_dir: str = "data/raw"):
        """Save raw extracted data to CSV files"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for name, df in data_dict.items():
            filename = f"{output_dir}/{name}_{timestamp}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved {len(df)} records to {filename}")
        
        # Save metadata
        metadata = {
            "extraction_time": datetime.now().isoformat(),
            "datasets_extracted": list(data_dict.keys()),
            "total_records": sum(len(df) for df in data_dict.values())
        }
        
        metadata_file = f"{output_dir}/metadata_{timestamp}.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

def main():
    """Main extraction function"""
    logger.info("Starting OECD education data extraction pipeline")
    
    try:
        # Initialize extractor
        extractor = OECDDataExtractor()
        
        # Extract all data
        all_data = extractor.extract_all_data()
        
        # Save raw data
        extractor.save_raw_data(all_data)
        
        logger.info("Extraction completed successfully")
        return all_data
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Education Data Loader
Loads cleaned education data into SQLite database
"""
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import logging
from typing import Dict, List, Optional
import yaml
import os

logger = logging.getLogger(__name__)

class EducationDataLoader:
    """Loads education data into SQLite database"""
    
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.db_path = self.config['database']['sqlite_path']
        self.tables = self.config['database']['tables']
        
        # Create database directory if it doesn't exist
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize database connection
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        self.metadata = MetaData()
        
        # Define table schemas
        self._define_schemas()
    
    def _define_schemas(self):
        """Define database table schemas"""
        # Enrollment table
        self.enrollment_table = Table(
            self.tables['enrollment'],
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('country_code', String(3)),
            Column('country_name', String(100)),
            Column('year', Integer),
            Column('enrollment_rate', Float),
            Column('education_level', String(50)),
            Column('gender', String(10)),
            Column('data_source', String(50)),
            Column('extraction_date', Date),
            Column('created_at', Date, default=datetime.now().date())
        )
        
        # Graduation table
        self.graduation_table = Table(
            self.tables['graduation'],
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('country_code', String(3)),
            Column('country_name', String(100)),
            Column('year', Integer),
            Column('graduation_rate', Float),
            Column('completion_rate', Float),
            Column('education_level', String(50)),
            Column('data_source', String(50)),
            Column('extraction_date', Date),
            Column('created_at', Date, default=datetime.now().date())
        )
        
        # Spending table
        self.spending_table = Table(
            self.tables['spending'],
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('country_code', String(3)),
            Column('country_name', String(100)),
            Column('year', Integer),
            Column('spending_usd', Float),
            Column('spending_per_capita', Float),
            Column('spending_percent_gdp', Float),
            Column('currency', String(3)),
            Column('data_source', String(50)),
            Column('extraction_date', Date),
            Column('created_at', Date, default=datetime.now().date())
        )
        
        # Country metadata table
        self.country_table = Table(
            self.tables['countries'],
            self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('country_code', String(3), unique=True),
            Column('country_name', String(100), unique=True),
            Column('region', String(50)),
            Column('income_group', String(50)),
            Column('population', Integer),
            Column('gdp_per_capita', Float),
            Column('data_available', Integer),
            Column('last_updated', Date),
            Column('created_at', Date, default=datetime.now().date())
        )
    
    def create_tables(self):
        """Create database tables if they don't exist"""
        try:
            self.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def load_enrollment_data(self, df: pd.DataFrame):
        """Load enrollment data into database"""
        logger.info(f"Loading enrollment data: {len(df)} records")
        
        try:
            # Prepare DataFrame for database
            df_to_load = df.copy()
            
            # Ensure column names match table schema
            column_mapping = {
                'enrollment_rate': 'enrollment_rate',
                'year': 'year',
                'country_code': 'country_code',
                'country_name': 'country_name',
                'data_source': 'data_source',
                'extraction_date': 'extraction_date'
            }
            
            # Select and rename columns
            df_to_load = df_to_load.rename(columns={v: k for k, v in column_mapping.items() 
                                                   if v in df_to_load.columns})
            
            # Add missing columns with default values
            for col in ['education_level', 'gender']:
                if col not in df_to_load.columns:
                    df_to_load[col] = 'Not Specified'
            
            # Add creation timestamp
            df_to_load['created_at'] = datetime.now().date()
            
            # Load to database
            df_to_load.to_sql(
                self.tables['enrollment'],
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df_to_load)} enrollment records")
            
        except Exception as e:
            logger.error(f"Error loading enrollment data: {e}")
            raise
    
    def load_graduation_data(self, df: pd.DataFrame):
        """Load graduation data into database"""
        logger.info(f"Loading graduation data: {len(df)} records")
        
        try:
            df_to_load = df.copy()
            
            column_mapping = {
                'graduation_rate': 'graduation_rate',
                'completion_rate': 'completion_rate',
                'year': 'year',
                'country_code': 'country_code',
                'country_name': 'country_name',
                'data_source': 'data_source',
                'extraction_date': 'extraction_date'
            }
            
            df_to_load = df_to_load.rename(columns={v: k for k, v in column_mapping.items() 
                                                   if v in df_to_load.columns})
            
            if 'education_level' not in df_to_load.columns:
                df_to_load['education_level'] = 'All Levels'
            
            df_to_load['created_at'] = datetime.now().date()
            
            df_to_load.to_sql(
                self.tables['graduation'],
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df_to_load)} graduation records")
            
        except Exception as e:
            logger.error(f"Error loading graduation data: {e}")
            raise
    
    def load_spending_data(self, df: pd.DataFrame):
        """Load spending data into database"""
        logger.info(f"Loading spending data: {len(df)} records")
        
        try:
            df_to_load = df.copy()
            
            column_mapping = {
                'spending_usd': 'spending_usd',
                'spending_per_capita': 'spending_per_capita',
                'year': 'year',
                'country_code': 'country_code',
                'country_name': 'country_name',
                'currency': 'currency',
                'data_source': 'data_source',
                'extraction_date': 'extraction_date'
            }
            
            df_to_load = df_to_load.rename(columns={v: k for k, v in column_mapping.items() 
                                                   if v in df_to_load.columns})
            
            # Calculate spending as percent of GDP if not available
            if 'spending_percent_gdp' not in df_to_load.columns:
                # Placeholder calculation - would need actual GDP data
                df_to_load['spending_percent_gdp'] = None
            
            df_to_load['created_at'] = datetime.now().date()
            
            df_to_load.to_sql(
                self.tables['spending'],
                self.engine,
                if_exists='append',
                index=False
            )
            
            logger.info(f"Successfully loaded {len(df_to_load)} spending records")
            
        except Exception as e:
            logger.error(f"Error loading spending data: {e}")
            raise
    
    def load_country_metadata(self, df: pd.DataFrame):
        """Load country metadata into database"""
        logger.info(f"Loading country metadata: {len(df)} records")
        
        try:
            df_to_load = df.copy()
            
            column_mapping = {
                'country_code': 'country_code',
                'country_name': 'country_name',
                'region': 'region',
                'income_group': 'income_group',
                'data_available': 'data_available',
                'last_updated': 'last_updated'
            }
            
            df_to_load = df_to_load.rename(columns={v: k for k, v in column_mapping.items() 
                                                   if v in df_to_load.columns})
            
            # Add missing columns
            for col in ['population', 'gdp_per_capita']:
                if col not in df_to_load.columns:
                    df_to_load[col] = None
            
            df_to_load['created_at'] = datetime.now().date()
            
            # Use SQLAlchemy to handle duplicates
            from sqlalchemy.dialects.sqlite import insert
            
            conn = self.engine.connect()
            
            for _, row in df_to_load.iterrows():
                stmt = insert(self.country_table).values(**row.to_dict())
                stmt = stmt.on_conflict_do_update(
                    index_elements=['country_code'],
                    set_=row.to_dict()
                )
                conn.execute(stmt)
            
            conn.close()
            
            logger.info(f"Successfully loaded/updated {len(df_to_load)} country records")
            
        except Exception as e:
            logger.error(f"Error loading country metadata: {e}")
            raise
    
    def run_etl_pipeline(self, data_dict: Dict[str, pd.DataFrame]):
        """Run complete ETL pipeline"""
        logger.info("Starting ETL pipeline")
        
        try:
            # Create tables if they don't exist
            self.create_tables()
            
            # Load data in correct order (metadata first)
            if 'countries' in data_dict:
                self.load_country_metadata(data_dict['countries'])
            
            if 'enrollment' in data_dict:
                self.load_enrollment_data(data_dict['enrollment'])
            
            if 'graduation' in data_dict:
                self.load_graduation_data(data_dict['graduation'])
            
            if 'spending' in data_dict:
                self.load_spending_data(data_dict['spending'])
            
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
    
    def query_database(self, query: str) -> pd.DataFrame:
        """Query the database and return results as DataFrame"""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(query, conn)
            return result
        except Exception as e:
            logger.error(f"Error querying database: {e}")
            raise
    
    def get_table_stats(self) -> Dict:
        """Get statistics about database tables"""
        stats = {}
        
        tables = [self.tables['enrollment'], self.tables['graduation'], 
                  self.tables['spending'], self.tables['countries']]
        
        for table in tables:
            try:
                query = f"SELECT COUNT(*) as count FROM {table}"
                df = self.query_database(query)
                stats[table] = df['count'].iloc[0]
            except:
                stats[table] = 0
        
        return stats

def main():
    """Main loading function"""
    logger.info("Starting data loading pipeline")
    
    try:
        loader = EducationDataLoader()
        
        # For demo, create sample data
        # In production, this would come from the transformation step
        sample_enrollment = pd.DataFrame({
            'country_code': ['USA', 'GBR', 'DEU'],
            'country_name': ['United States', 'United Kingdom', 'Germany'],
            'year': [2022, 2022, 2022],
            'enrollment_rate': [95.5, 96.2, 97.1],
            'education_level': ['Primary', 'Primary', 'Primary'],
            'gender': ['Total', 'Total', 'Total'],
            'data_source': ['OECD'],
            'extraction_date': [datetime.now().date()] * 3
        })
        
        sample_graduation = pd.DataFrame({
            'country_code': ['USA', 'GBR', 'DEU'],
            'country_name': ['United States', 'United Kingdom', 'Germany'],
            'year': [2022, 2022, 2022],
            'graduation_rate': [85.5, 88.2, 90.1],
            'completion_rate': [0.855, 0.882, 0.901],
            'education_level': ['Upper Secondary', 'Upper Secondary', 'Upper Secondary'],
            'data_source': ['OECD'],
            'extraction_date': [datetime.now().date()] * 3
        })
        
        sample_countries = pd.DataFrame({
            'country_code': ['USA', 'GBR', 'DEU'],
            'country_name': ['United States', 'United Kingdom', 'Germany'],
            'region': ['North America', 'Europe', 'Europe'],
            'income_group': ['High Income', 'High Income', 'High Income'],
            'data_available': [True, True, True],
            'last_updated': [datetime.now().date()] * 3
        })
        
        data_dict = {
            'enrollment': sample_enrollment,
            'graduation': sample_graduation,
            'countries': sample_countries
        }
        
        # Run ETL pipeline
        loader.run_etl_pipeline(data_dict)
        
        # Show statistics
        stats = loader.get_table_stats()
        logger.info(f"Database statistics: {stats}")
        
        logger.info("Loading completed successfully")
        
    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise

if __name__ == "__main__":
    main()

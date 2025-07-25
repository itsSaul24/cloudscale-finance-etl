"""
Configuration management for CloudScale Finance ETL
Loads settings from environment variables and provides defaults
"""

import os
from pathlib import Path
from dotenv import load_dotenv


# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(env_path)


class Config:
    """Application configuration settings"""
    
    # Environment
    ENV = os.getenv('ENV', 'development')
    DEBUG = ENV == 'development'
    
    # API Configuration
    ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
    YAHOO_FINANCE_API_KEY = os.getenv('YAHOO_FINANCE_API_KEY')
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'cloudscale-finance-etl-data')
    
    # Google Cloud Configuration
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    GCP_SERVICE_ACCOUNT_PATH = os.getenv('GCP_SERVICE_ACCOUNT_PATH')
    BIGQUERY_DATASET_NAME = os.getenv('BIGQUERY_DATASET_NAME', 'financial_data')
    
    # Application Settings
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Monitoring
    ENABLE_MONITORING = os.getenv('ENABLE_MONITORING', 'true').lower() == 'true'
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
    ALERT_EMAIL = os.getenv('ALERT_EMAIL')
    
    # Data Settings
    DATA_DIR = Path(__file__).parent.parent.parent / 'data'
    RAW_DATA_DIR = DATA_DIR / 'raw'
    PROCESSED_DATA_DIR = DATA_DIR / 'processed'
    
    # API Rate Limits
    ALPHA_VANTAGE_RATE_LIMIT = 5  # calls per minute
    ALPHA_VANTAGE_DAILY_LIMIT = 500  # calls per day
    
    @classmethod
    def validate(cls):
        """Validate required configuration settings"""
        errors = []
        
        # Check required API keys
        if not cls.ALPHA_VANTAGE_API_KEY:
            errors.append("ALPHA_VANTAGE_API_KEY is required")
        
        # Check AWS credentials if not in development
        if cls.ENV != 'development':
            if not cls.AWS_ACCESS_KEY_ID:
                errors.append("AWS_ACCESS_KEY_ID is required")
            if not cls.AWS_SECRET_ACCESS_KEY:
                errors.append("AWS_SECRET_ACCESS_KEY is required")
            if not cls.GCP_PROJECT_ID:
                errors.append("GCP_PROJECT_ID is required")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        print(f"✓ Configuration validated for {cls.ENV} environment")
        
    @classmethod
    def display(cls):
        """Display current configuration (hiding sensitive values)"""
        print("\nCurrent Configuration:")
        print(f"  Environment: {cls.ENV}")
        print(f"  Debug Mode: {cls.DEBUG}")
        print(f"  S3 Bucket: {cls.S3_BUCKET_NAME}")
        print(f"  BigQuery Dataset: {cls.BIGQUERY_DATASET_NAME}")
        print(f"  Log Level: {cls.LOG_LEVEL}")
        print(f"  Monitoring Enabled: {cls.ENABLE_MONITORING}")
        print(f"  Alpha Vantage API Key: {'***' + cls.ALPHA_VANTAGE_API_KEY[-4:] if cls.ALPHA_VANTAGE_API_KEY else 'Not Set'}")
        print()


# Validate configuration on import
if __name__ == "__main__":
    Config.display()
    Config.validate()
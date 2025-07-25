#!/usr/bin/env python3
"""
Project setup script for CloudScale Finance ETL
Creates the complete directory structure with placeholder files
"""

import os
import sys
from pathlib import Path


def create_directory_structure():
    """Create the complete project directory structure"""
    
    # Define the directory structure
    directories = [
        "src/ingestion",
        "src/transformation", 
        "src/storage",
        "src/monitoring",
        "src/utils",
        "infrastructure/terraform",
        "infrastructure/docker",
        "airflow/dags",
        "airflow/plugins",
        "airflow/config",
        "scripts",
        "tests/unit",
        "tests/integration",
        "tests/e2e",
        "docs/screenshots",
        "portfolio/assets",
        "data/raw",
        "data/processed",
        "logs",
        "credentials"
    ]
    
    # Create directories
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✓ Created directory: {directory}")
    
    # Create __init__.py files for Python packages
    python_packages = [
        "src",
        "src/ingestion",
        "src/transformation",
        "src/storage", 
        "src/monitoring",
        "src/utils",
        "tests",
        "tests/unit",
        "tests/integration",
        "tests/e2e"
    ]
    
    for package in python_packages:
        init_file = Path(package) / "__init__.py"
        init_file.touch(exist_ok=True)
        print(f"✓ Created {init_file}")
    
    # Create placeholder files
    placeholder_files = {
        "src/ingestion/alpha_vantage.py": "# Alpha Vantage API client\n",
        "src/ingestion/yahoo_finance.py": "# Yahoo Finance backup API client\n",
        "src/ingestion/data_validator.py": "# Data validation functions\n",
        "src/transformation/cleaner.py": "# Data cleaning functions\n",
        "src/transformation/indicators.py": "# Technical indicator calculations\n",
        "src/transformation/aggregator.py": "# Data aggregation functions\n",
        "src/storage/s3_handler.py": "# AWS S3 operations\n",
        "src/storage/bigquery_handler.py": "# Google BigQuery operations\n",
        "src/monitoring/health_checks.py": "# System health monitoring\n",
        "src/monitoring/alerting.py": "# Alert management\n",
        "src/utils/config.py": "# Configuration management\n",
        "src/utils/logger.py": "# Logging setup\n",
        "airflow/dags/daily_etl_pipeline.py": "# Main ETL DAG\n",
        "infrastructure/terraform/main.tf": "# Terraform main configuration\n",
        "infrastructure/terraform/variables.tf": "# Terraform variables\n",
        "infrastructure/terraform/outputs.tf": "# Terraform outputs\n",
        "infrastructure/docker/Dockerfile": "# Docker image for ETL\n",
        "infrastructure/docker/docker-compose.yml": "# Production docker-compose\n",
        "infrastructure/docker/docker-compose.local.yml": "# Local development docker-compose\n",
        "scripts/deploy-demo.sh": "#!/bin/bash\n# Demo deployment script\n",
        "scripts/teardown-demo.sh": "#!/bin/bash\n# Demo cleanup script\n",
        "scripts/seed-data.sh": "#!/bin/bash\n# Sample data seeding\n",
        "scripts/run-local.sh": "#!/bin/bash\n# Local startup script\n",
        "tests/conftest.py": "# Pytest configuration\n",
        "docs/architecture.md": "# Architecture documentation\n",
        "docs/demo-guide.md": "# Demo guide\n",
        "portfolio/index.html": "<!-- Portfolio page -->\n"
    }
    
    for filepath, content in placeholder_files.items():
        file_path = Path(filepath)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(content)
        print(f"✓ Created {filepath}")
        
        # Make shell scripts executable
        if filepath.endswith(".sh"):
            os.chmod(filepath, 0o755)
    
    print("\n✅ Project structure created successfully!")
    print("\nNext steps:")
    print("1. Copy .env.example to .env and add your API keys")
    print("2. Run 'pip install -r requirements.txt' to install dependencies")
    print("3. Run 'pre-commit install' to set up git hooks")
    print("4. Start coding! Begin with src/utils/config.py and src/utils/logger.py")


if __name__ == "__main__":
    print("Setting up CloudScale Finance ETL project structure...\n")
    
    # Check if we're in the right directory
    if os.path.exists("README.md"):
        response = input("README.md already exists. Continue anyway? (y/n): ")
        if response.lower() != 'y':
            print("Setup cancelled.")
            sys.exit(0)
    
    try:
        create_directory_structure()
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        sys.exit(1)
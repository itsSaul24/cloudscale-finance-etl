# CloudScale Finance ETL Pipeline

A financial data ETL pipeline that demonstrates modern data engineering practices. This project fetches stock market data, transforms it with technical indicators, and loads it into a cloud data warehouse.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Docker Desktop
- AWS CLI (configured with credentials)
- Google Cloud SDK
- Terraform

### Local Setup
```bash
# Clone the repository
git clone https://github.com/itsSaul24/cloudscale-finance-etl.git
cd cloudscale-finance-etl

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencieså
pip install -r requirements.txt

# Copy environment variables
cp .env.example .env
# Edit .env with your API keys

# Run tests
python -m pytest tests/
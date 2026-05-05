# 🌍 West Africa Economic Data Pipeline

> An end-to-end automated ETL pipeline that extracts, 
> transforms, and loads economic indicators for 
> 10 West African countries using Apache Airflow, 
> AWS S3, and PostgreSQL.

## 🏗️ Architecture

Extract (World Bank API) → Transform (Python/Pandas) 
→ Load (AWS S3 + PostgreSQL) → Validate (Airflow)

## 🛠️ Tech Stack
| Tool | Purpose |
|------|---------|
| Apache Airflow 2.7 | Pipeline orchestration |
| Python 3.10 | Data extraction & transformation |
| AWS S3 | Raw & processed data storage |
| PostgreSQL 13 | Analytical data warehouse |
| Docker & Docker Compose | Containerization |
| Pandas / PyArrow | Data processing |

## 📊 Data Coverage
- **10 countries**: Senegal, Nigeria, Ghana, Côte d'Ivoire...
- **5 indicators**: GDP, Population, Inflation, Unemployment, Internet usage
- **23 years**: 2000 to 2023
- **Schedule**: Runs automatically every month

## 🚀 Quick Start
```bash
git clone https://github.com/bachir00/west-africa-economic-data-pipeline
cd west-africa-economic-data-pipeline
cp .env.example .env  # Add your AWS credentials
docker-compose up -d
# Open Airflow UI at http://localhost:8080
# Trigger DAG: west_africa_economic_pipeline
```

## 📁 Project Structure
[structure des dossiers ici]

## 📈 Sample Output
[Screenshot de l'Airflow UI avec le DAG qui tourne]
[Screenshot des données dans PostgreSQL]
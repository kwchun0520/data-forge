# Data Forge

A modern data engineering pipeline for weather data processing using Apache Airflow, dbt, and PostgreSQL.

## Overview

Data Forge is a comprehensive data platform that extracts weather data from external APIs, processes it through multiple data layers (raw, staging, mart), and provides clean, reliable data for analytics and reporting. The platform is built with modern data engineering best practices including containerization, data quality testing, and incremental processing.

## Architecture

```
                                    Data Forge Pipeline
    ┌────────────────────────────────────────────────────────────────────────────────┐
    │                                                                                │
    │  ┌─────────────────┐     ┌─────────────────────────────────────────────────┐   │
    │  │   Weather API   │────▶│                Airflow                          │   │
    │  │                 │     │          ┌─────────────────────┐                │   │
    │  │ • Current Data  │     │          │  orchestrator.py    │                │   │
    │  │ • Air Quality   │     │          │   (DAG Schedule)    │                │   │
    │  │ • Astro Data    │     │          └─────────────────────┘                │   │
    │  └─────────────────┘     └─────────────────┬───────────────────────────────┘   │
    │                                            │                                   │
    │                                            ▼                                   │
    │  ┌─────────────────────────────────────────────────────────────────────────┐   │
    │  │                        PostgreSQL Database                              │   │
    │  │                                                                         │   │
    │  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐  │   │
    │  │  │   source    │   │     raw     │   │   staging   │   │    mart     │  │   │
    │  │  │             │   │             │   │             │   │             │  │   │
    │  │  │ weather_    │   │ raw_weather │   │ stg_weather │   │ fact_weather│  │   │
    │  │  │ data        │──▶│ _data       │──▶│ _data       │──▶│ _summary    │  │   │
    │  │  │             │   │             │   │             │   │             │  │   │
    │  │  │ (API Raw)   │   │ (Cleaned)   │   │ (Typed)     │   │ (Analytics) │  │   │
    │  │  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘  │   │
    │  └─────────────────────────────────────────────────────────────────────────┘   │
    │                                            ▲                                   │
    │                                            │                                   │
    │  ┌─────────────────────────────────────────────────────────────────────────┐   │
    │  │                           dbt (Data Build Tool)                         │   │
    │  │                                                                         │   │
    │  │  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐  │   │
    │  │  │   sources   │   │    models   │   │    tests    │   │    docs     │  │   │
    │  │  │             │   │             │   │             │   │             │  │   │
    │  │  │ • Schema    │   │ • Raw SQL   │   │ • Quality   │   │ • Lineage   │  │   │
    │  │  │ • Tests     │   │ • Staging   │   │ • Range     │   │ • Catalog   │  │   │
    │  │  │ • Contracts │   │ • Mart      │   │ • Null      │   │ • Metrics   │  │   │
    │  │  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘  │   │
    │  └─────────────────────────────────────────────────────────────────────────┘   │
    │                                                                                │
    └────────────────────────────────────────────────────────────────────────────────┘

                            ┌─────────────────────────────────┐
                            │        Infrastructure           │
                            │                                 │
                            │ • Docker Containers             │
                            │ • Volume Persistence            │
                            │ • Network Isolation             │
                            │ • Service Orchestration         │
                            └─────────────────────────────────┘
```

## Features

- **Automated Data Pipeline**: Scheduled data extraction and processing using Apache Airflow
- **Multi-Layer Data Architecture**: Raw, staging, and mart layers for clean data separation
- **Data Quality Testing**: Comprehensive testing with dbt including range checks and null validation
- **Incremental Processing**: Efficient data updates with incremental materialization
- **Containerized Infrastructure**: Complete Docker-based deployment for consistent environments
- **Weather Data Coverage**: Comprehensive weather metrics including current conditions, air quality, and astronomical data

## Tech Stack

- **Orchestration**: Apache Airflow 2.x
- **Data Transformation**: dbt (Data Build Tool)
- **Database**: PostgreSQL 14
- **Containerization**: Docker & Docker Compose
- **Python**: 3.13+ with modern dependency management
- **Data Quality**: dbt tests with dbt-utils

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.13+
- 8GB+ RAM recommended

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data_forge
   ```

2. **Create environment configuration**
   ```bash
   # Copy the example environment file
   cp .env.example .env
   
   # Edit .env with your actual configuration values
   # Key variables to update:
   # - WEATHER_API_KEY: Your weather API key
   # - POSTGRES_USER: Database username
   # - POSTGRES_PASSWORD: Database password
   # - POSTGRES_DB: Database name
   # - AIRFLOW_DB_USER: Airflow database user
   # - AIRFLOW_DB_PASSWORD: Airflow database password
   # - AIRFLOW_USERNAME: Airflow admin username
   # - AIRFLOW_PASSWORD: Airflow admin password
   ```

3. **Create database initialization script**
   ```bash
   # Copy the example SQL initialization script
   cp infrastructure/postgres/airflow-init.example.sql infrastructure/docker/postgres/airflow-init.sql
   
   # Edit the airflow-init.sql file to match your .env credentials
   # Ensure the database user and password match your AIRFLOW_DB_* variables
   ```

4. **Start the infrastructure**
   ```bash
   docker-compose up -d
   ```

5. **Access the services**
   - Airflow UI: http://localhost:8080
   - PostgreSQL: localhost:5432

### First Run

1. **Initialize the database**
   ```bash
   docker exec airflow_container airflow db init
   ```

2. **Run dbt models**
   ```bash
   docker exec dbt_container dbt run
   ```

3. **Execute data pipeline**
   - Access Airflow UI and trigger the `weather_data_pipeline` DAG

## Project Structure

```
data_forge/
├── infrastructure/           # Infrastructure configuration
│   ├── airflow/             # Airflow DAGs and configuration
│   │   └── dags/            # Airflow DAG definitions
│   ├── dbt/                 # dbt project
│   │   └── data_forge/      # dbt models, tests, and configuration
│   ├── docker/              # Docker configurations
│   └── postgres/            # PostgreSQL configuration
│       └── airflow-init.example.sql  # Database initialization script
├── src/                     # Source code
│   └── data_forge/          # Python package
│       ├── connections/     # Database connections
│       ├── models/          # Data models
│       └── scripts/         # Data processing scripts
├── config/                  # Configuration files
│   └── schemas/             # Database schema definitions
├── tests/                   # Test suite
├── data/                    # Data storage (PostgreSQL data)
├── scripts/                 # Setup and utility scripts
├── .env.example             # Environment variables template
├── docker-compose.yaml      # Docker services definition
├── pyproject.toml          # Python project configuration
└── README.md               # This file
```

## Data Flow

### 1. Data Extraction
- Airflow orchestrates API calls to weather services
- Raw data is stored in `source.weather_data` table
- Comprehensive weather metrics including:
  - Current weather conditions
  - Air quality measurements
  - Astronomical data (sunrise, sunset, moon phases)
  - Location and timezone information

### 2. Data Processing (dbt)

#### Raw Layer (`raw/`)
- Mirrors source data with minimal transformation
- Primary key generation and basic data typing

#### Staging Layer (`staging/`)
- Data cleaning and standardization
- Column renaming and data type enforcement
- Data quality validations

#### Mart Layer (`mart/`)
- Business logic and aggregations
- Denormalized tables for analytics
- Performance-optimized models

### 3. Data Quality
- Automated testing with dbt
- Range validation for numeric fields
- Null checks for required fields
- Referential integrity validation

## Configuration

### Environment Variables
```bash
# Database Configuration
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=db

# Airflow Configuration
AIRFLOW_DB_USER=airflow_user
AIRFLOW_DB_PASSWORD=airflow_password

# API Configuration
WEATHER_API_KEY=your_api_key
```

### dbt Configuration
- Project configuration: `infrastructure/dbt/data_forge/dbt_project.yml`
- Database profiles: `infrastructure/dbt/data_forge/profiles.yml`
- Source definitions: `infrastructure/dbt/data_forge/models/sources.yml`

## Development

### Adding New Data Sources
1. Define schema in `config/schemas/source/`
2. Update `models/sources.yml` with new source definition
3. Create raw model in `models/raw/`
4. Add staging transformations in `models/staging/`

### Running Tests
```bash
# dbt tests
docker exec dbt_container dbt test

# Python tests
docker exec airflow_container python -m pytest

# Data quality validation
docker exec dbt_container dbt test --models source:raw_data
```

### Development Workflow
```bash
# Parse dbt models
docker exec dbt_container dbt parse

# Run specific models
docker exec dbt_container dbt run --models raw_weather_data

# Generate documentation
docker exec dbt_container dbt docs generate
docker exec dbt_container dbt docs serve
```

## Monitoring and Maintenance

### Health Checks
- Airflow UI provides DAG run status and logs
- dbt test results indicate data quality issues
- PostgreSQL logs available via Docker logs

### Performance Optimization
- Incremental models for large datasets
- Proper indexing on frequently queried columns
- Regular VACUUM and ANALYZE operations

### Backup and Recovery
- PostgreSQL data persisted in `./data/postgres/`
- Regular database backups recommended
- Version control for all code and configuration

## Troubleshooting

### Common Issues

1. **dbt compilation errors**
   ```bash
   docker exec dbt_container dbt parse
   docker exec dbt_container dbt debug
   ```

2. **Airflow connection issues**
   - Check database connectivity
   - Verify environment variables
   - Review Airflow logs

3. **Performance issues**
   - Monitor resource usage
   - Optimize dbt models
   - Check database indexes

### Logs
```bash
# Airflow logs
docker logs airflow_container

# dbt logs
docker exec dbt_container cat logs/dbt.log

# PostgreSQL logs
docker logs postgresdb_container
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

[License details here]

## Support

For questions and support:
- Create an issue in the repository
- Check the troubleshooting section
- Review logs for error details
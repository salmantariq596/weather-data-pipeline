# Weather Data Pipeline — Bahawalpur

![Python](https://img.shields.io/badge/Python-3.11-blue) ![Airflow](https://img.shields.io/badge/Apache%20Airflow-orchestration-017CEE) ![dbt](https://img.shields.io/badge/dbt-transform-FF694B) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-warehouse-336791) ![Redis](https://img.shields.io/badge/Redis-dedup-DC382D) ![Grafana](https://img.shields.io/badge/Grafana-dashboards-F46800) ![Tests](https://github.com/salmantariq596/weather-data-pipeline/actions/workflows/tests.yml/badge.svg)

> Setup: `cp .env.example .env` and add a free OpenWeatherMap API key.

A production-grade data engineering pipeline with multi-source extraction, data quality checks, partitioning, orchestration, and analytics.

## What's New vs. Basic Version

| Feature | Basic | Advanced |
|---------|-------|----------|
| Data sources | 1 (OpenWeatherMap) | 3 (OpenWeatherMap + Open-Meteo + Air Quality) |
| Raw storage | Simple file | Date-partitioned files + PostgreSQL raw layer |
| Data quality | None | Automated checks + scoring + issue logging |
| Pipeline tracking | None | Full lineage tracking in database |
| Deduplication | None | Redis-based fingerprint deduplication |
| Orchestration | None | Apache Airflow DAGs |
| Transformations | Inline Python | dbt models (staging + marts) |
| Backfilling | None | 30-day historical backfill |
| Dashboards | None | Grafana dashboards |
| Testing | None | Unit tests + dbt schema tests |
| Analytics | Basic SQL | Rolling averages, correlations, heat wave detection |

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  OpenWeatherMap │────▶│                 │     │                 │
│  (Current)      │     │   Python ETL    │     │   PostgreSQL    │
├─────────────────┤     │   (Docker)      │────▶│   (Partitioned) │
│  OpenWeatherMap │────▶│                 │     │                 │
│  (Forecast)     │     │  ┌───────────┐  │     │  ┌───────────┐  │
├─────────────────┤     │  │ Data      │  │     │  │ Raw Layer │  │
│  OpenWeatherMap │────▶│  │ Quality   │  │     │  │ Staging   │  │
│  (Air Quality)  │     │  │ Checker   │  │     │  │ Marts     │  │
├─────────────────┤     │  └───────────┘  │     │  └───────────┘  │
│  Open-Meteo     │────▶│                 │     │                 │
│  (No API key)   │     └─────────────────┘     └─────────────────┘
└─────────────────┘              │
                                 ▼
                        ┌─────────────────┐
                        │  raw_data/      │
                        │  YYYY-MM-DD/    │
                        │  source/        │
                        │  *.json         │
                        └─────────────────┘

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Apache Airflow │────▶│      dbt        │────▶│    Grafana      │
│  (Scheduling)   │     │  (Transform)    │     │  (Dashboards)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Quick Start

### 1. Prerequisites
- Docker & Docker Compose installed
- Free OpenWeatherMap API key (get at https://home.openweathermap.org/api_keys)

### 2. Configure
```bash
# Edit .env with your API key
OPENWEATHER_API_KEY=your_real_key_here
```

### 3. Start Everything
```bash
# Start all services
docker-compose up -d

# Check services are running
docker-compose ps
```

### 4. Run Initial Backfill (30 days of historical data)
```bash
docker-compose up backfill
```

### 5. Run the ETL Pipeline
```bash
# One-time run
docker-compose up etl

# Or let Airflow schedule it automatically
# Access Airflow UI at http://localhost:8080 (admin/admin)
```

### 6. Access Dashboards
- **Airflow**: http://localhost:8080 (admin/admin)
- **Grafana**: http://localhost:3000 (admin/admin)
- **PostgreSQL**: localhost:5432 (weather_user/weather_pass)

## Data Sources

### OpenWeatherMap (requires API key, free tier: 1000 calls/day)
- Current weather
- 5-day/3-hour forecast
- Air quality index (CO, NO, NO2, O3, SO2, PM2.5, PM10, NH3)

### Open-Meteo (no API key required)
- Current weather
- Hourly forecast (7 days)
- Historical data (back to 1940)

## Database Schema

### Raw Layer (`raw_weather`)
| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| source | VARCHAR | API source name |
| api_endpoint | VARCHAR | Specific endpoint |
| raw_json | JSONB | Complete raw response |
| http_status | INT | HTTP response code |
| response_time_ms | INT | API response time |
| processed | BOOLEAN | Whether transformed |

### Staging Layer (`stg_weather_current`, `stg_weather_forecast`, `stg_air_quality`)
Cleaned, typed data with quality scores.

### Mart Layer (`mart_daily_weather`)
Pre-aggregated daily summaries for fast analytics.

### Pipeline Metadata (`pipeline_runs`, `data_quality_issues`)
Track every run and every data quality issue.

## Running dbt

```bash
# Run all models
docker-compose up dbt

# Or manually
cd dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Running Tests

```bash
# Unit tests (in Docker)
docker exec weather_etl python -m pytest tests/

# Unit tests (locally, no services needed)
pip install -r requirements.txt && python -m pytest tests/

# Data quality tests via dbt
docker-compose up dbt
```

## Key SQL Queries

### Pipeline Health
```sql
SELECT pipeline_name, status, records_extracted, records_loaded, 
       duration_seconds, error_message
FROM pipeline_runs
ORDER BY started_at DESC
LIMIT 10;
```

### Data Quality Report
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE data_quality_score < 0.5) as low_quality,
    ROUND(AVG(data_quality_score)::numeric, 3) as avg_quality
FROM stg_weather_current;
```

### Source Comparison
```sql
SELECT source, COUNT(*) as readings,
       ROUND(AVG(temperature)::numeric, 2) as avg_temp
FROM stg_weather_current
GROUP BY source;
```

### Heat Wave Detection
```sql
-- See scripts/queries.py for full query
```

## Project Structure

```
weather_pipeline_advanced/
├── docker-compose.yml          # Full stack orchestration
├── Dockerfile.etl              # ETL container
├── Dockerfile.airflow          # Airflow container
├── Dockerfile.dbt              # dbt container
├── requirements.txt            # Python dependencies
├── init.sql                    # Database schema
├── .env                        # API keys & config
├── raw_data/                   # Date-partitioned raw JSON
│   └── YYYY-MM-DD/
│       └── source/
│           └── *.json
├── scripts/
│   ├── etl_full.py            # Main ETL pipeline
│   ├── backfill.py            # Historical data backfill
│   └── queries.py             # SQL analysis queries
├── airflow/
│   └── dags/
│       └── weather_pipeline.py # Airflow DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_weather_current_clean.sql
│       ├── marts/
│       │   ├── daily_weather_summary.sql
│       │   └── weather_alerts.sql
│       └── schema.yml
├── grafana/
│   └── dashboards/
│       └── weather_dashboard.json
├── notebooks/
│   └── weather_analysis.ipynb
└── tests/
    └── test_etl.py
```

## Monitoring & Alerting

### Check Pipeline Status
```bash
docker logs weather_etl
docker logs weather_scheduler
```

### Check Data Freshness
```sql
SELECT MAX(fetched_at) as latest_data 
FROM stg_weather_current;
```

### Check for Data Quality Issues
```sql
SELECT issue_type, severity, COUNT(*) as count
FROM data_quality_issues
WHERE is_resolved = FALSE
GROUP BY issue_type, severity;
```

## Stopping Everything

```bash
docker-compose down          # Stop containers
docker-compose down -v       # Stop + delete all data (careful!)
```

## Why This Architecture

1. **Multi-source**: Validates data by cross-referencing independent APIs
2. **Raw landing**: Never lose original data, can reprocess if schemas change
3. **Date partitioning**: Efficient queries and easy archival
4. **Data quality**: Catch bad data before it reaches analytics
5. **Pipeline tracking**: Know exactly when things break and why
6. **dbt**: Version-controlled, tested SQL transformations
7. **Airflow**: Production-grade scheduling with retries and monitoring
8. **Grafana**: Real-time dashboards without writing frontend code

## Next Steps

1. Add more cities by modifying `CITY`, `LAT`, `LON` in the config
2. Add more data sources (e.g., weather.gov, meteostat)
3. Set up alerting in Grafana for extreme weather conditions
4. Add ML models for temperature prediction
5. Deploy to cloud (AWS/GCP/Azure) using managed services

## Author

**Salman Tariq** — Backend & Python Data Engineer · [GitHub](https://github.com/salmantariq596)
